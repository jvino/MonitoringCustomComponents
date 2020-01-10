/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ch.cern.alice.o2.kafka.connectors;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;
import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import static net.sourceforge.argparse4j.impl.Arguments.store;

import java.net.SocketException;
import java.io.File;

import ch.cern.alice.o2.kafka.utils.YamlInfluxdbUdpConsumer;

public class InfluxdbUdpConsumer {
	private static Logger logger = LoggerFactory.getLogger(InfluxdbUdpConsumer.class);
	private static String data_endpoint_port_str = "";
	private static int[] data_endpoint_ports = null;
	private static int data_endpoint_ports_size = 0;
	private static int data_endpoint_ports_index = 0;
	private static int stats_endpoint_port = 0;
	private static long receivedRecords = 0;
	private static long sentRecords = 0;
	private static long stats_period_ms = 0;
	private static long startMs = 0;
	private static String stats_type;
	private static String data_endpoint_hostname = null;
	private static InetAddress data_address = null;
	private static InetAddress stats_address = null;
	private static DatagramSocket datagramSocket;
	private static String topicName = "";
	private static boolean stats_enabled;
	private static KafkaConsumer<byte[], byte[]> consumer = null;

	/* Stats parameters */
	private static final String STATS_TYPE_INFLUXDB = "influxdb";
	private static final String DEFAULT_STATS_TYPE = STATS_TYPE_INFLUXDB;

	/* Kafka consumer parameters */
	private static final String DEFAULT_AUTO_OFFSET_RESET = "latest";
	private static final String DEFAULT_FETCH_MIN_BYTES = "1";
	private static final String DEFAULT_RECEIVE_BUFFER_BYTES = "262144";
	private static final String DEFAULT_MAX_POLL_RECORDS = "1000000";
	private static final String DEFAULT_ENABLE_AUTO_COMMIT_CONFIG = "true";
	private static final String DEFAULT_GROUP_ID_CONFIG = "influxdb-udp-consumer";
	private static final int POLLING_PERIOD_MS = 50;

	private static final String GENERAL_LOG4J_CONFIG = "log4jfilename";
	private static final String KAFKA_TOPIC_CONFIG = "topic.input";
	private static final String SENDER_HOSTNAME_CONFIG = "hostname";
	private static final String SENDER_PORTS_CONFIG = "ports";
	private static final String STATS_HOSTNAME_CONFIG = "hostname";
	private static final String STATS_PORT_CONFIG = "port";
	private static final String STATS_PERIOD_S = "period.s";

	static Map<String, String> general_config = null;
	static Map<String, String> kafka_config = null;
	static Map<String, String> sender_config = null;
	static Map<String, String> stats_config = null;

	private static void importYamlConfiguration(String filename) throws IOException {
		File confFile = new File(filename);
		if (!confFile.exists()) { 
			throw new IOException("Configuration file does not exist.");
		}
		/* Parse yaml configuration file */
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		YamlInfluxdbUdpConsumer config = mapper.readValue(confFile, YamlInfluxdbUdpConsumer.class);
		general_config = config.getGeneral();
		kafka_config = config.getKafka_consumer_config();
		sender_config = config.getSender_config();
		stats_config = config.getStats_config();

		try {
			setGeneralConfiguration(general_config);
		} catch (Exception e) {
			logger.error(e.getMessage());
			System.exit(1);
		}
		try {
			setKafkaConfiguration(kafka_config);
		} catch (Exception e) {
			logger.error(e.getMessage());
			System.exit(1);
		}
		try {
			setSenderConfiguration(sender_config);
		} catch (Exception e) {
			logger.error(e.getMessage());
			System.exit(1);
		}
		setStatsConfiguration(stats_config);
	}

	private static Map<String,String> getGeneralConfig(){
		return general_config;
	}

	private static Map<String,String> getKafkaConfig(){
		return kafka_config;
	}

	private static Map<String,String> getSenderConfig(){
		return sender_config;
	}

	private static Map<String,String> getStatsConfig(){
		return stats_config;
	}

	private static boolean getStatsEnable(){
		return stats_enabled;
	}

	private static void setGeneralConfiguration( Map<String,String> general_config) throws Exception {
		if( !general_config.containsKey(GENERAL_LOG4J_CONFIG)){
			throw new Exception("Configuration file - general section - does not contain '"+GENERAL_LOG4J_CONFIG+"' key");
		}
		String log4jfilename = general_config.get(GENERAL_LOG4J_CONFIG);
		if( ! new File(log4jfilename).exists()){
			throw new IOException("Log configuration file '"+log4jfilename+"' does not exist");
		} else {
			PropertyConfigurator.configure(log4jfilename);
		}
	}

	private static void setKafkaConfiguration( Map<String,String> kafka_consumer_config) throws Exception {
		if( !kafka_consumer_config.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)){
			throw new Exception("Configuration file - kafka section - does not contain '"+ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG+"' key");
		}
		if( !kafka_consumer_config.containsKey(KAFKA_TOPIC_CONFIG)){
			throw new Exception("Configuration file - kafka section - does not contain '"+KAFKA_TOPIC_CONFIG+"' key");
		}
		topicName = kafka_consumer_config.get(KAFKA_TOPIC_CONFIG);
		
		String groupId = kafka_consumer_config.getOrDefault(ConsumerConfig.GROUP_ID_CONFIG,DEFAULT_GROUP_ID_CONFIG)
		String enableAutoCommit = kafka_consumer_config.getOrDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, DEFAULT_ENABLE_AUTO_COMMIT_CONFIG);
		String boostrapServers = kafka_consumer_config.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
		String autoOffsetReset = kafka_consumer_config.getOrDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, DEFAULT_AUTO_OFFSET_RESET);
		String fetchMinBytes = kafka_consumer_config.getOrDefault(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, DEFAULT_FETCH_MIN_BYTES);
		String receiveBuffer = kafka_consumer_config.getOrDefault(ConsumerConfig.RECEIVE_BUFFER_CONFIG, DEFAULT_RECEIVE_BUFFER_BYTES);
		String maxPollRecords = kafka_consumer_config.getOrDefault(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, DEFAULT_MAX_POLL_RECORDS);

		Properties props = new Properties();
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
		props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, receiveBuffer);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);

		logger.info("Kafka configuration. Input topic :"+topicName);
		logger.info("Kafka configuration. "+ConsumerConfig.GROUP_ID_CONFIG+" :"+groupId);
		logger.info("Kafka configuration. "+ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG+" :"+ByteArrayDeserializer.class.getName());
		logger.info("Kafka configuration. "+ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG+" :"+ByteArrayDeserializer.class.getName());
		logger.info("Kafka configuration. "+ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG+" :"+enableAutoCommit);
		logger.info("Kafka configuration. "+ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG+" :"+boostrapServers);
		logger.info("Kafka configuration. "+ConsumerConfig.AUTO_OFFSET_RESET_CONFIG+" :"+autoOffsetReset);
		logger.info("Kafka configuration. "+ConsumerConfig.FETCH_MIN_BYTES_CONFIG+" :"+fetchMinBytes);
		logger.info("Kafka configuration. "+ConsumerConfig.RECEIVE_BUFFER_CONFIG+" :"+receiveBuffer);
		logger.info("Kafka configuration. "+ConsumerConfig.MAX_POLL_RECORDS_CONFIG+" :"+maxPollRecords);
		
		consumer = new KafkaConsumer<byte[],byte[]>(props);
		consumer.subscribe(Collections.singletonList(topicName));
	}

	private static void setSenderConfiguration( Map<String,String> sender_config) throws Exception {
		if(!sender_config.containsKey(SENDER_HOSTNAME_CONFIG)){
			throw new Exception("Configuration file - sender section - does not contain '"+SENDER_HOSTNAME_CONFIG+"' key");
		}
		if(!sender_config.containsKey(SENDER_PORTS_CONFIG)){
			throw new Exception("Configuration file - sender section - does not contain '"+SENDER_PORTS_CONFIG+"' key");
		}
		data_endpoint_hostname = sender_config.get(SENDER_HOSTNAME_CONFIG);
		data_endpoint_port_str = sender_config.get(SENDER_PORTS_CONFIG);
		String[] data_endpoint_ports_str = data_endpoint_port_str.split(",");
        data_endpoint_ports = new int[data_endpoint_ports_str.length];
        for( int i=0; i < data_endpoint_ports_str.length; i++) {
          	data_endpoint_ports[i] = Integer.parseInt(data_endpoint_ports_str[i]);
        }
        data_endpoint_ports_size = data_endpoint_ports_str.length;
		logger.info("Data Endpoint Hostname: "+ data_endpoint_hostname);
		logger.info("Data Endpoint Port(s): " + data_endpoint_port_str);

		try {
			data_address = InetAddress.getByName(data_endpoint_hostname);
		} catch (IOException e) {
			logger.error("Error opening creation address using hostname: "+data_endpoint_hostname + ". Consumer crashed.", e);
			System.exit(1);
		}
	}

	private static void setStatsConfiguration( Map<String,String> stats_config){
		if( stats_config == null){
			stats_enabled = false;
			return;
		}
		if( !stats_config.containsKey(STATS_HOSTNAME_CONFIG) && !stats_config.containsKey(STATS_PORT_CONFIG) && !stats_config.containsKey(STATS_PERIOD_S)){
			stats_enabled = false;
			return;
		}
		if( !stats_config.containsKey(STATS_HOSTNAME_CONFIG) || !stats_config.containsKey(STATS_PORT_CONFIG) || !stats_config.containsKey(STATS_PERIOD_S)){
			stats_enabled = false;
			logger.warn("Configuration File - stats section - not all the fields ['hostname','port','period.s'] are present");
			return;
		}

		String stats_endpoint_hostname = stats_config.get(STATS_HOSTNAME_CONFIG);
		stats_endpoint_port = Integer.parseInt(stats_config.get(STATS_PORT_CONFIG));
		stats_period_ms = Integer.parseInt(stats_config.get(STATS_PERIOD_S))  * 1000;
		stats_type = DEFAULT_STATS_TYPE;
			
		try {
			datagramSocket = new DatagramSocket();
		} catch (SocketException e) {
			logger.error("Error while creating UDP socket. Internal monitoring disabled.", e);
			stats_enabled = false;
			return;
		}
		
		logger.info("Stats Endpoint Hostname: "+stats_endpoint_hostname);
		logger.info("Stats Endpoint Port: "+stats_endpoint_port);
		logger.info("Stats Period: "+stats_period_ms+" ms");
		try {
			stats_address = InetAddress.getByName(stats_endpoint_hostname);
		} catch (IOException e) {
			logger.error("Error opening creation address using hostname: "+stats_endpoint_hostname + ". Internal monitoring disabled.", e);
			stats_enabled = false;
			return;
		}
	}

	private static void run(){
		while (true) {
			try {
				ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(POLLING_PERIOD_MS);
			  	receivedRecords += consumerRecords.count();
			  	consumerRecords.forEach( record -> sendUdpData(record.value()));
			  	//consumer.commitAsync();
			  	if( stats_enabled ) stats();
			} catch (RetriableCommitFailedException e) {
				logger.error("Kafka Consumer Committ Exception: ",e);
				consumer.close();
				break;
			} catch (Exception e) {
				logger.error("Kafka Consumer Error: ",e);
				consumer.close();
				break;
			}
	  	}
	}
	
	public static void main(String[] args) throws Exception {
        startMs = System.currentTimeMillis(); 
		
        /* Parse command line argumets */
        ArgumentParser parser = argParser();
        Namespace res = parser.parseArgs(args);
        String config_filename = res.getString("config");
		importYamlConfiguration(config_filename);
		run();     
    }
	
	private static void sendUdpData(byte[] data2send) {
		if (data2send.length > 0){
			try {
				if(++data_endpoint_ports_index >= data_endpoint_ports_size) data_endpoint_ports_index = 0;
				int data_port = data_endpoint_ports[data_endpoint_ports_index];
				DatagramPacket packet = new DatagramPacket(data2send, data2send.length, data_address, data_port );
				datagramSocket.send(packet);
				sentRecords++;
		  	} catch (Exception e) {
				logger.error("Error. Index: "+data_endpoint_ports_index+" size: "+data_endpoint_ports_size+" module: "+data_endpoint_ports_index%data_endpoint_ports_size,e);
			}
	  	} else {
	    	logger.info("Message lenght zero");
	  	}
	}
	
	private static void stats() throws IOException {
		long nowMs = System.currentTimeMillis();
		if(receivedRecords < 0) receivedRecords = 0;
		if(sentRecords < 0) sentRecords = 0;
		if ( nowMs - startMs > stats_period_ms) {
			startMs = nowMs;
			String hostname = InetAddress.getLocalHost().getHostName();
			if(stats_type.equals(STATS_TYPE_INFLUXDB)) {
				String data2send = "kafka_consumer,endpoint_type=InfluxDB,endpoint="+data_endpoint_hostname+":"+data_endpoint_port_str.replace(',','|')+",hostname="+hostname+",topic="+topicName;
				data2send += " receivedRecords="+receivedRecords+"i,sentRecords="+sentRecords+"i "+nowMs+"000000";
				DatagramPacket packet = new DatagramPacket(data2send.getBytes(), data2send.length(), stats_address, stats_endpoint_port);
				datagramSocket.send(packet);
			}
		}
	}
    
	private static ArgumentParser argParser() {
		@SuppressWarnings("deprecation")
	  	ArgumentParser parser = ArgumentParsers
	 		.newArgumentParser("influxdb-udp-consumerr")
          	.defaultHelp(true)
          	.description("This tool is used to send UDP packets to InfluxDB.");

    	parser.addArgument("--config")
			.action(store())
			.required(true)
			.type(String.class)
			.dest("config")
			.help("config file");

        return parser;
    }
}	
