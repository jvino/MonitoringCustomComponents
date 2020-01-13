package ch.cern.alice.o2.kafka.connectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

class InfluxdbUdpConsumerTest {

    // private final InfluxdbUdpConsumer consumerUnderTest = new
    // InfluxdbUdpConsumer();
    String testTopicName = "mytopic";
    String boostrapServers = "localhost:9092";
    String senderHostname = "localhost";
    String senderPorts = "1234,1235,1236";
    String statsHostname = "localhost";
    String statsPorts = "1111";
    String statsPeriodS = "15";

    @Test
    void generalConfigurationBadLogKey() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        Map<String, String> conf = new HashMap<String, String>();
        conf.put(InfluxdbUdpConsumer.GENERAL_LOG4J_CONFIG + "#", "fakeFile");
        Exception exception = assertThrows(Exception.class, () -> consumerUnderTest.setGeneralConfiguration(conf));
        assertEquals("Configuration file - general section - does not contain '"
                + InfluxdbUdpConsumer.GENERAL_LOG4J_CONFIG + "' key", exception.getMessage());
    }

    @Test
    void generalConfigurationNoLogFile() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        Map<String, String> conf = new HashMap<String, String>();
        conf.put(InfluxdbUdpConsumer.GENERAL_LOG4J_CONFIG, "fakeFile");
        IOException exception = assertThrows(IOException.class, () -> consumerUnderTest.setGeneralConfiguration(conf));
        assertEquals("Log configuration file 'fakeFile' does not exist", exception.getMessage());
    }

    @Test
    void generalConfigurationOK() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        Map<String, String> conf = new HashMap<String, String>();
        int num = (int) (Math.random() * 100000);
        String filename = "/tmp/fakelogfile-" + num;
        File logFile = new File(filename);
        FileWriter myWriter;
        try {
            myWriter = new FileWriter(filename);
            myWriter.write("...");
            myWriter.close();
            conf.put(InfluxdbUdpConsumer.GENERAL_LOG4J_CONFIG, filename);
            assertTrue(consumerUnderTest.setGeneralConfiguration(conf));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        if (logFile.exists())
            logFile.delete();
    }

    @Test
    void kafkaConfigurationNull() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        String expectedMsg = "Configuration file - kafka section - must be present";
        Exception exception = assertThrows(Exception.class, () -> consumerUnderTest.setKafkaConfiguration(null));
        assertEquals(expectedMsg, exception.getMessage());
    }

    @Test
    void kafkaConfigurationBadBoostrapKey() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        Map<String, String> conf = new HashMap<String, String>();
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + "#", boostrapServers);
        String expectedMsg = "Configuration file - kafka section - does not contain '"
                + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + "' key";
        Exception exception = assertThrows(Exception.class, () -> consumerUnderTest.setKafkaConfiguration(conf));
        assertEquals(expectedMsg, exception.getMessage());
    }

    @Test
    void kafkaConfigurationBadTopicKey() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        Map<String, String> conf = new HashMap<String, String>();
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        conf.put(InfluxdbUdpConsumer.KAFKA_TOPIC_CONFIG + "#", testTopicName);
        String expectedMsg = "Configuration file - kafka section - does not contain '"
                + InfluxdbUdpConsumer.KAFKA_TOPIC_CONFIG + "' key";
        Exception exception = assertThrows(Exception.class, () -> consumerUnderTest.setKafkaConfiguration(conf));
        assertEquals(expectedMsg, exception.getMessage());
    }

    @Test
    void kafkaConfigurationOkTopicKey() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        Map<String, String> conf = new HashMap<String, String>();
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        conf.put(InfluxdbUdpConsumer.KAFKA_TOPIC_CONFIG, testTopicName);
        try {
            consumerUnderTest.setKafkaConfiguration(conf);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        assertEquals(testTopicName, consumerUnderTest.getInputTopic());
    }

    @Test
    void kafkaConfigurationPropDefault() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        Map<String, String> conf = new HashMap<String, String>();
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        conf.put(InfluxdbUdpConsumer.KAFKA_TOPIC_CONFIG, testTopicName);
        try {
            consumerUnderTest.setKafkaConfiguration(conf);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        Properties returnedKafkaPros = consumerUnderTest.getKafkaProp();
        Properties expectedKafkaProp = new Properties();
        expectedKafkaProp.put(ConsumerConfig.GROUP_ID_CONFIG, InfluxdbUdpConsumer.DEFAULT_GROUP_ID_CONFIG);
        expectedKafkaProp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                InfluxdbUdpConsumer.DEFAULT_ENABLE_AUTO_COMMIT_CONFIG);
        expectedKafkaProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        expectedKafkaProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, InfluxdbUdpConsumer.DEFAULT_AUTO_OFFSET_RESET);
        expectedKafkaProp.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, InfluxdbUdpConsumer.DEFAULT_FETCH_MIN_BYTES);
        expectedKafkaProp.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, InfluxdbUdpConsumer.DEFAULT_RECEIVE_BUFFER_BYTES);
        expectedKafkaProp.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, InfluxdbUdpConsumer.DEFAULT_MAX_POLL_RECORDS);
        expectedKafkaProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        expectedKafkaProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        assertEquals(returnedKafkaPros, expectedKafkaProp);
    }

    @Test
    void kafkaConfigurationProp() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        Map<String, String> conf = new HashMap<String, String>();
        String groupId = "myGroup";
        String enableAutoCommit = "false";
        String autoOffsetReset = "earliest";
        String fetchMinBytes = "1234";
        String receiveBufferBytes = "123456";
        String maxPollRecords = "12345";
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        conf.put(InfluxdbUdpConsumer.KAFKA_TOPIC_CONFIG, testTopicName);
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        conf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        conf.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        conf.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, receiveBufferBytes);
        conf.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);

        try {
            consumerUnderTest.setKafkaConfiguration(conf);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        Properties returnedKafkaPros = consumerUnderTest.getKafkaProp();
        Properties expectedKafkaProp = new Properties();
        expectedKafkaProp.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        expectedKafkaProp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        expectedKafkaProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        expectedKafkaProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        expectedKafkaProp.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        expectedKafkaProp.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, receiveBufferBytes);
        expectedKafkaProp.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        expectedKafkaProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        expectedKafkaProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        assertEquals(returnedKafkaPros, expectedKafkaProp);
    
    
    }

    @Test
    void senderConfigurationNull() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        String expectedMsg = "Configuration file - sender section - must be present";
        Exception exception = assertThrows(Exception.class, () -> consumerUnderTest.setSenderConfiguration(null));
        assertEquals(expectedMsg, exception.getMessage());
    }

    @Test
    void senderConfigurationBadHostnameKey() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        Map<String, String> conf = new HashMap<String, String>();
        conf.put(InfluxdbUdpConsumer.SENDER_HOSTNAME_CONFIG + "#", senderHostname);
        String expectedMsg = "Configuration file - sender section - does not contain '"
                + InfluxdbUdpConsumer.SENDER_HOSTNAME_CONFIG + "' key";
        Exception exception = assertThrows(Exception.class, () -> consumerUnderTest.setSenderConfiguration(conf));
        assertEquals(expectedMsg, exception.getMessage());
    }

    @Test
    void senderConfigurationBadPortKey() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        Map<String, String> conf = new HashMap<String, String>();
        conf.put(InfluxdbUdpConsumer.SENDER_HOSTNAME_CONFIG, senderHostname);
        conf.put(InfluxdbUdpConsumer.SENDER_PORTS_CONFIG + "#", senderPorts);
        String expectedMsg = "Configuration file - sender section - does not contain '"
                + InfluxdbUdpConsumer.SENDER_PORTS_CONFIG + "' key";
        Exception exception = assertThrows(Exception.class, () -> consumerUnderTest.setSenderConfiguration(conf));
        assertEquals(expectedMsg, exception.getMessage());
    }

    @Test
    void senderConfigurationOK() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        Map<String, String> conf = new HashMap<String, String>();
        conf.put(InfluxdbUdpConsumer.SENDER_HOSTNAME_CONFIG, senderHostname);
        conf.put(InfluxdbUdpConsumer.SENDER_PORTS_CONFIG, senderPorts);
        try {
            consumerUnderTest.setSenderConfiguration(conf);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        int[] expertedSenderPorts = new int[]{1234,1235,1236};
        assertEquals(senderHostname, consumerUnderTest.getSenderHostname());
        assertArrayEquals(expertedSenderPorts,consumerUnderTest.getSenderPorts());
    }

    @Test
    void statsConfigurationNull() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        Map<String, String> conf = null;
        try {
            consumerUnderTest.setStatsConfiguration(conf);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        assertTrue( ! consumerUnderTest.getStatsEnable());
    }

    @Test
    void statsConfigurationNoDataProvided() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        Map<String, String> conf = new HashMap<String, String>();
        try {
            consumerUnderTest.setStatsConfiguration(conf);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        assertTrue( !consumerUnderTest.getStatsEnable());
    }

    @Test
    void statsConfigurationPartialDataProvided() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        Map<String, String> conf = new HashMap<String, String>();
        conf.put(InfluxdbUdpConsumer.STATS_HOSTNAME_CONFIG, statsHostname);
        conf.put(InfluxdbUdpConsumer.STATS_PORT_CONFIG, statsPorts);
        try {
            consumerUnderTest.setStatsConfiguration(conf);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        assertTrue( !consumerUnderTest.getStatsEnable());
    }

    @Test
    void statsConfigurationOK() {
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        Map<String, String> conf = new HashMap<String, String>();
        conf.put(InfluxdbUdpConsumer.STATS_HOSTNAME_CONFIG, statsHostname);
        conf.put(InfluxdbUdpConsumer.STATS_PORT_CONFIG, statsPorts);
        conf.put(InfluxdbUdpConsumer.STATS_PERIOD_S, statsPeriodS);
        try {
            consumerUnderTest.setStatsConfiguration(conf);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        assertTrue( consumerUnderTest.getStatsEnable());
        assertEquals(statsHostname, consumerUnderTest.getStatsHostname(), "Stats Hostname not well configured");
        assertEquals(statsPorts, consumerUnderTest.getStatsPort(), "Stats port not well configured");
        assertEquals(statsPeriodS+"000", consumerUnderTest.getStatsPeriodMs(), "Stats period not well configured");
    }

    @Test
    void YamlConfigurationNoStats() {
        int num = (int) (Math.random() * 100000);
        String yamlFilename = "/tmp/fake--yamlFile-" + num;
        String logFilename = yamlFilename;
        File yamlFile = new File(yamlFilename);
        FileWriter myWriter;
        try {
            myWriter = new FileWriter(yamlFilename);
            myWriter.write("general:\n  log4jfilename: "+logFilename+"\n\n");
            myWriter.write("kafka_consumer_config:\n  topic: inputTopic\n  bootstrap.servers: localhost:9092\n\n");
            myWriter.write("sender_config:\n  hostname: localhost\n  ports: 1234,1235,1236\n\n");
            myWriter.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        try {
            consumerUnderTest.importYamlConfiguration(yamlFilename);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        Map<String, String> expectedGenConf = new HashMap<String, String>();
        expectedGenConf.put(InfluxdbUdpConsumer.GENERAL_LOG4J_CONFIG, logFilename);
        Map<String, String> expectedKafkaConf = new HashMap<String, String>();
        expectedKafkaConf.put(InfluxdbUdpConsumer.KAFKA_TOPIC_CONFIG, "inputTopic");
        expectedKafkaConf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        Map<String, String> expectedSenderConf = new HashMap<String, String>();
        expectedSenderConf.put(InfluxdbUdpConsumer.SENDER_HOSTNAME_CONFIG, "localhost");
        expectedSenderConf.put(InfluxdbUdpConsumer.SENDER_PORTS_CONFIG, "1234,1235,1236");
        assertEquals(expectedGenConf, consumerUnderTest.getGeneralConfig());
        assertEquals(expectedKafkaConf, consumerUnderTest.getKafkaConfig());
        assertEquals(expectedSenderConf, consumerUnderTest.getSenderConfig());
        assertTrue( !consumerUnderTest.getStatsEnable());
        if (yamlFile.exists()) yamlFile.delete();
    }

    @Test
    void YamlConfigurationOK() {
        int num = (int) (Math.random() * 100000);
        String yamlFilename = "/tmp/fake--yamlFile-" + num;
        String logFilename = yamlFilename;
        File yamlFile = new File(yamlFilename);
        FileWriter myWriter;
        try {
            myWriter = new FileWriter(yamlFilename);
            myWriter.write("general:\n  log4jfilename: "+logFilename+"\n\n");
            myWriter.write("kafka_consumer_config:\n  topic: inputTopic\n  bootstrap.servers: localhost:9092\n\n");
            myWriter.write("sender_config:\n  hostname: localhost\n  ports: 1234,1235,1236\n\n");
            myWriter.write("stats_config:\n  hostname: localhost\n  port: 1111\n  period.s: 25\n");
            myWriter.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();
        try {
            consumerUnderTest.importYamlConfiguration(yamlFilename);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        Map<String, String> expectedGenConf = new HashMap<String, String>();
        expectedGenConf.put(InfluxdbUdpConsumer.GENERAL_LOG4J_CONFIG, logFilename);
        Map<String, String> expectedKafkaConf = new HashMap<String, String>();
        expectedKafkaConf.put(InfluxdbUdpConsumer.KAFKA_TOPIC_CONFIG, "inputTopic");
        expectedKafkaConf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        Map<String, String> expectedSenderConf = new HashMap<String, String>();
        expectedSenderConf.put(InfluxdbUdpConsumer.SENDER_HOSTNAME_CONFIG, "localhost");
        expectedSenderConf.put(InfluxdbUdpConsumer.SENDER_PORTS_CONFIG, "1234,1235,1236");
        Map<String, String> expectedStatsConf = new HashMap<String, String>();
        expectedStatsConf.put(InfluxdbUdpConsumer.STATS_HOSTNAME_CONFIG, "localhost");
        expectedStatsConf.put(InfluxdbUdpConsumer.STATS_PORT_CONFIG, "1111");
        expectedStatsConf.put(InfluxdbUdpConsumer.STATS_PERIOD_S, "25");
        assertEquals(expectedGenConf, consumerUnderTest.getGeneralConfig());
        assertEquals(expectedKafkaConf, consumerUnderTest.getKafkaConfig());
        assertEquals(expectedSenderConf, consumerUnderTest.getSenderConfig());
        assertEquals(expectedStatsConf, consumerUnderTest.getStatsConfig());
        assertTrue( consumerUnderTest.getStatsEnable());
        if (yamlFile.exists()) yamlFile.delete();
    }






}