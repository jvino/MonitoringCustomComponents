package ch.cern.alice.o2.kafka.utils;

import java.util.List;
import java.util.Map;

public class YamlNewAggregatorConfig{
		private Map<String,String> general;
		private Map<String,String> kafka_config;
		private Map<String,String> aggregation_config;
		private List<Map<String,String>> selection;
		private Map<String,String> stats_config;
		
		public YamlNewAggregatorConfig() { }
		
		public Map<String,String> getGeneral() {
			return general;
		}
		
		public void setGeneral(Map<String,String> new_gen) {
			general = new_gen;
		}
		
		public Map<String,String> getkafka_config(){
			return kafka_config;
		}
		
		public void setkafka_config(Map<String,String> k_config) {
			kafka_config = k_config;
		}
		
		public Map<String,String> getAggregation_config(){
			return aggregation_config;
		}
		
		public void setAggregators(Map<String,String> aggr) {
			aggregation_config = aggr;
		}

		public List<Map<String,String>> getSelection(){
			return selection;
		}
		
		public void setSelection(List<Map<String,String>> new_selection) {
			selection = new_selection;
		}

		public Map<String,String> getStats_config() {
			return stats_config;
		}
		
		public void setStats_config(Map<String,String> new_gen) {
			stats_config = new_gen;
		}
}