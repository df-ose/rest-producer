package com.usuarios.custompartitioner;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

public class Ciudades extends DefaultPartitioner{

	
	private Map<String, Integer> usersMap;

	  public Ciudades() {
		    usersMap = new HashMap<>();
		    usersMap.put("Lima", 0);
		    usersMap.put("Tacna", 1);
		    usersMap.put("Tumbes", 2); 
		  }
	
	  
	  public Integer getpartition(String city) {
	    return usersMap.get(city);
	  }
	  
	
	
}