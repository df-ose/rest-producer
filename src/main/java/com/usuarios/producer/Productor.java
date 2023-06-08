package com.usuarios.producer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.usuarios.custompartitioner.Ciudades;
import com.usuarios.data.Datos;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;



@SpringBootApplication
@Configuration
@EnableKafka

public class Productor {


	 @Value("${broker1}")
     private String broker1;
	 @Value("${port1}")
	 private String port1;
	 @Value("${broker2}")
     private String broker2;
	 @Value("${port2}")
	 private String port2;
	 @Value("${broker3}")
     private String broker3;
	 @Value("${port3}")
	 private String port3;	 
	 @Value("${topico1}")
     private String topico1;
	 Properties configuracion = new Properties();
	 DateFormat dtFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS");
	 Ciudades ciudades = new Ciudades();
	 String topic ;
	 String mensaje;
	 private  final Logger LOG = LoggerFactory.getLogger("Producer1Application");


	 private void opciones(Properties config) {
		 
		 //config.put("bootstrap.servers", broker1+","+broker2+","+broker3);
		 //config.put("bootstrap.servers", broker1+":"+port1+","+broker2+":"+port2","+broker3+":"+port3);
		 config.put("bootstrap.servers", broker1+":"+port1+","+broker2+":"+port2+","+broker3+":"+port3);
		 config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 config.put("partitioner.class",
			        "com.usuarios.custompartitioner.Ciudades");
	    }

	 @Autowired
	 ObjectMapper objectMapper;

	 public String ejecutar (Datos Events) throws JsonProcessingException {




	     Integer key = Events.geteventId();
	     String ciudad = Events.getcity();
	     String datain = objectMapper.writeValueAsString(Events);



	     Integer partitionid;

	     if (ciudad.equals("Lima"))

	     {   partitionid= ciudades.getpartition(ciudad);  }
	     else
	     {partitionid=0;}

	     if (ciudad.equals("Tumbes"))

	     {   partitionid= ciudades.getpartition(ciudad);  }


	     if (ciudad.equals("Tacna"))

	     {   partitionid= ciudades.getpartition(ciudad);  }



	     opciones(configuracion);


		 KafkaProducer<String, String> myProducer = new KafkaProducer<String, String>(configuracion);

	        topic = topico1;



	        int numberOfRecords = 1; // numero de mensajes enviados
	        long sleepTimer = 0; // timepo de espera entre cada envio de mensajes
	        mensaje ="";
	        int nb=0;

	        try {
	                for (int i = 0; i < numberOfRecords; i++ )
	                	{nb=i;
	                	mensaje = String.format("Mensaje  %s :  es entregado a la fecha %s", Integer.toString(nb), dtFormat.format(new Date()));
	                    myProducer.send(new ProducerRecord<String, String>(topic,partitionid,ciudad,datain));
	                    LOG.info( datain);
	                    mensaje= datain;
						LOG.info(mensaje);
	                    Thread.sleep(sleepTimer);}
	                    // Thread.sleep(new Random(5000).nextLong()); // envio de mensajes con tiempo de espera aleatorio
	        } catch (Exception e) {
	            e.printStackTrace();
	            LOG.info("error de ingreso de data");
	        } finally {
	            myProducer.close();
	        }

	        return mensaje;
	 }



}
