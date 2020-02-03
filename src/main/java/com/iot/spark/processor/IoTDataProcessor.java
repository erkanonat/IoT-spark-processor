
package com.iot.spark.processor;

import com.iot.spark.dto.IoTData;
import com.iot.spark.util.IoTDataDecoder;
import com.iot.spark.util.PropertyFileReader;

import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.*;


public class IoTDataProcessor {
	
	 private static final Logger logger = Logger.getLogger(IoTDataProcessor.class);
	
	 public static void main(String[] args) throws Exception {

	     //read Spark and Cassandra properties and create SparkConf
		 Properties prop = PropertyFileReader.readPropertyFile();
		 SparkConf conf = new SparkConf()
				 .setAppName(prop.getProperty("com.iot.app.spark.app.name"))
				 .setMaster(prop.getProperty("com.iot.app.spark.master"))
				 .set("spark.cassandra.connection.host", prop.getProperty("com.iot.app.cassandra.host"))
				 .set("spark.cassandra.connection.port", prop.getProperty("com.iot.app.cassandra.port"))
				 .set("spark.cassandra.connection.keep_alive_ms", prop.getProperty("com.iot.app.cassandra.keep_alive"));		 
		 //batch interval of 5 seconds for incoming stream		 
		 JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));	
		 //add check point directory
		 jssc.checkpoint(prop.getProperty("com.iot.app.spark.checkpoint.dir"));
		 
		 //read and set Kafka properties
		 Map<String, String> kafkaParams = new HashMap<String, String>();
		 kafkaParams.put("zookeeper.connect", prop.getProperty("com.iot.app.kafka.zookeeper"));
		 kafkaParams.put("metadata.broker.list", prop.getProperty("com.iot.app.kafka.brokerlist"));
		 String topic = prop.getProperty("com.iot.app.kafka.topic");
		 Set<String> topicsSet = new HashSet<String>();
		 topicsSet.add(topic);
		 //create direct kafka stream
		 JavaPairInputDStream<String, IoTData> directKafkaStream = KafkaUtils.createDirectStream(
			        jssc,
			        String.class,
			        IoTData.class,
			        StringDecoder.class,
			        IoTDataDecoder.class,
			        kafkaParams,
			        topicsSet
			    );
		 logger.info("Starting Stream Processing");
		 

		 JavaDStream<IoTData> iotDataStream = directKafkaStream.map(tuple -> tuple._2());

         iotDataStream.cache();

		 //process data
		 com.iot.spark.processor.IoTTrafficDataProcessor iotTrafficProcessor = new com.iot.spark.processor.IoTTrafficDataProcessor();
		 iotTrafficProcessor.processTotalTrafficData(iotDataStream);
		 iotTrafficProcessor.processWindowTrafficData(iotDataStream);
		 iotTrafficProcessor.controlAnomally(iotDataStream);
//		 iotTrafficProcessor.calculateAverageSpeed(iotDataStream);
		 
		 //start context
		 jssc.start();            
		 jssc.awaitTermination();  
  }

          
}
