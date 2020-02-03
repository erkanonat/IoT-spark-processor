# iot-spark-processor

This is simple maven project . 
The project, use spark-streaming api in order to analyze iot events which consumed from kafka broker ("iot-data-event" topic).
Then process these raw data  , by using spark streaming api with high level transformation methods.
After that, writes results into the cassandra db. 

this project performing 4 different analyzing operation
  1. calculate total traffic data (group iot events with respect to vehicle types )
  2. calculate window traffic data  (group iot events with respect to vehicle types in window based )
  3. calculate average speed of cars in each PTS sensor.
  4. control anomally events which controls duplicates plate numbers which seen in different PTS sensors in same time interval.
  
  
 architecture diagram of  iot-traffic-monitoring system

![Alt text](/src/main/resources/architecture.png?raw=true "architecture")  



![Alt text](/src/main/resources/spark-rdd.png?raw=true "spark processing RDD")  
