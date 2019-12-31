package com.iot.spark.processor;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.common.base.Optional;
import com.iot.spark.dto.AggregateKey;
import com.iot.spark.dto.IoTData;
import com.iot.spark.entity.AnomallyTrafficData;
import com.iot.spark.entity.AverageSpeedTrafficData;
import com.iot.spark.entity.TotalTrafficData;
import com.iot.spark.entity.WindowTrafficData;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import scala.Tuple3;
import scala.util.parsing.combinator.testing.Str;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;


public class IoTTrafficDataProcessor implements Serializable {
	private static final Logger logger = Logger.getLogger(IoTTrafficDataProcessor.class);


	public void calculateAverageSpeed(JavaDStream<IoTData> filteredIotDataStream) {

		JavaPairDStream<String, Double> averages = filteredIotDataStream.mapToPair(iot-> new Tuple2<String,List<Double>>(iot.getPtsId(),Arrays.asList(new Double[]{iot.getSpeed()})))
				.reduceByKeyAndWindow((new Function2<Tuple2<String, List<Double>>, Tuple2<String, List<Double>>, Tuple2<String, List<Double>>>() {
					@Override
					public Tuple2<String, List<Double>> call(Tuple2<String, List<Double>> first, Tuple2<String, List<Double>> second) throws Exception {
							first._2.addAll(second._2);
							return new Tuple2<String, List<Double>>(first._1, first._2 ) ;
			}
		}, Durations.seconds(600),Durations.seconds(30))
				.map(new Function<Tuple2<String, List<Double>>, Tuple2<String,Double>>() {
			@Override
			public Tuple2<String, Double> call(Tuple2<String, List<Double>> data) throws Exception {

				String pts = data._1;
				Double sum = 0d;
				for(int i=0;i<data._2.size();i++) {
					sum += data._2.get(i);
				}
				Double average = sum / data._2.size();
				return new Tuple2<>(pts,average);
			}
		})
		;

		// Transform to dstream of TrafficData
		JavaDStream<AverageSpeedTrafficData> averageSpeedTrafficDStream = averages.map(new Function<Tuple2<String, Double>,
				AverageSpeedTrafficData>() {
			@Override
			public AverageSpeedTrafficData call(Tuple2<String, Double> item) throws Exception {
				AverageSpeedTrafficData data = new AverageSpeedTrafficData();
				data.setPtsId(item._1);
				data.setAverageSpeed(item._2);
				data.setTimeStamp(new Date());
				data.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
				return data;
			}
		});

		// Map Cassandra table column
		Map<String, String> columnNameMappings = new HashMap<String, String>();

		columnNameMappings.put("ptsId", "ptsid");
		columnNameMappings.put("averageSpeed", "averagespeed");
		columnNameMappings.put("timeStamp", "timestamp");
		columnNameMappings.put("recordDate", "recorddate");

		// call CassandraStreamingJavaUtil function to save in DB
		javaFunctions(averageSpeedTrafficDStream).writerBuilder("traffickeyspace", "average_speed_traffic",
				CassandraJavaUtil.mapToRow(AverageSpeedTrafficData.class, columnNameMappings)).saveToCassandra();
	}


	public void controlAnomally(JavaDStream<IoTData> filteredIotDataStream) {

		// reduce by key and window (10 minutes window and 30 sec slide).
		JavaPairDStream<String, String> countDStreamPair = filteredIotDataStream
				.mapToPair(iot -> new Tuple2<>( iot.getPlateNumber(),iot.getPtsId()))
				.reduceByKeyAndWindow(new Function2<String, String, String>() {
					@Override
					public String call(String s1, String s2) throws Exception {
						if(!s1.contains(s2)){
							return s1+","+s2;
						}else {
							return s1;
						}
					}
				}, Durations.seconds(600), Durations.seconds(30))
				.filter(new Function<Tuple2<String, String>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String, String> stringLongTuple2) throws Exception {
						return stringLongTuple2._2.split(",").length > 1 ;
					}
				})
				;

		// Transform to dstream of TrafficData
		JavaDStream<AnomallyTrafficData> trafficDStream = countDStreamPair.map(new Function<Tuple2<String, String>,
				AnomallyTrafficData>() {
			@Override
			public AnomallyTrafficData call(Tuple2<String, String> tuple) throws Exception {
				AnomallyTrafficData result = new AnomallyTrafficData();
				result.setPlateNumber(tuple._1);
				result.setDuplicates(tuple._2);
				result.setTotalCount(tuple._2.split(",").length);
				result.setTimeStamp(new Date());
				result.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
				return result;
			}
		});

		// Map Cassandra table column
		Map<String, String> columnNameMappings = new HashMap<String, String>();

		columnNameMappings.put("plateNumber", "platenumber");
		columnNameMappings.put("recordDate", "recorddate");
		columnNameMappings.put("duplicates", "duplicates");
		columnNameMappings.put("timeStamp", "timestamp");
		columnNameMappings.put("totalCount", "totalcount");


		// call CassandraStreamingJavaUtil function to save in DB
		javaFunctions(trafficDStream).writerBuilder("traffickeyspace", "anomally_traffic_data",
				CassandraJavaUtil.mapToRow(AnomallyTrafficData.class, columnNameMappings)).saveToCassandra();

	}

	public void processTotalTrafficData(JavaDStream<IoTData> filteredIotDataStream) {

		// We need to get count of vehicle group by ptsId and vehicleType
		JavaPairDStream<AggregateKey, Long> countDStreamPair = filteredIotDataStream
				.mapToPair(iot -> new Tuple2<>(new AggregateKey(iot.getPtsId(), iot.getVehicleType()), 1L))
				.reduceByKey((a, b) -> a + b);
		
		// Need to keep state for total count
		JavaMapWithStateDStream<AggregateKey, Long, Long, Tuple2<AggregateKey, Long>> countDStreamWithStatePair = countDStreamPair
				.mapWithState(StateSpec.function(totalSumFunc).timeout(Durations.seconds(3600)));//maintain state for one hour

		// Transform to dstream of TrafficData
		JavaDStream<Tuple2<AggregateKey, Long>> countDStream = countDStreamWithStatePair.map(tuple2 -> tuple2);
		JavaDStream<TotalTrafficData> trafficDStream = countDStream.map(totalTrafficDataFunc);

		// Map Cassandra table column
		Map<String, String> columnNameMappings = new HashMap<String, String>();
		columnNameMappings.put("ptsId", "ptsid");
		columnNameMappings.put("vehicleType", "vehicletype");
		columnNameMappings.put("totalCount", "totalcount");
		columnNameMappings.put("timeStamp", "timestamp");
		columnNameMappings.put("recordDate", "recorddate");

		// call CassandraStreamingJavaUtil function to save in DB
		javaFunctions(trafficDStream).writerBuilder("traffickeyspace", "total_traffic",
				CassandraJavaUtil.mapToRow(TotalTrafficData.class, columnNameMappings)).saveToCassandra();
	}

	/**
	 * Method to get window traffic counts of different type of vehicles for each route.
	 * Window duration = 30 seconds and Slide interval = 10 seconds
	 * 
	 * @param filteredIotDataStream IoT data stream
	 */
	public void processWindowTrafficData(JavaDStream<IoTData> filteredIotDataStream) {

		// reduce by key and window (30 sec window and 10 sec slide).
		JavaPairDStream<AggregateKey, Long> countDStreamPair = filteredIotDataStream
				.mapToPair(iot -> new Tuple2<>(new AggregateKey(iot.getPtsId(), iot.getVehicleType()), 1L))
				.reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(30), Durations.seconds(10));

		// Transform to dstream of TrafficData
		JavaDStream<WindowTrafficData> trafficDStream = countDStreamPair.map(windowTrafficDataFunc);

		// Map Cassandra table column
		Map<String, String> columnNameMappings = new HashMap<String, String>();
		columnNameMappings.put("ptsId", "ptsid");
		columnNameMappings.put("vehicleType", "vehicletype");
		columnNameMappings.put("totalCount", "totalcount");
		columnNameMappings.put("timeStamp", "timestamp");
		columnNameMappings.put("recordDate", "recorddate");

		// call CassandraStreamingJavaUtil function to save in DB
		javaFunctions(trafficDStream).writerBuilder("traffickeyspace", "window_traffic",
				CassandraJavaUtil.mapToRow(WindowTrafficData.class, columnNameMappings)).saveToCassandra();
	}

	
	//Function to get running sum by maintaining the state
	private static final Function3<AggregateKey, Optional<Long>, State<Long>,Tuple2<AggregateKey,Long>> totalSumFunc = (key,currentSum,state) -> {
        long totalSum = currentSum.or(0L) + (state.exists() ? state.get() : 0);
        Tuple2<AggregateKey, Long> total = new Tuple2<>(key, totalSum);
        state.update(totalSum);
        return total;
    };
    
    //Function to create TotalTrafficData object from IoT data
    private static final Function<Tuple2<AggregateKey, Long>, TotalTrafficData> totalTrafficDataFunc = (tuple -> {
		logger.debug("Total Count : " + "key " + tuple._1().getPtsId() + "-" + tuple._1().getVehicleType() + " value "+ tuple._2());
		TotalTrafficData trafficData = new TotalTrafficData();
		trafficData.setPtsId(tuple._1().getPtsId());
		trafficData.setVehicleType(tuple._1().getVehicleType());
		trafficData.setTotalCount(tuple._2());
		trafficData.setTimeStamp(new Date());
		trafficData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
		return trafficData;
	});

	//Function to create WindowTrafficData object from IoT data
	private static final Function<Tuple2<AggregateKey, Long>, WindowTrafficData> windowTrafficDataFunc = (tuple -> {
		logger.debug("Window Count : " + "key " + tuple._1().getPtsId() + "-" + tuple._1().getVehicleType()+ " value " + tuple._2());
		WindowTrafficData trafficData = new WindowTrafficData();
		trafficData.setPtsId(tuple._1().getPtsId());
		trafficData.setVehicleType(tuple._1().getVehicleType());
		trafficData.setTotalCount(tuple._2());
		trafficData.setTimeStamp(new Date());
		trafficData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
		return trafficData;
	});



}
