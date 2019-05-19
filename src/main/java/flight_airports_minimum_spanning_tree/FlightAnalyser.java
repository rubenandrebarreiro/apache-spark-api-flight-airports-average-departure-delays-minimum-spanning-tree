package flight_airports_minimum_spanning_tree;

/**
*
* Apache Spark API - Flight Airports' Average Departure Delays Minimum Spanning Tree (Prim's Algorithm)
* 
* High Performance Computing
* Work Assignment/Practical Lab's Project #2
* 
* Integrated Master of Computer Science and Engineering
* Faculty of Science and Technology of New University of Lisbon
*
* Description/Steps of operations performed by the Apache Spark's API:
* - Build an Undirected Graph to represent the Average Departure Delay of the Flights between any two Airports
*   (the Average Departure Delays must consider all Flights between the both Airports, disregarding the origin and the destination).
*   The Graph's nodes are thus the Airports, and the edges represent direct routes between Airports,
*   labelled with the Average Departure Delays of the routes' flights. (STEP DONE)
*   
*   Suggestion:
*   - Represent the Graph through an Adjacency Matrix.
*   	- See https://spark.apache.org/docs/latest/mllib-data-types.html#distributed-matrix
*   - To use the Distributed Matrix of the Spark's API, you will have to add the following dependency to file build.gradle:
*      - implementation 'org.apache.spark:spark-mllib_2.11:2.3.0'
*      
* - Compute the Graph's Minimum Spanning Tree (M.S.T.) by
*   implementing the parallel version of Prim's Algorithm (available from CLIP platform).
*   The M.S.T. will be the subgraph of the original with the minimum total edge weight
*   (sum of the average delays). Output the M.S.T. and its total edge weight.
*   
* - Identify the 'Bottleneck' Airport, i.e., the Airport with higher aggregated Average Departure Delay time
*   (sum of the Average Departure Delays of all routes going out of the Airport)
*   from the ones contained in the complement Graph of the M.S.T. previously computed.
*   
* - Modify the Graph to reduce by a given factor the Average Departure Delay time of
*   all routes going out of the selected airport.
*   This factor will be a parameter of your algorithm (received in the command line) and must be a value in ]0, 1[.
*   
* - Recompute the M.S.T. and display the changes perceived in the resulting subgraph and
*   on the sum of the total edge weight
*
* Authors:
* @author Ruben Andre Barreiro - r.barreiro@campus.fct.unl.pt
*
*/

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vectors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;

import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag;

public class FlightAnalyser {

	private static final String DefaulftFile = "data/flights.csv";
	
	// IMPLEMENTATION STEPS
	
	// 1. Build an Undirected Graph to represent the Average Departure Delay of the Flights between any two Airports
	// (the Average Departure Delays must consider all Flights between the both Airports, disregarding the origin and the destination).
	// The Graph's nodes are thus the Airports, and the edges represent direct routes between Airports,
	// labelled with the Average Departure Delays of the routes' flights. (STEP DONE)
	
	// Suggestion:
	// - Represent the Graph through an Adjacency Matrix.
	//   - See https://spark.apache.org/docs/latest/mllib-data-types.html#distributed-matrix
	// - To use the Distributed Matrix of the Spark's API, you will have to add the following dependency to file build.gradle:
	//   - implementation 'org.apache.spark:spark-mllib_2.11:2.3.0'
	
	// 2. Compute the Graph's Minimum Spanning Tree (M.S.T.) by
	// implementing the parallel version of Prim's Algorithm (available from CLIP platform).
	// The M.S.T. will be the subgraph of the original with the minimum total edge weight
	// (sum of the Average Departure Delays). Output the M.S.T. and its total edge weight.
	
	// 3. Identify the 'Bottleneck' Airport, i.e., the Airport with higher aggregated Average Departure Delay time
	// (sum of the Average Departure Delays of all routes going out of the Airport)
	// from the ones contained in the complement Graph of the M.S.T. previously computed.
	
	// 4. Modify the Graph to reduce by a given factor the Average Departure Delay time of
	// all routes going out of the selected airport.
	// This factor will be a parameter of your algorithm (received in the command line) and must be a value in ]0, 1[.
	
	// 5. Recompute the M.S.T. and display the changes perceived in the resulting subgraph and
	// on the sum of the total edge weight
	
	// NOTE:
	// - Fields of the Structure of .CSV file, include:
	//   1) day_of_month - StringType
	//   2) day_of_week - StringType
	//   3) carrier - StringType
	//   4) tail_num - StringType
	//   5) flight_num - IntegerType
	//   6) origin_id - LongType
	//   7) origin_name - StringType
	//   8) destination_id - LongType
	//   9) destination_name - StringType
	//   10) scheduled_departure_time - DoubleType
	//   11) departure_real_time - DoubleType
	//   12) departure_delay - DoubleType
	//   13) scheduled_arrival_time - DoubleType
	//   14) arrival_real_time - DoubleType
	//   15) arrival_delay - DoubleType
	//   16) elapsed_time - DoubleType
	//   17) distance - IntegerType
	
	
	// Methods:
	

	/**
	 * Returns all Average Departure Delays of Flights for each route, ordered by descending of Average field.
	 * 
	 * @param flightsDataset the Dataset (built of Rows) of the flights read from the .CSV file
	 * 
	 * @return all Average Departure Delays of Flights for each route, ordered by descending of Average field
	 */
	public static Dataset<Row> getAllAirportsIDsDataset(Dataset<Row> flightsDataset) {
		Dataset<Row> allOriginAirportDataset = 
					flightsDataset.select("origin_id").withColumnRenamed("origin_id", "id");
		
		Dataset<Row> allDestinationAirportDataset = 
					flightsDataset.select("destination_id").withColumnRenamed("destination_id", "id");
		
		Dataset<Row> allAirportsDataset = 
					allOriginAirportDataset.union(allDestinationAirportDataset)
										   .distinct();
		
		return allAirportsDataset.orderBy("id");
	}
	
	
	
	/**
	 * Returns all Average Departure Delays of Flights for each route, ordered by descending of Average field.
	 * 
	 * @param flightsDataset the Dataset (built of Rows) of the flights read from the .CSV file
	 * 
	 * @return all Average Departure Delays of Flights for each route, ordered by descending of Average field
	 */
	public static Dataset<Row> getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset(Dataset<Row> flightsDataset) {
		Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset = 
					 flightsDataset.groupBy("origin_id", "destination_id")
					 			   .avg("departure_delay")
					 			   .orderBy("origin_id");
		
		return allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset;
	}
	
	/**
	 * Returns all Average Departure Delays of Flights for each route, ordered by descending of Average field,
	 * disregarding Origin and Destination, with indexes (origin_num, destination_num) to
	 * build the Coordinate Adjacency Matrix.
	 * 
	 * @param sqlContext the SQL Context to perform SQL Queries
	 * @param flightsDataset the Dataset (built of Rows) of the flights read from the .CSV file
	 * 
	 * @return all Average Departure Delays of Flights for each route, ordered by descending of Average field,
	 * 		   disregarding Origin and Destination, with indexes (origin_num, destination_num) to
	 *         build the Coordinate Adjacency Matrix
	 */
	@SuppressWarnings("deprecation")
	public static Dataset<Row> 
			getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset
					(SQLContext sqlContext, Dataset<Row> flightsDataset, Dataset<Row> allAirportsByIndexMap) {
			
			Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset = 
			 		 	 flightsDataset.groupBy("origin_id", "destination_id")
					 	 		       .avg("departure_delay")
					 			       .orderBy("origin_id");
			
			allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset.registerTempTable("all_average_delays_of_the_flights_between_any_two_airports");
			
			String sqlQueryRemovePairDuplicates = "SELECT DISTINCT * " +
					  "FROM all_average_delays_of_the_flights_between_any_two_airports t1 " +
				      "WHERE t1.origin_id > t1.destination_id " +
                      	"OR NOT EXISTS (" +
                      		"SELECT * " +
                      		"FROM all_average_delays_of_the_flights_between_any_two_airports t2 " +
                      		"WHERE t2.origin_id = t1.destination_id " + 
                      		"AND t2.destination_id = t1.origin_id" +
                      	")";
			
			Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
							  					  sqlContext.sql(sqlQueryRemovePairDuplicates);
			
			Dataset<Row> allPossibleOriginAirportsByIndexMap = allAirportsByIndexMap.withColumnRenamed("id", "origin_id").withColumnRenamed("index", "origin_index");
			
			Dataset<Row> allPossibleDestinationAirportsByIndexMap = allAirportsByIndexMap.withColumnRenamed("id", "destination_id").withColumnRenamed("index", "destination_index");
			
			allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
					allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
					.join(allPossibleOriginAirportsByIndexMap,
						  allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset.col("origin_id")
						  .equalTo(allPossibleOriginAirportsByIndexMap.col("origin_id")), "left");
			
			allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
					allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
					.join(allPossibleDestinationAirportsByIndexMap,
						  allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset.col("destination_id")
						  .equalTo(allPossibleDestinationAirportsByIndexMap.col("destination_id")), "left");
			
			allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
					allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
							.select("origin_index", "t1.origin_id", "destination_index", "t1.destination_id", "t1.avg(departure_delay)");
			
			return allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset;
	}
	
	/**
	 * Returns all Average Departure Delays of Flights for each route, ordered by descending of Average field,
	 * disregarding Origin and Destination, with indexes (origin_num, destination_num) to
	 * build the Coordinate Adjacency Matrix.
	 * 
	 * @param sqlContext the SQL Context to perform SQL Queries
	 * @param flightsDataset the Dataset (built of Rows) of the flights read from the .CSV file
	 * 
	 * @return all Average Departure Delays of Flights for each route, ordered by descending of Average field
	 * 	       with indexes (origin_num, destination_num) to build the Coordinate Adjacency Matrix
	 */
	@SuppressWarnings("deprecation")
	public static Dataset<Row> 
			getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
					(SQLContext sqlContext, Dataset<Row> flightsDataset) {
		
			Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset = 
					 	flightsDataset.groupBy("origin_id", "destination_id")
					 			   .avg("departure_delay")
					 			   .orderBy("origin_id");
		
			allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset.registerTempTable("all_average_delays_of_the_flights_between_any_two_airports");
			
			String sqlQueryRemovePairDuplicates = "SELECT DISTINCT * " +
					  "FROM all_average_delays_of_the_flights_between_any_two_airports t1 " +
				      "WHERE t1.origin_id > t1.destination_id " +
                      	"OR NOT EXISTS (" +
                      		"SELECT * " +
                      		"FROM all_average_delays_of_the_flights_between_any_two_airports t2 " +
                      		"WHERE t2.origin_id = t1.destination_id " + 
                      		"AND t2.destination_id = t1.origin_id" +
                      	")";
			
			Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
							  					  sqlContext.sql(sqlQueryRemovePairDuplicates);
			
			return allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset;
	}
	
	/**
	 * Returns the maximum number between the Origin and Destination IDs' count. 
	 * 
	 * @param allOriginAirportsByIndexMap the Map of All Origin Airports by Index
	 *        (to be used as row's index in Coordinate Adjacency Matrix)
	 * @param allDestinationAirportsByIndexMap the Map of All Destination Airports by Index
	 *        (to be used as column's index in Coordinate Adjacency Matrix)
	 * 
	 * @return the maximum number between the Origin and Destination IDs' count
	 */
	public static long getNumMaxBetweenOriginAndDestinationIDsCount
			(Dataset<Row> allOriginAirportsByIndexMap, Dataset<Row> allDestinationAirportsByIndexMap) {
		
		Dataset<Row> originIDsCount = allOriginAirportsByIndexMap.groupBy("origin_id").count().orderBy("origin_id");
		Dataset<Row> destinationIDsCount = allDestinationAirportsByIndexMap.groupBy("destination_id").count().orderBy("destination_id");
		
		long numTotalOrigins = originIDsCount.sort(functions.desc("count")).first().getLong(1);
		long numTotalDestinations = destinationIDsCount.sort(functions.desc("count")).first().getLong(1);
		
		return Long.max(numTotalOrigins, numTotalDestinations);
	}
	
	/**
	 * Returns the number of Airport IDs' count. 
	 * 
	 * @param allAirportsIDsDataset the Dataset (built of Rows) of all Airports
	 * 
	 * @return the number of Airport IDs' count
	 */
	public static long getNumAllAirports(Dataset<Row> allAirportsIDsDataset) {
		return allAirportsIDsDataset.orderBy("id").count();
	}
	
	/**
	 * Returns the Map of All Origin Airports by Index
	 * (to be used as row's index in Coordinate Adjacency Matrix).
	 * 
	 * @param flightsDataset the Dataset (built of Rows) of the flights read from the .CSV file
	 * 
	 * @return the Map of All Origin Airports by Index
	 *         (to be used as row's index in Coordinate Adjacency Matrix)
	 */
	public static Dataset<Row> mapAllOriginAirportsByIndex(Dataset<Row> flightsDataset) {
		Dataset<Row> allOriginAirportsDataset = 
				 flightsDataset.select("origin_id")
				 			   .orderBy("origin_id")
				 			   .distinct();
		
		return allOriginAirportsDataset.withColumn("origin_num", functions.row_number().over(Window.orderBy("origin_id")));
	}
	
	/**
	 * Returns the Map of All Destination Airports by Index
	 * (to be used as column's index in Coordinate Adjacency Matrix).
	 * 
	 * @param flightsDataset the Dataset (built of Rows) of the flights read from the .CSV file
	 * 
	 * @return the Map of All Destination Airports by Index
	 *         (to be used as column's index in Coordinate Adjacency Matrix)
	 */
	public static Dataset<Row> mapAllDestinationAirportsByIndex(Dataset<Row> flightsDataset) {
		Dataset<Row> allOriginAirportsDataset = 
				 flightsDataset.select("destination_id")
				 			   .orderBy("destination_id")
				 			   .distinct();
		
		return allOriginAirportsDataset.withColumn("destination_num", functions.row_number().over(Window.orderBy("destination_id")));
	}
	
	public static Dataset<Row> mapAllAirportsByIndex(Dataset<Row> allAirportsIDsDataset) {
		allAirportsIDsDataset = 
				allAirportsIDsDataset.select("id")
				 			   		 .orderBy("id")
				 			   		 .distinct();
		
		return allAirportsIDsDataset.withColumn("index", functions.row_number().over(Window.orderBy("id")));
	}
	
	/**
	 * Returns the Origin Airport index (row's index), given an Origin Airport ID.
	 * 
	 * @param sqlContext the SQL Context to perform SQL Queries
	 * @param originAirportsByIndexMap the Map of All Origin Airports by Index
	 * 		  (to be used as row's index in Coordinate Adjacency Matrix)
	 * @param originID an Origin Airport ID
	 * 
	 * @return the Origin Airport index (row's index), given an Origin Airport ID
	 */
	@SuppressWarnings("deprecation")
	public static long getOriginAirportIndex(SQLContext sqlContext, Dataset<Row> originAirportsByIndexMap, long originID) {
		
		originAirportsByIndexMap.registerTempTable("origin_airports_by_index_map");
		
		String sqlQueryFindOriginAirportByIndex = "SELECT t1.origin_num " +
				  "FROM origin_airports_by_index_map t1 " +
			      "WHERE t1.origin_id = " + originID;
		
		Dataset<Row> originAirportIndex = sqlContext.sql(sqlQueryFindOriginAirportByIndex);

		return originAirportIndex.first().getLong(0);
	}

	/**
	 * Returns the Destination Airport index (column's index), given a Destination Airport ID.
	 * 
	 * @param sqlContext the SQL Context to perform SQL Queries
	 * @param destinationAirportsByIndexMap the Map of All Destination Airports by Index
	 * 		  (to be used as column's index in Coordinate Adjacency Matrix)
	 * @param destinationID a Destination Airport ID
	 * 
	 * @return the Destination Airport index (column's index), given a Destination Airport ID
	 */
	@SuppressWarnings("deprecation")
	public static long getDestinationAirportIndex(SQLContext sqlContext, Dataset<Row> destinationAirportsByIndexMap, long destinationID) {
				
		destinationAirportsByIndexMap.registerTempTable("destination_airports_by_index_map");
		
		String sqlQueryFindDestinationAirportByIndex = "SELECT t1.destination_num " +
				  "FROM destination_airports_by_index_map t1 " +
			      "WHERE t1.destination_id = " + destinationID;
		
		Dataset<Row> destinationAirportIndex = sqlContext.sql(sqlQueryFindDestinationAirportByIndex);

		return destinationAirportIndex.first().getLong(0);
	}
	
	/**
	 * Returns the Origin Airport ID, given an Origin Airport's Number (row's index).
	 * 
	 * @param sqlContext the SQL Context to perform SQL Queries
	 * @param originAirportsByIndexMap the Map of All Origin Airports by Index
	 * 		  (to be used as row's index in Coordinate Adjacency Matrix)
	 * @param originIndex an Origin Airport's Number (row's index)
	 * 
	 * @return the Origin Airport ID, given an Origin Airport index (row's index)
	 */
	@SuppressWarnings("deprecation")
	public static long getOriginAirportID(SQLContext sqlContext, Dataset<Row> originAirportsByIndexMap, long originIndex) {
		
		originAirportsByIndexMap.registerTempTable("origin_airports_by_index_map");
		
		String sqlQueryFindOriginAirportByID = "SELECT t1.origin_id " +
				  "FROM origin_airports_by_index_map t1 " +
			      "WHERE t1.origin_num = " + originIndex;
		
		Dataset<Row> originAirportIndex = sqlContext.sql(sqlQueryFindOriginAirportByID);

		return originAirportIndex.first().getLong(0);
	}
	
	/**
	 * Returns a Java Pair RDD containing a collection (Disregarding Airport's Origin and Destination)
	 * of mappings between Airport Origin's ID and the a tuple containing other two tuples:
	 * - (row's index, column's index) of the Adjacency Coordinate Matrix
	 * - (Airport Destination's ID, Average Departure Delays of the Flights between that two Airports) 
	 * 
	 * @param allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
	 *        the Dataset (built of Rows) containing all Average Departure Delays of Flights for each route,
	 *        ordered by descending of Average field, disregarding Origin and Destination
	 * 
	 * @return a Java Pair RDD containing a collection (Disregarding Airport's Origin and Destination)
	 * 		   of mappings between Airport Origin's ID and the a tuple containing other two tuples:
	 *         - (row's index, column's index) of the Adjacency Coordinate Matrix
	 *         - (Airport Destination's ID, Average Departure Delays of the Flights between that two Airports)
	 */
	public static JavaPairRDD<Long, Tuple2<Tuple2<Long, Long>, Tuple2<Long, Double>>> 
			getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD
					(Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset) {
				
		JavaRDD<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationJavaRDD = 
				allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset.orderBy("origin_id").javaRDD().cache();
	
		JavaPairRDD<Long, Tuple2<Tuple2<Long, Long>, Tuple2<Long, Double>>> 
				averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD = 
						allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationJavaRDD.mapToPair(
							new PairFunction<Row, Long, Tuple2<Tuple2<Long, Long>, Tuple2<Long, Double>>> () {

							/**
							 * The default serial version UID
							 */
							private static final long serialVersionUID = 1L;
		 
							/**
							 * The call method to perform the map for each tuple/pair
							 */
							@Override
							public Tuple2<Long, Tuple2<Tuple2<Long, Long>, Tuple2<Long, Double>>> call(Row row) throws Exception {
	
								return new Tuple2<Long, Tuple2<Tuple2<Long, Long>, Tuple2<Long, Double>>> 
									(row.getLong(1),
									 new Tuple2<Tuple2<Long, Long>, Tuple2<Long, Double>>(new Tuple2<Long, Long>((long) row.getInt(0), (long) row.getInt(2)),
									   		 									  		  new Tuple2<Long, Double>(row.getLong(3), row.getDouble(4)))
									);
							}
						}
				);
	
		return averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD;
	}
	
	/**
	 * Returns a Java Pair RDD containing a collection (Disregarding Airport's Origin and Destination)
	 * of mappings between Airport Origin's ID and the a tuple containing:
	 * - (Airport Destination's ID, Average Departure Delays of the Flights between that two Airports) 
	 * 
	 * @param allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
	 * 		  the Dataset (built of Rows) containing all Average Departure Delays of Flights for each route,
	 *        ordered by descending of Average field, disregarding Origin and Destination
	 * 
	 * @return a Java Pair RDD containing a collection (Disregarding Airport's Origin and Destination)
	 * 		   of mappings between Airport Origin's ID and the a tuple containing:
	 * 		   - (Airport Destination's ID, Average Departure Delays of the Flights between that two Airports) 
	 */
	public static JavaPairRDD<Long, Tuple2<Long, Double>> 
			getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationJavaPairRDD
					(Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset) {
		
		JavaRDD<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationJavaRDD = 
									allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset.orderBy("origin_id").javaRDD();
		
		JavaPairRDD<Long, Tuple2<Long, Double>> averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationJavaPairRDD = 
				allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationJavaRDD.mapToPair(
						new PairFunction<Row, Long, Tuple2 <Long, Double> > () {
	
							/**
							 * The default serial version UID
							 */
							private static final long serialVersionUID = 1L;
		 
							/**
							 * The call method to perform the map for each tuple/pair
							 */
							@Override
							public Tuple2<Long, Tuple2<Long, Double>> call(Row row) throws Exception {
		
								return new Tuple2<Long, Tuple2<Long, Double>>(row.getLong(0),
										                                      new Tuple2<Long, Double>(
										                                    		  row.getLong(1), row.getDouble(2))
										                                      );
							}
						}
				);
		
		return averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationJavaPairRDD;
	}
	
	/**
	 * Returns a Java Pair RDD containing a collection (Disregarding Airport's Origin and Destination)
	 * of mappings between Airport Origin's ID and an iterable tuple containing:
	 * - (Airport Destination's ID, Average Departure Delays of the Flights between that two Airports) 
	 * 
	 * @param allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
	 * 		  the Dataset (built of Rows) containing all Average Departure Delays of Flights for each route,
	 *        ordered by descending of Average field, disregarding Origin and Destination
	 * 
	 * @return a Java Pair RDD containing a collection (Disregarding Airport's Origin and Destination)
	 * 		   of mappings between Airport Origin's ID and an iterable tuple containing:
	 * 		   - (Airport Destination's ID, Average Departure Delays of the Flights between that two Airports) 
	 */
	public static JavaPairRDD<Long, Iterable<Tuple2<Long, Double>>> 
		getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationGroupByKeyOriginJavaPairRDD(Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset) {
		
		JavaRDD<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationJavaRDD = 
									allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset.orderBy("origin_id").javaRDD();
		
		JavaPairRDD<Long, Tuple2<Long, Double>> averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationJavaPairRDD = 
				allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationJavaRDD.mapToPair(
						new PairFunction<Row, Long, Tuple2 <Long, Double> > () {

							/**
							 * The default serial version UID
							 */
							private static final long serialVersionUID = 1L;
		 
							/**
							 * The call method to perform the map for each tuple/pair
							 */
							@Override
							public Tuple2<Long, Tuple2<Long, Double>> call(Row row) throws Exception {
		
								return new Tuple2<Long, Tuple2<Long, Double>>(row.getLong(0),
										                                      new Tuple2<Long, Double>(
										                                    		  row.getLong(1), row.getDouble(2))
										                                      );
							}
						}
				);
		
		return averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationJavaPairRDD.groupByKey().sortByKey();
	}
	
	/**
	 * Returns the Adjacency Matrix (Coordinate Matrix) to represent the Graph of
	 * Average Departure Delays between all two Airports (Disregarding Airport's Origin and Destination),
	 * containing the Vertexes (Airports) and its weights (Average Departure Delays).
	 * 
	 * @param averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD
	 * @param matrixDimensions the dimensions of the Adjacency Matrix (Coordinate Matrix)
	 * 
	 * @return the Adjacency Matrix (Coordinate Matrix) to represent the Graph of
	 * 		   Average Departure Delays between all two Airports (Disregarding Airport's Origin and Destination),
	 *         containing the Vertexes (Airports) and its weights (Average Departure Delays)
	 */
	public static CoordinateMatrix buildCoordinateAdjacencyMatrix
										(JavaPairRDD< Long, Tuple2<Tuple2<Long, Long>, Tuple2<Long, Double>> > 
										 averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD,
										 long matrixDimensions) {
		
		JavaRDD<MatrixEntry> matrixEntriesJavaRDD = averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD.map(
				
				new Function<Tuple2< Long, Tuple2<Tuple2<Long, Long>, Tuple2<Long, Double>> >, MatrixEntry>() {

					/**
					 * The default serial version UID
					 */
					private static final long serialVersionUID = 1L;

					/**
					 * The call method to perform the map for each tuple/pair
					 */
					@Override
					public MatrixEntry call(Tuple2<Long, Tuple2<Tuple2<Long, Long>, Tuple2<Long, Double>>> tuple) throws Exception {
						
						long row = tuple._2()._1()._1();
						long col = tuple._2()._1()._2();
						
						double matrixEntryValue = tuple._2()._2()._2();
						
						return new MatrixEntry(row, col, matrixEntryValue);
					}
				}
		);
		
		RDD<MatrixEntry> matrixEntriesRDD = matrixEntriesJavaRDD.rdd();
		
		return new CoordinateMatrix(matrixEntriesRDD, matrixDimensions, matrixDimensions);
	}
	
	
	public static void computeMinimumSpanningTree(JavaSparkContext javaSparkContext,
												  Dataset<Row> allAirportIDsDataset,
												  CoordinateMatrix coordinateAdjacencyMatrix,
												  long numAirports) {  
	    
		JavaRDD<MatrixEntry> coordinateAdjacencyMatrixEntriesJavaRDD = coordinateAdjacencyMatrix.entries().toJavaRDD();

		JavaRDD<MatrixEntry> initialVertexMatrixEntryJavaRDD = coordinateAdjacencyMatrixEntriesJavaRDD.filter(new Function<MatrixEntry, Boolean>() {
			
			/**
			 * The default serial version UID
			 */
			private static final long serialVersionUID = 1L;

			/**
			 * The random seed to generate random values
			 */
			private Random random = new Random();
			
			/**
			 * 
			 */
			private final long initialVertexIndex = random.nextInt((int) (numAirports + 1L));
			
			
					
			@Override
			public Boolean call(MatrixEntry matrixEntry) throws Exception {
				return matrixEntry.i() == initialVertexIndex;
			}
			
		});
		
		MatrixEntry initialVertexMatrixEntry = initialVertexMatrixEntryJavaRDD.first();
		long initialVertex = initialVertexMatrixEntry.i();
		
		// The array to keep all the distances from the root of
		// the Minimum Spanning Tree (M.S.T.), initialised as INFINITE
		double[] distanceArray = new double[(int) numAirports];
		Arrays.fill(distanceArray, Double.MAX_VALUE); // TODO melhorar?
		
		// The array to represent and keep the set of
		// vertices already yet included in Minimum Spanning Tree (M.S.T.),
		// initialised as FALSE
		boolean[] visited = new boolean[(int) numAirports];	
		Arrays.fill(visited, false);
		
		// The position of the root it's initialised with the distance of 0 (ZERO)
		// to build the Minimum Spanning Tree (M.S.T.)
		distanceArray[(int) initialVertex] = 0.0;
		
		// The position of the root it's initialised as TRUE in
		// the set of vertices already yet included in Minimum Spanning Tree (M.S.T.)
		visited[(int) initialVertex] = true;
		
		JavaRDD<MatrixEntry> directPathsFromInitialVertex = coordinateAdjacencyMatrixEntriesJavaRDD.filter(new Function<MatrixEntry, Boolean>() {

			/**
			 * The default serial version UID
			 */
			private static final long serialVersionUID = 1L;
			
			/**
			 * The call method to perform the filter of all the Matrix Entries,
			 * beginning with the initial vertex as row's index 
			 */
			@Override
			public Boolean call(MatrixEntry matrixEntry) throws Exception {
				return (matrixEntry.i() == initialVertex) ? true : false;
			}
		});
		
		directPathsFromInitialVertex.foreach(new VoidFunction<MatrixEntry>() {
			
			/**
			 * The default serial version UID
			 */
			private static final long serialVersionUID = 1L;
			
			/**
			 * The call method to perform the update distance array,
			 * with the weights from the direct paths from the initial vertex's edges,
			 * filtered before
			 */
			@Override
			public void call(MatrixEntry matrixEntry) throws Exception {
				distanceArray[(int) matrixEntry.j()] = matrixEntry.value();
			}
		});
		
		JavaRDD<MatrixEntry> distancesParallelJavaDoubleRDD = javaSparkContext.parallelize(coordinateAdjacencyMatrix.transpose().entries().toJavaRDD().collect(), (int) numAirports);
				
		distancesParallelJavaDoubleRDD.foreachPartition(new VoidFunction<Iterator<MatrixEntry>>() {

			/**
			 * The default serial version UID
			 */
			private static final long serialVersionUID = 1L;

			private double minDistanceValue = Double.MAX_VALUE;
			private long minDistanceVertex = Long.MAX_VALUE;
			
			/**
			 * TODO
			 */
			@Override
			public void call(Iterator<MatrixEntry> matrixEntryIterator) throws Exception {
				while(matrixEntryIterator.hasNext()) {
					MatrixEntry matrixEntry = matrixEntryIterator.next();
					double matrixEntryValue = matrixEntry.value();
					
					if(matrixEntryValue < minDistanceValue) {
						minDistanceValue = matrixEntryValue;
						minDistanceVertex = matrixEntry.j();
					}
				}
			}
		});
		
		MatrixEntry minFromDistanceParallelJavaDoubleRDD = distancesParallelJavaDoubleRDD.reduce(new Function2<MatrixEntry, MatrixEntry, MatrixEntry>() {

			/**
			 * The default serial version UID
			 */
			private static final long serialVersionUID = 1L;

			/**
			 * 
			 */
			@Override
			public MatrixEntry call(MatrixEntry matrixEntry1, MatrixEntry matrixEntry2) throws Exception {
				return Double.min(matrixEntry1.value(), matrixEntry2.value()) == matrixEntry1.value() ? matrixEntry1 : matrixEntry2;
			}
			
		});
		
		
		/*
	    
	    // The MST will have V vertices 
	    for (int count = 0; count < V-1; count++) 
	    { 
	        // Pick the minimum key vertex from the  
	        // set of vertices not yet included in MST 
	        int u = minKey(key, mstSet); 
	  
	        // Add the picked vertex to the MST Set 
	        mstSet[u] = true; 
	  
	        // Update key value and parent index of  
	        // the adjacent vertices of the picked vertex.  
	        // Consider only those vertices which are not  
	        // yet included in MST 
	        for (int v = 0; v < V; v++) 
	  
	        // graph[u][v] is non zero only for adjacent vertices of m 
	        // mstSet[v] is false for vertices not yet included in MST 
	        // Update the key only if graph[u][v] is smaller than key[v] 
	        if (graph[u][v] && mstSet[v] == false && graph[u][v] < key[v]) 
	            parent[v] = u, key[v] = graph[u][v]; 
	    } 
	  
	    // print the constructed MST 
	    printMST(parent, V, graph);
	    
	    */ 
	}
	
	
	
	/**
	 * Main method to process the flights' file and analyse it.
	 * 
	 * @param args the file of flights to process
	 *        (if no args, uses the file flights.csv, by default)
	 */
	public static void main(String[] args) {
		
		String fileName = null;
		float reduceFactor = 0.0f;
		
		Random random = new Random();
		
		if(args.length < 1) {
			fileName = DefaulftFile;
			
			reduceFactor = 0.0f;
			
			while(reduceFactor == 0.0f)
				reduceFactor = random.nextFloat();
		}
		else {
			if(args.length == 2) {
				fileName = args[0];
				
				if(Float.parseFloat(args[1]) > 0.0 && Float.parseFloat(args[1]) < 1.0) {
					reduceFactor = Float.parseFloat(args[1]);
				}
				else {
					reduceFactor = 0.0f;
					
					while(reduceFactor == 0.0f)
						reduceFactor = random.nextFloat();
				}
			}
		}
		
		// Start Spark Session (SparkContext API may also be used) 
		// master("local") indicates local execution
		SparkSession sparkSession = SparkSession.builder().
				appName("FlightAnalyser").
				master("local[*]").
				getOrCreate();
		
		// The Spark Context
		SparkContext sparkContext = sparkSession.sparkContext();
		
		// The SQL Context
		@SuppressWarnings("deprecation")
		SQLContext sqlContext = new SQLContext(sparkContext);
		
		// Only error messages are logged from this point onward
		// comment (or change configuration) if you want the entire log
		sparkContext.setLogLevel("ERROR");
		
		// The Dataset (built of Strings) of
		// the information read from the file by the Spark Session
		Dataset<String> flightsTextFile = sparkSession.read().textFile(fileName).as(Encoders.STRING());	
		
		// The Dataset (built of Rows) of
		// the information read, previously, from the file by the Spark Session
		Dataset<Row> flightsInfo = 
				flightsTextFile.map((MapFunction<String, Row>) l -> Flight.parseFlight(l), 
				Flight.encoder()).cache();

		
		// Printing the information (for debug) 

		// All the information about the Flights,
		// organised by the corresponding fields
		for(Row flightsInfoRow : flightsInfo.collectAsList())
			System.out.println(flightsInfoRow);
		
		System.out.println();
		System.out.println();
		
		
		// The Dataset (built of Rows), containing All Airports' IDs
		Dataset<Row> allAirportsIDsDataset = getAllAirportsIDsDataset(flightsInfo).cache();
				
			
		// Printing the information (for debug) 
				
		// All the information about All Airports' IDs,
		// organised by the corresponding fields 
		System.out.println("The Number of Rows/Entries in the Dataset of All Airports:");
		System.out.println("- " + allAirportsIDsDataset.count());
				
		System.out.println();
				
		for(Row allAirportsIDsRow : allAirportsIDsDataset.collectAsList())
			System.out.println(allAirportsIDsRow);
				
		System.out.println();
		System.out.println();
				
		
		// The Dataset (built of Rows), containing All Average Departure Delays of the Flights Between Any Two Airports
		Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset = getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset(flightsInfo).cache();
		
		
		// Printing the information (for debug) 
		
		// All the information about All Average Departure Delays of the Flights Between Any Two Airports,
		// organised by the corresponding fields 
		System.out.println("The Number of Rows/Entries in the Dataset of All Average Delays Of The Flights Between Any Two Airports:");
		System.out.println("- " + allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset.count());
		
		System.out.println();
		
		for(Row allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsRow : allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset.collectAsList())
			System.out.println(allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsRow);
		
		System.out.println();
		System.out.println();
		
		
		// The Dataset (built of Rows), containing All Average Departure Delays of the Flights Between Any Airports
		// (Disregarding Origin and Destination Airports) with Indexes, organised by the corresponding fields
		Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
				getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset(sqlContext, flightsInfo).cache();

		
		// Printing the information (for debug)
		
		// The information about all Average Departure Delays of the Flights Between Any Two Airports,
		// (Disregarding Origin and Destination Airports) with Indexes, organised by the corresponding fields
		System.out.println("The Number of Rows/Entries in the Dataset of All Average Departure Delays Of The Flights Between Any Two Airports, Disregarding Origin and Destination:");
		System.out.println("- " + allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset.count());
		
		System.out.println();
		
		for(Row allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesRow : allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset.collectAsList())
			System.out.println(allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesRow);
		
		// The Map of all Airports by Index
		// (to be used as row's or column's index in Coordinate Adjacency Matrix)
		Dataset<Row> allAirportsByIndexMap = mapAllAirportsByIndex(allAirportsIDsDataset).cache();
		
		
		// Printing the information (for debug) 
		
		// The information about the Map of all Origin Airports by Index
		// (to be used as row's index in Coordinate Adjacency Matrix),
		// organised by the corresponding fields 
		for(Row allAirportsByIndexMapRow : allAirportsByIndexMap.collectAsList())
			System.out.println(allAirportsByIndexMapRow);
		
		System.out.println();
		System.out.println();
		
		
		// The Dataset (built of Rows), containing all Average Departure Delays of the Flights Between Any Airports
		// (Disregarding Origin and Destination Airports) with Indexes, organised by the corresponding fields
		// with Indexes (origin_num, destination_num) to build the Coordinate Adjacency Matrix
		Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset = 
				getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset(sqlContext, flightsInfo, allAirportsByIndexMap).cache();

		
		// Printing the information (for debug)
		
		// The information about all Average Departure Delays of the Flights Between Any Two Airports,
		// (Disregarding Origin and Destination Airports) with Indexes, organised by the corresponding fields
		// with indexes (origin_num, destination_num) build the Coordinate Adjacency Matrix
		System.out.println("The Number of Rows/Entries in the Dataset of All Average Departure Delays Of The Flights Between Any Two Airports, Disregarding Origin and Destination");
		System.out.println("[ with indexes (origin_num, destination_num) build the Coordinate Adjacency Matrix ]:");
		System.out.println("- " + allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset.count());
		
		System.out.println();
		
		// The information about all Average Departure Delays of the Flights Between Any Two Airports,
		// (Disregarding Origin and Destination Airports) with Indexes, organised by the corresponding fields
		// with indexes (origin_num, destination_num) build the Coordinate Adjacency Matrix
		for(Row allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesRow : allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset.collectAsList())
			System.out.println(allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesRow);
		
		System.out.println();
		System.out.println();
		
		
		// The Java Pair RDD containing a collection (Disregarding Airport's Origin and Destination)
		// of mappings between Airport Origin's ID and an iterable tuple containing:
		// - (Airport Destination's ID, Average Departure Delays of the Flights between that two Airports)
		JavaPairRDD<Long, Iterable<Tuple2<Long, Double>>> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationGroupByKeyOriginJavaPairRDD = 
				getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationGroupByKeyOriginJavaPairRDD
						(allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset).cache();
		
		// The information about all Average Departure Delays of the Flights Between Any Two Airports,
		// (Disregarding Origin and Destination Airports) with Indexes, organised by the corresponding fields,
		// grouped by Key (origin_id) and associated to a collection of (destination_id, average_departure_delay)
		for(Entry<Long, Iterable<Tuple2<Long, Double>>> tupleEntry : allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationGroupByKeyOriginJavaPairRDD.collectAsMap().entrySet()) {
			System.out.println("Average Departure Delays Of The Flights From Origin ID: " + tupleEntry.getKey());
		    
		    Iterable<Tuple2<Long, Double>> tupleOfDestinationIDAndAverageDepartureDelayIterable = tupleEntry.getValue();
		    
		    Iterator<Tuple2<Long, Double>> tupleOfDestinationIDAndAverageDepartureDelayIterator = 
		    											tupleOfDestinationIDAndAverageDepartureDelayIterable.iterator();
		    
		    Tuple2<Long, Double> tupleOfDestinationIDAndAverageDepartureDelay;
		    
			while(tupleOfDestinationIDAndAverageDepartureDelayIterator.hasNext()) {
				tupleOfDestinationIDAndAverageDepartureDelay = tupleOfDestinationIDAndAverageDepartureDelayIterator.next();
		    	
		    	System.out.println("- (" + tupleOfDestinationIDAndAverageDepartureDelay._1() + ", " + tupleOfDestinationIDAndAverageDepartureDelay._2 + ");");
			}
			
			System.out.println();
			System.out.println();
		}
		
		
		// The Map of all Origin Airports by Index
		// (to be used as row's index in Coordinate Adjacency Matrix)
		Dataset<Row> allOriginAirportsByIndexMap = mapAllOriginAirportsByIndex(flightsInfo).cache();
		
		
		// Printing the information (for debug) 
		
		// The information about the Map of all Origin Airports by Index
		// (to be used as row's index in Coordinate Adjacency Matrix),
		// organised by the corresponding fields 
		for(Row allOriginAirportsByIndexMapRow : allOriginAirportsByIndexMap.collectAsList())
			System.out.println(allOriginAirportsByIndexMapRow);
		
		System.out.println();
		System.out.println();
		
		
		// The Map of all Destination Airports by Index
		// (to be used as column's index in Coordinate Adjacency Matrix)
		Dataset<Row> allDestinationAirportsByIndexMap = mapAllDestinationAirportsByIndex(flightsInfo).cache();
		
		
		// Printing the information (for debug) 
		
		// The information about the Map of all Destination Airports by Index
		// (to be used as column's index in Coordinate Adjacency Matrix),
		// organised by the corresponding fields 
		for(Row allDestinationAirportsByIndexMapRow : allDestinationAirportsByIndexMap.collectAsList())
			System.out.println(allDestinationAirportsByIndexMapRow);
		
		System.out.println();
		System.out.println();
		
		
		// The Java Pair RDD containing a collection (Disregarding Airport's Origin and Destination)
		// of mappings between Airport Origin's ID and a tuple containing other two tuples:
		// - (row's index, column's index) of the Adjacency Coordinate Matrix
		// - (Airport Destination's ID, Average Departure Delays of the Flights between that two Airports)
		JavaPairRDD<Long, Tuple2<Tuple2<Long, Long>, Tuple2<Long, Double>>> 
				allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD = 
						getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD
								(allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset).cache();
		
		
		// Printing the information (for debug) 
		
		// The information about the mappings between Airport Origin's ID and a tuple containing other two tuples:
		// - (row's index, column's index) of the Adjacency Coordinate Matrix
		// - (Airport Destination's ID, Average Departure Delays of the Flights between that two Airports)
		// This information will be organised by the corresponding fields (Disregarding Airport's Origin and Destination)
		for(Row allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesRow : 
			allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset.collectAsList())
				System.out.println(allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesRow);
		
		System.out.println();
		System.out.println();
		
		// The maximum number between the Origin and Destination IDs (to be used as the dimensions of Coordinate Adjacency Matrix)
		long numAllAirports = getNumAllAirports(allAirportsIDsDataset);
		
		// The Adjacency Matrix (Coordinate Matrix) to represent the Graph of
		// Average Departure Delays between all two Airports (Disregarding Airport's Origin and Destination),
		// containing the Vertexes (Airports) and its weights (Average Departure Delays)
		CoordinateMatrix coordinateAdjacencyMatrix = 
				 buildCoordinateAdjacencyMatrix
			 				(allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD,
							 numAllAirports);
				 	
		// The Java RDD containing all the Matrix Entries of
		// the Adjacency Matrix (Coordinate Matrix) to represent the Graph of
		// Average Departure Delays between all two Airports (Disregarding Airport's Origin and Destination),
		// containing the Vertexes (Airports) and its weights (Average Departure Delays)
		JavaRDD<MatrixEntry> matrixEntryJavaRDD = coordinateAdjacencyMatrix.entries().toJavaRDD().cache();
		
		// The list containing all the Matrix Entries of
		// the Adjacency Matrix (Coordinate Matrix) to represent the Graph of
		// Average Departure Delays between all two Airports (Disregarding Airport's Origin and Destination),
		// containing the Vertexes (Airports) and its weights (Average Departure Delays)
		List<MatrixEntry> matrixEntryList = matrixEntryJavaRDD.collect();
		
		
		// Printing the information (for debug) 
		
		// The information about all the Matrix Entries of
		// the Adjacency Matrix (Coordinate Matrix) to represent the Graph of
		// Average Departure Delays between all two Airports (Disregarding Airport's Origin and Destination),
		// containing the Vertexes (Airports) and its weights (Average Departure Delays)
		for(MatrixEntry matrixEntry : matrixEntryList)
			System.out.println("(" + matrixEntry.i() + "," + matrixEntry.j() + ") = " + matrixEntry.value());
		
		System.out.println();
		System.out.println();
		
		
		// Terminate the Spark Session
		sparkSession.stop();
	}
}