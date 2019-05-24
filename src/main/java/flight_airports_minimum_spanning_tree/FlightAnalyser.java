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
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.ReduceFunction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class FlightAnalyser {

	private static final String DefaulftFile = "data/flights.csv";
	
	private static StructType coordinateAdjacencyMatrixEntriesDatasetSchema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("row_index",  DataTypes.IntegerType, true),
            DataTypes.createStructField("column_index",  DataTypes.IntegerType, true),
            DataTypes.createStructField("distance", DataTypes.DoubleType, true)
    });
	
	private static StructType distancesVisitedDatasetSchema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("index",  DataTypes.IntegerType, true),
            DataTypes.createStructField("from_index",  DataTypes.IntegerType, true),
            DataTypes.createStructField("distance", DataTypes.DoubleType, true),
            DataTypes.createStructField("visited", DataTypes.BooleanType, true)
    });
	
	private static StructType minimumSpanningTreeDatasetSchema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("origin",  DataTypes.IntegerType, true),
            DataTypes.createStructField("destination",  DataTypes.IntegerType, true),
            DataTypes.createStructField("distance", DataTypes.DoubleType, true)
    });
	
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
	// (sum of the Average Departure Delays). Output the M.S.T. and its total edge weight. (PARTIALLY DONE - TODO verify)
	
	// 3. Identify the 'Bottleneck' Airport, i.e., the Airport with higher aggregated Average Departure Delay time
	// (sum of the Average Departure Delays of all routes going out of the Airport)
	// from the ones contained in the complement Graph of the M.S.T. previously computed. (DONE - TODO verify)
	
	// 4. Modify the Graph to reduce by a given factor the Average Departure Delay time of
	// all routes going out of the selected airport.
	// This factor will be a parameter of your algorithm (received in the command line) and must be a value in ]0, 1[. (DONE - TODO verify)
	
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
		
		RDD<MatrixEntry> matrixEntriesRDD = matrixEntriesJavaRDD.rdd().cache();
		
		return new CoordinateMatrix(matrixEntriesRDD, matrixDimensions, matrixDimensions);
	}
	
	/*
	public static Dataset<Row> getMinRowInDistancesVisitedDataset(SparkSession sparkSession, Dataset<Row> distancesVisitedDataset) {
		Row minDistanceVisitedDataset = distancesVisitedDataset.reduce(new ReduceFunction<Row>() {*/

			/**
			 * The default serial version UID
			 */
			//private static final long serialVersionUID = 1L;

			/**
			 * 
			 */
			//@Override
		/*	public Row call(Row row1, Row row2) throws Exception {
				
				double distance1 = row1.getDouble(2);
				double distance2 = row2.getDouble(2);
				
				boolean visited1 = row1.getBoolean(3);
				boolean visited2 = row2.getBoolean(3);
				
				if(!visited1 && !visited2) {
					if(distance1 < distance2) {
						return row1;
					}
					else {		
						return row2;
					}
				}
				else if(visited1) {
					return row2;
				}
				else {
					return row1;
				}
			}
		});*/
		
		/*Dataset<Row> minInDistancesVisitedDataset = distancesVisitedDataset.select("index", "distance")
																		   .where(distancesVisitedDataset.col("index").$eq$eq$eq(minDistanceVisitedDataset.getInt(0))).cache();
		
		if(!minInDistancesVisitedDataset.isEmpty()) {
			
			Row minInDistancesVisitedDatasetRow = minInDistancesVisitedDataset.first();
			
			nextLastVertex = minInDistancesVisitedDatasetRow.getInt(0);
			
			Row nextRowToBeVisitedAndChanged = RowFactory.create(nextLastVertex, lastVertex, minInDistancesVisitedDatasetRow.getDouble(1), true);
			
			List<Row> nextRowToBeVisitedAndChangedList = new ArrayList<>();
			nextRowToBeVisitedAndChangedList.add(nextRowToBeVisitedAndChanged);
			
			lastVertex = nextLastVertex;
			
			Dataset<Row> nextRowToBeVisitedAndChangedDataset = sparkSession.createDataFrame(nextRowToBeVisitedAndChangedList, minDistancesVisitedDatasetSchema);
			
			return nextRowToBeVisitedAndChangedDataset;
		}
		
		return null;*/
	//}
	
	@SuppressWarnings("deprecation")
	public static JavaPairRDD<Integer, Tuple2<Integer, Double>> computeMinimumSpanningTreeJavaPairRDD(SparkSession sparkSession, SQLContext sqlContext,
												  JavaRDD<Integer> allAiportIndexesRDD,
												  CoordinateMatrix coordinateAdjacencyMatrix,
												  long numAllAirports) {  
	    
		JavaRDD<MatrixEntry> coordinateAdjacencyMatrixEntriesJavaRDD = coordinateAdjacencyMatrix.transpose().entries().toJavaRDD();
		
		JavaRDD<Row> coordinateAdjacencyMatrixRowsJavaRDD = coordinateAdjacencyMatrixEntriesJavaRDD
															.map(matrixEntry -> RowFactory.create((int) matrixEntry.i(), (int) matrixEntry.j(), matrixEntry.value()));
		
		Dataset<Row> coordinateAdjacencyMatrixRowsDataset = sparkSession.createDataFrame(coordinateAdjacencyMatrixRowsJavaRDD, coordinateAdjacencyMatrixEntriesDatasetSchema)
																        .sort(functions.asc("row_index")).cache();
		
		Random random = new Random();
		
		boolean validInitialVertex = false;
		int initialVertexIndex = -1;
		
		while(!validInitialVertex) {
			initialVertexIndex = random.nextInt((int) (numAllAirports + 1L));
			
			Dataset<Row> initialVertexDataset = coordinateAdjacencyMatrixRowsDataset.where(coordinateAdjacencyMatrixRowsDataset.col("row_index").$eq$eq$eq(initialVertexIndex));
		
			if(!initialVertexDataset.isEmpty()) {
				validInitialVertex = true;
			}
		}
		
		JavaRDD<Row> distancesVisitedJavaRDD = allAiportIndexesRDD.map(index -> RowFactory.create(index, -1, Double.MAX_VALUE, false));
		
		Dataset<Row> distancesVisitedDataset = sparkSession.createDataFrame(distancesVisitedJavaRDD, distancesVisitedDatasetSchema).sort(functions.asc("index")).cache();
		
		distancesVisitedDataset = distancesVisitedDataset.where(distancesVisitedDataset.col("index").$bang$eq$eq(initialVertexIndex));
		
		Row initialVertexRow = RowFactory.create(initialVertexIndex, initialVertexIndex, 0.0, true);
		List<Row> initialVertexRowList = new ArrayList<>();
		initialVertexRowList.add(initialVertexRow);
		
		Dataset<Row> initialVertexRowInDistancesVisitedDataset = sparkSession
													   .createDataFrame(initialVertexRowList, distancesVisitedDatasetSchema)
													   .cache();
		
		distancesVisitedDataset = distancesVisitedDataset.union(initialVertexRowInDistancesVisitedDataset);
		
		distancesVisitedDataset = distancesVisitedDataset.sort(functions.asc("index")).cache();
		
		Dataset<Row> directPathsFromInitialVertexDataset = coordinateAdjacencyMatrixRowsDataset.where(coordinateAdjacencyMatrixRowsDataset.col("row_index").$eq$eq$eq(initialVertexIndex));
		
		distancesVisitedDataset = distancesVisitedDataset.join(directPathsFromInitialVertexDataset, distancesVisitedDataset.col("index")
				 .equalTo(directPathsFromInitialVertexDataset.col("row_index")), "leftanti").sort(functions.asc("index")).cache();
		
		List<Row> directPathsFromInitialVertexRowList = new ArrayList<>();
		
		for(Row directPathsFromInitialVertexRow: directPathsFromInitialVertexDataset.collectAsList()) {	
			Row directPathsFromInitialVertexRowToBeAddedToDataset = RowFactory.create(directPathsFromInitialVertexRow.getInt(1), initialVertexIndex, 
																					  directPathsFromInitialVertexRow.getDouble(2), false);

			directPathsFromInitialVertexRowList.add(directPathsFromInitialVertexRowToBeAddedToDataset);
		}
		
		distancesVisitedDataset = distancesVisitedDataset.union(initialVertexRowInDistancesVisitedDataset);
		
		distancesVisitedDataset = distancesVisitedDataset.sort(functions.asc("index")).cache();
		
		int lastVertexIndex = initialVertexIndex;
		int nextLastVertexIndex = -1;
		
		int numVisitedAirports = (int) distancesVisitedDataset.select("visited").where(distancesVisitedDataset.col("visited").$eq$eq$eq(true)).count();
		
		while(numVisitedAirports < numAllAirports) {
			
			System.out.println("Already visited " + numVisitedAirports + " airports!");
			
			Row minDistanceVisitedDataset = distancesVisitedDataset.reduce(new ReduceFunction<Row>() {

				/**
				 * The default serial version UID
				 */
				private static final long serialVersionUID = 1L;

				/**
				 * 
				 */
				@Override
				public Row call(Row row1, Row row2) throws Exception {
					
					double distance1 = row1.getDouble(2);
					double distance2 = row2.getDouble(2);
					
					boolean visited1 = row1.getBoolean(3);
					boolean visited2 = row2.getBoolean(3);
					
					if(!visited1 && !visited2) {
						if(distance1 < distance2) {
							return row1;
						}
						else {		
							return row2;
						}
					}
					else if(visited1) {
						return row2;
					}
					else {
						return row1;
					}
				}
			});

			Dataset<Row> minInDistancesVisitedDataset = distancesVisitedDataset.select("index", "distance")
																			   .where(distancesVisitedDataset.col("index").$eq$eq$eq(minDistanceVisitedDataset.getInt(0)))
																			   .cache();
			
			if(!minInDistancesVisitedDataset.isEmpty()) {				
				Row minInDistancesVisitedDatasetRow = minInDistancesVisitedDataset.first();
				
				System.out.println("The vertex index with minimum distance is: " + minInDistancesVisitedDatasetRow.getInt(0));
				
				nextLastVertexIndex = minInDistancesVisitedDatasetRow.getInt(0);
				
				Row nextRowToBeVisitedAndChanged = RowFactory.create(nextLastVertexIndex, lastVertexIndex, minInDistancesVisitedDatasetRow.getDouble(1), true);
				List<Row> nextRowToBeVisitedAndChangedList = new ArrayList<>();
				nextRowToBeVisitedAndChangedList.add(nextRowToBeVisitedAndChanged);
				
				Dataset<Row> minRowInDistancesVisitedDataset = sparkSession
															   .createDataFrame(nextRowToBeVisitedAndChangedList, distancesVisitedDatasetSchema)
															   .cache();
				
				distancesVisitedDataset = distancesVisitedDataset.join(minRowInDistancesVisitedDataset, distancesVisitedDataset.col("index")
						 										 .equalTo(minRowInDistancesVisitedDataset.col("index")), "leftanti");
				
				distancesVisitedDataset = distancesVisitedDataset.union(minRowInDistancesVisitedDataset);
				
				distancesVisitedDataset = distancesVisitedDataset.sort(functions.asc("index")).cache();
				
				Dataset<Row> directPathsFromLastVertexIndexDataset = coordinateAdjacencyMatrixRowsDataset.where(coordinateAdjacencyMatrixRowsDataset.col("row_index").$eq$eq$eq(lastVertexIndex));
				
				List<Row> directPathsFromLastVertexIndexList = new ArrayList<>();
				
				for(Row directPathFromLastVertexIndexDatasetRow : directPathsFromLastVertexIndexDataset.collectAsList()) {
					Row directPathsFromLastVertexRowToBeAddedToDataset = RowFactory.create(directPathFromLastVertexIndexDatasetRow.getInt(1), directPathFromLastVertexIndexDatasetRow.getInt(0), 
																						   directPathFromLastVertexIndexDatasetRow.getDouble(2), false);

					directPathsFromLastVertexIndexList.add(directPathsFromLastVertexRowToBeAddedToDataset);
				}
				
				distancesVisitedDataset = distancesVisitedDataset.join(minRowInDistancesVisitedDataset, distancesVisitedDataset.col("index")
						 .equalTo(minRowInDistancesVisitedDataset.col("index")), "left");

				distancesVisitedDataset = distancesVisitedDataset.map(new MapFunction<Row, Row>() {

					/**
					 * The default serial version UID
					 */
					private static final long serialVersionUID = 1L;

					/**
					 * 
					 */
					@Override
					public Row call(Row distancesVisitedDatasetRow) throws Exception {
						if(distancesVisitedDatasetRow.get(4) == null) {
							return RowFactory.create(distancesVisitedDatasetRow.getInt(0), distancesVisitedDatasetRow.getInt(1),
													 distancesVisitedDatasetRow.getDouble(2), distancesVisitedDatasetRow.getBoolean(3));
						}
						else if(distancesVisitedDatasetRow.getInt(0) == distancesVisitedDatasetRow.getInt(4)) {
							
							if(distancesVisitedDatasetRow.getDouble(6) < distancesVisitedDatasetRow.getDouble(2)) {
								return RowFactory.create(distancesVisitedDatasetRow.getInt(4), distancesVisitedDatasetRow.getInt(5),
														 distancesVisitedDatasetRow.getDouble(6), distancesVisitedDatasetRow.getBoolean(7));
							}
							else {
								return RowFactory.create(distancesVisitedDatasetRow.getInt(0), distancesVisitedDatasetRow.getInt(1),
										 				 distancesVisitedDatasetRow.getDouble(2), distancesVisitedDatasetRow.getBoolean(3));
							}
						}
						else {
							return RowFactory.create(distancesVisitedDatasetRow.getInt(0), distancesVisitedDatasetRow.getInt(1),
									 distancesVisitedDatasetRow.getDouble(2), distancesVisitedDatasetRow.getBoolean(3));
						}
					}
				},
				RowEncoder.apply(distancesVisitedDatasetSchema)).cache();
								
				lastVertexIndex = nextLastVertexIndex;
				
				numVisitedAirports = (int) distancesVisitedDataset.select("visited").where(distancesVisitedDataset.col("visited").$eq$eq$eq(true)).cache().count();	
			}	
		}
		
		
		
		
		
		
		
		
		
		
		//JavaRDD<MatrixEntry> coordinateAdjacencyMatrixEntriesJavaRDD = coordinateAdjacencyMatrix.transpose().entries().toJavaRDD().cache();
		
		//JavaRDD<MatrixEntry> initialVertexMatrixEntryJavaRDD = coordinateAdjacencyMatrixEntriesJavaRDD.filter(new Function<MatrixEntry, Boolean>() {
			
			/**
			 * The default serial version UID
			 */
			//private static final long serialVersionUID = 1L;

			/**
			 * The random seed to generate random values
			 */
			//private Random random = new Random();
			
			/**
			 * 
			 */
			//private final long initialVertexIndex = random.nextInt((int) (numAllAirports + 1L));
			
			/**
			 * 		
			 */
			//@Override
			//public Boolean call(MatrixEntry matrixEntry) throws Exception {
				//return matrixEntry.i() == initialVertexIndex;
			//}
		//}).cache();
		
	//	MatrixEntry initialVertexMatrixEntry = initialVertexMatrixEntryJavaRDD.first();
		//int initialVertex = (int) initialVertexMatrixEntry.i();
		
		//System.out.println(initialVertex);
		
		// The array to keep all the distances from the root of
		// the Minimum Spanning Tree (M.S.T.), initialised as INFINITE
		//JavaPairRDD<Integer, Tuple2<Integer, Double>> distancesJavaPairRDD = 
			//allAiportIndexesRDD.mapToPair(
				//(index) -> new Tuple2<Integer, Tuple2<Integer, Double>>(index, new Tuple2<Integer, Double>(-1, Double.MAX_VALUE))
			//).cache();
	 
		// The array to represent and keep the set of
		// vertices already yet included in Minimum Spanning Tree (M.S.T.),
		// initialised as FALSE
		//JavaPairRDD<Integer, Boolean> visitedJavaPairRDD = allAiportIndexesRDD.mapToPair( (index) -> new Tuple2<Integer, Boolean>(index, false)).cache();
		
		// The position of the root it's initialised with the distance of 0 (ZERO)
		// to build the Minimum Spanning Tree (M.S.T.)
		//distancesJavaPairRDD = distancesJavaPairRDD.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Integer, Double>>, Integer, Tuple2<Integer, Double>>() {

			/**
			 * The default serial version UID
			 */
			//private static final long serialVersionUID = 1L;

			/**
			 * 
			 */
			/*@Override
			public Tuple2<Integer, Tuple2<Integer, Double>> call(Tuple2<Integer, Tuple2<Integer, Double>> distancePair) throws Exception {
				if(distancePair._1() == initialVertex) {
					return new Tuple2<Integer, Tuple2<Integer, Double>>(distancePair._1(), new Tuple2<Integer, Double>(distancePair._1(), 0.0));
				}
				else {
					return new Tuple2<Integer, Tuple2<Integer, Double>>(distancePair._1(), new Tuple2<Integer, Double>(distancePair._2()._1(), distancePair._2()._2()));
				}
			}
		}).cache();*/
		
		// The position of the root it's initialised as TRUE in
		// the set of vertices already yet included in Minimum Spanning Tree (M.S.T.)
		//visitedJavaPairRDD = visitedJavaPairRDD.mapToPair(new PairFunction<Tuple2<Integer, Boolean>, Integer, Boolean>() {

			/**
			 * The default serial version UID
			 */
			//private static final long serialVersionUID = 1L;

			/**
			 * 
			 */
			/*@Override
			public Tuple2<Integer, Boolean> call(Tuple2<Integer, Boolean> distancePair) throws Exception {
				if(distancePair._1() == initialVertex) {
					return new Tuple2<Integer, Boolean>(distancePair._1(), true);
				}
				else {
					return new Tuple2<Integer, Boolean>(distancePair._1(), distancePair._2());
				}
			}
		}).cache();*/
		
//		JavaRDD<MatrixEntry> directPathsFromInitialVertexJavaRDD = coordinateAdjacencyMatrixEntriesJavaRDD.filter(new Function<MatrixEntry, Boolean>() {

			/**
			 * The default serial version UID
			 */
//			private static final long serialVersionUID = 1L;
			
			/**
			 * The call method to perform the filter of all the Matrix Entries,
			 * beginning with the initial vertex as row's index 
			 */
	/*		@Override
			public Boolean call(MatrixEntry matrixEntry) throws Exception {
				return (matrixEntry.i() == initialVertex) ? true : false;
			}
		}).cache();*/
		
		/**
		 * Just for debug:
		 * - The content of the vector of minimum distances from the origin
		 */
/*		for(Tuple2<Integer, Tuple2<Integer, Double>> distanceTuple : distancesJavaPairRDD.collect()) {
			System.out.println("(" + distanceTuple._2() + " -> " + distanceTuple._1() + ") = " + distanceTuple._2()._2());
		}*/
		
		/**
		 * Just for debug:
		 * - The content of the vector of visited vertices from the origin
		 */
		/*for(Tuple2<Integer, Boolean> visitedTuple : visitedJavaPairRDD.collect()) {
			System.out.println(visitedTuple._1() + " -> " + visitedTuple._2());
		}
		
		JavaRDD<Row> directPathsFromInitialVertexRowJavaRDD = directPathsFromInitialVertexJavaRDD
				.map(directPathsFromInitialVertexMatrixEntry -> RowFactory.create(new Object[] {(int) directPathsFromInitialVertexMatrixEntry.i(),
																								(int) directPathsFromInitialVertexMatrixEntry.j(),
																								directPathsFromInitialVertexMatrixEntry.value()})).cache();
		
		StructType directPathsFromInitialVertexDatasetSchema = DataTypes.createStructType(new StructField[] {
				DataTypes.createStructField("origin_index",  DataTypes.IntegerType, true),
				DataTypes.createStructField("destination_index",  DataTypes.IntegerType, true),
	            DataTypes.createStructField("distance", DataTypes.DoubleType, true)
	    });
		
		Dataset<Row> directPathsFromInitialVertexDataset = sparkSession.createDataFrame(directPathsFromInitialVertexRowJavaRDD, directPathsFromInitialVertexDatasetSchema).cache();
		
		JavaRDD<Row> distancesJavaRDD = distancesJavaPairRDD.map(distanceJavaPair -> RowFactory.create(new Object[] {distanceJavaPair._1(), distanceJavaPair._2()._1(), distanceJavaPair._2()._2()})).cache();
		
		StructType distancesDatasetSchema = DataTypes.createStructType(new StructField[] {
	            DataTypes.createStructField("index",  DataTypes.IntegerType, true),
	            DataTypes.createStructField("from_index",  DataTypes.IntegerType, true),
	            DataTypes.createStructField("distance", DataTypes.DoubleType, true)
	    });
		
		Dataset<Row> distancesDataset = sparkSession.createDataFrame(distancesJavaRDD, distancesDatasetSchema).cache();
		
		JavaRDD<Row> visitedJavaRDD = visitedJavaPairRDD.map(visitedJavaPair -> RowFactory.create(new Object[] {visitedJavaPair._1(), visitedJavaPair._2()})).cache();
		
		StructType visitedDatasetSchema = DataTypes.createStructType(new StructField[] {
	            DataTypes.createStructField("visited_index",  DataTypes.IntegerType, true),
	            DataTypes.createStructField("visited", DataTypes.BooleanType, true)
	    });
		
		Dataset<Row> visitedDataset = sparkSession.createDataFrame(visitedJavaRDD, visitedDatasetSchema).cache();
		
		for(Row row : distancesDataset.collectAsList())
			System.out.println(row);
		
		System.out.println();
		System.out.println();
		
		for(Row row : visitedDataset.collectAsList())
			System.out.println(row);
		
		System.out.println();
		System.out.println();
		
		for(Row row : directPathsFromInitialVertexDataset.collectAsList())
			System.out.println(row);
		
		distancesDataset = distancesDataset.join(directPathsFromInitialVertexDataset.select("destination_index", "distance"),
											distancesDataset.col("index")
											.equalTo(directPathsFromInitialVertexDataset.col("destination_index")), "left")
											.sort(functions.asc("index"))
											.cache();
		
		distancesDataset = distancesDataset.map(new MapFunction<Row, Row>() {*/

			/**
			 * The default serial version UID
			 */
//			private static final long serialVersionUID = 1L;

			/**
			 * 
			 */
	/*		@Override
			public Row call(Row distancesDatasetRow) throws Exception {
				if(distancesDatasetRow.getInt(1) != -1) {
					return RowFactory.create(distancesDatasetRow.getInt(0), distancesDatasetRow.getInt(1), distancesDatasetRow.getDouble(2));
				}
				else if(distancesDatasetRow.get(3) == null) {
					return RowFactory.create(distancesDatasetRow.getInt(0), -1, distancesDatasetRow.getDouble(2));
				}
				else {
					return RowFactory.create(distancesDatasetRow.getInt(0), initialVertex, distancesDatasetRow.getDouble(4));
				}
			}
		},
		RowEncoder.apply(distancesDatasetSchema)).cache();
		
		System.out.println();
		System.out.println();
		
		for(Row row : distancesDataset.collectAsList())
			System.out.println(row);
		
		Dataset<Row> distancesVisitedDataset = distancesDataset.join(visitedDataset.select("visited_index", "visited"), distancesDataset.col("index")
															   .equalTo(visitedDataset.col("visited_index")), "left").select("index", "from_index", "distance", "visited")
															   .sort(functions.asc("index"))
															   .cache();
		
		int lastVertex = initialVertex;
		int nextLastVertex = -1;
		
		int numVisitedAirports = (int) distancesVisitedDataset.select("visited").where(distancesVisitedDataset.col("visited").$eq$eq$eq(true)).count();
		
		while(numVisitedAirports < numAllAirports) {
			
			System.out.println("Already visited " + numVisitedAirports + " airports!");
			
			Row minDistanceVisitedDataset = distancesVisitedDataset.reduce(new ReduceFunction<Row>() {*/

				/**
				 * The default serial version UID
				 */
//				private static final long serialVersionUID = 1L;

				/**
				 * 
				 */
	/*			@Override
				public Row call(Row row1, Row row2) throws Exception {
					
					double distance1 = row1.getDouble(2);
					double distance2 = row2.getDouble(2);
					
					boolean visited1 = row1.getBoolean(3);
					boolean visited2 = row2.getBoolean(3);
					
					if(!visited1 && !visited2) {
						if(distance1 < distance2) {
							return row1;
						}
						else {		
							return row2;
						}
					}
					else if(visited1) {
						return row2;
					}
					else {
						return row1;
					}
				}
			});
			
			Dataset<Row> minInDistancesVisitedDataset = distancesVisitedDataset.select("index", "distance")
																			   .where(distancesVisitedDataset.col("index").$eq$eq$eq(minDistanceVisitedDataset.getInt(0)))
																			   .cache();
			
			if(!minInDistancesVisitedDataset.isEmpty()) {				
				Row minInDistancesVisitedDatasetRow = minInDistancesVisitedDataset.first();
				
				System.out.println("The vertex index with minimum distance is: " + minInDistancesVisitedDatasetRow.getInt(0));
				
				nextLastVertex = minInDistancesVisitedDatasetRow.getInt(0);
				
				Row nextRowToBeVisitedAndChanged = RowFactory.create(nextLastVertex, lastVertex, minInDistancesVisitedDatasetRow.getDouble(1), true);
				List<Row> nextRowToBeVisitedAndChangedList = new ArrayList<>();
				nextRowToBeVisitedAndChangedList.add(nextRowToBeVisitedAndChanged);
				
				lastVertex = nextLastVertex;
				
				Dataset<Row> minRowInDistancesVisitedDataset = sparkSession
															   .createDataFrame(nextRowToBeVisitedAndChangedList, distancesVisitedDatasetSchema)
															   .cache();
				
				distancesVisitedDataset = distancesVisitedDataset.join(minRowInDistancesVisitedDataset, distancesVisitedDataset.col("index")
						 										 .equalTo(minRowInDistancesVisitedDataset.col("index")), "leftanti");
				
				distancesVisitedDataset = distancesVisitedDataset.union(minRowInDistancesVisitedDataset);
			}*/
			
			/*distancesVisitedDataset = distancesVisitedDataset.map(new MapFunction<Row, Row>() {

				/**
				 * The default serial version UID
				 */
				//private static final long serialVersionUID = 1L;

				/**
				 * 
				 */
				//@Override
				//public Row call(Row distancesVisitedDatasetRow) throws Exception {
					//if(distancesVisitedDatasetRow.get(4) == null) {
						//return RowFactory.create(distancesVisitedDatasetRow.getInt(0), distancesVisitedDatasetRow.getInt(1),
												// distancesVisitedDatasetRow.getDouble(2), distancesVisitedDatasetRow.getBoolean(3));
					//}
					//else {
				//		return RowFactory.create(distancesVisitedDatasetRow.getInt(4), distancesVisitedDatasetRow.getInt(5),
								 				// distancesVisitedDatasetRow.getDouble(6), distancesVisitedDatasetRow.getBoolean(7));
			//		}
		//		}
	//		},
//			RowEncoder.apply(distancesVisitedDatasetSchema)).cache();
				
			//numVisitedAirports = (int) distancesVisitedDataset.select("visited").where(distancesVisitedDataset.col("visited").$eq$eq$eq(true)).cache().count();
		//}
		
		JavaPairRDD<Integer, Tuple2<Integer, Double>> minimumSpanningTree = distancesVisitedDataset.javaRDD().mapToPair(
				new PairFunction<Row, Integer, Tuple2<Integer, Double>> () {
					
				/**
				 * The default serial version UID
				 */
				private static final long serialVersionUID = 1L;

				/**
				 * 
				 */
				@Override
				public Tuple2<Integer, Tuple2<Integer, Double>> call(Row row) throws Exception {
					return new Tuple2<Integer, Tuple2<Integer, Double>>(row.getInt(1), new Tuple2<Integer, Double>(row.getInt(0), row.getDouble(2)));
				}
			}
		);
		
		return minimumSpanningTree;
	}
	
	public static JavaRDD<MatrixEntry> computeMinimumSpanningTreeComplement(JavaPairRDD<Integer, Tuple2<Integer, Double>> minimumSpanningTree,
																		    CoordinateMatrix coordinateAdjacencyMatrix) {

		JavaRDD<MatrixEntry> minimumSpanningTreeComplementCoordinateAdjacencyMatrix = coordinateAdjacencyMatrix.entries().toJavaRDD().filter(new Function<MatrixEntry, Boolean>() {

			/**
			 * The default serial version UID
			 */
			private static final long serialVersionUID = 1L;

			/**
			 * 
			 */
			@Override
			public Boolean call(MatrixEntry matrixEntry) throws Exception {
				
				JavaPairRDD<Integer, Tuple2<Integer, Double>> currentOriginVertexLinksInMinimumSpanningTree = 
						minimumSpanningTree.filter(new Function<Tuple2<Integer, Tuple2<Integer, Double>>, Boolean>() {

						/**
						 * The default serial version UID
						 */
						private static final long serialVersionUID = 1L;
						
						/**
						 * 
						 */
						@Override
						public Boolean call(Tuple2<Integer, Tuple2<Integer, Double>> minimumSpanningTreeJavaPairRDD) throws Exception {
							if(minimumSpanningTreeJavaPairRDD._1() == matrixEntry.i() && minimumSpanningTreeJavaPairRDD._2()._1() == matrixEntry.j()) {
								return true;
							}
							else {
								return false;
							}
						}					
				});

				if(!currentOriginVertexLinksInMinimumSpanningTree.isEmpty()) {
					return true;
				}
				else {
					return false;
				}
			}
		});
		
		return minimumSpanningTreeComplementCoordinateAdjacencyMatrix;
	}
	
	public static Tuple2<Integer, Double> getBottleneckAirportFromMinimumSpanningTreeComplementJavaRDD(JavaRDD<MatrixEntry> minimumSpanningTreeComplementJavaRDD) {

		JavaPairRDD<Integer, Double> minimumSpanningTreeComplementJavaPairRDD = 
			minimumSpanningTreeComplementJavaRDD.mapToPair(
					new PairFunction<MatrixEntry, Integer, Double> () {

						/**
						 * The default serial version UID
						 */
						private static final long serialVersionUID = 1L;

						/**
						 * 
						 */
						@Override
						public Tuple2<Integer, Double> call(MatrixEntry matrixEntry) throws Exception {
							return new Tuple2<Integer, Double>((int) matrixEntry.i(), matrixEntry.value());
						}
					}
			).cache();
		
		JavaPairRDD<Integer, Double> sumOfDepartureDelaysByAirportFromMinimumSpanningTreeComplementJavaPairRDD = 
				minimumSpanningTreeComplementJavaPairRDD.reduceByKey((departureDelay1, departureDelay2) -> departureDelay1 + departureDelay2).cache();
			
		Tuple2<Integer, Double> bottleneckAirportFromMinimumSpanningTreeComplementTuple = 
			sumOfDepartureDelaysByAirportFromMinimumSpanningTreeComplementJavaPairRDD.max(new Comparator<Tuple2<Integer, Double>>() {

				@Override
				public int compare(Tuple2<Integer, Double> tuple1, Tuple2<Integer, Double> tuple2) {
					return tuple1._2() > tuple2._2() ? 1 : -1;
				}
		});
		
		return bottleneckAirportFromMinimumSpanningTreeComplementTuple;
	}

	// TODO
	public static JavaPairRDD<Integer, Tuple2<Integer, Double>> getMinimumSpanningTreeComplementWithBottleneckAirportReducedByFactorJavaRDD
				(SparkSession sparkSession, JavaPairRDD<Integer, Tuple2<Integer, Double>> minimumSpanningTreeComplementJavaRDD,
				 Tuple2<Integer, Double> bottleneckAirport, float reduceFactor) { 
		
		JavaRDD<Row> minimumSpanningTreeComplementRowsJavaRDD = minimumSpanningTreeComplementJavaRDD
																.map(tuple -> RowFactory.create(tuple._1(), tuple._2()._1(), tuple._2()._2()));
		
		Dataset<Row> minimumSpanningTreeComplementRowsDataset = sparkSession.createDataFrame(minimumSpanningTreeComplementRowsJavaRDD, minimumSpanningTreeDatasetSchema)
																			.sort(functions.asc("origin")).cache();
		
		Dataset<Row> bottleneckAirportFromMinimumSpanningTreeComplementRowsDataset = minimumSpanningTreeComplementRowsDataset
				                                                                     .where(minimumSpanningTreeComplementRowsDataset.col("origin").$eq$eq$eq(bottleneckAirport._1()));
			
		bottleneckAirportFromMinimumSpanningTreeComplementRowsDataset = bottleneckAirportFromMinimumSpanningTreeComplementRowsDataset
																		.withColumn("distance_reduced_factor", bottleneckAirportFromMinimumSpanningTreeComplementRowsDataset.col("distance").$times(reduceFactor));
		
		bottleneckAirportFromMinimumSpanningTreeComplementRowsDataset = 
				bottleneckAirportFromMinimumSpanningTreeComplementRowsDataset.select(bottleneckAirportFromMinimumSpanningTreeComplementRowsDataset.col("origin"),
																					 bottleneckAirportFromMinimumSpanningTreeComplementRowsDataset.col("destination"),
																					 bottleneckAirportFromMinimumSpanningTreeComplementRowsDataset.col("distance_reduced_factor").as("distance"));
		
		minimumSpanningTreeComplementRowsDataset = minimumSpanningTreeComplementRowsDataset
												  .join(bottleneckAirportFromMinimumSpanningTreeComplementRowsDataset, minimumSpanningTreeComplementRowsDataset.col("origin")
												  .equalTo(bottleneckAirportFromMinimumSpanningTreeComplementRowsDataset.col("index")), "leftanti").sort(functions.asc("index")).cache();
		
		Dataset<Row> modifiedMinimumSpanningTreeComplementWithBottleneckAirportReducedByFactorDataset = minimumSpanningTreeComplementRowsDataset.union(bottleneckAirportFromMinimumSpanningTreeComplementRowsDataset);
		
		JavaPairRDD<Integer, Tuple2<Integer, Double>> modifiedMinimumSpanningTreeComplementWithBottleneckAirportReducedByFactorJavaPairRDD = 
													  modifiedMinimumSpanningTreeComplementWithBottleneckAirportReducedByFactorDataset.javaRDD()
													  .mapToPair(row -> new Tuple2<Integer, Tuple2<Integer, Double>>(row.getInt(0), new Tuple2<Integer, Double>(row.getInt(1), row.getDouble(2))))
													  .cache();
		
		return modifiedMinimumSpanningTreeComplementWithBottleneckAirportReducedByFactorJavaPairRDD;
	}	
	
	/**
	 * Main method to process the flights' file and analyse it.
	 * 
	 * @param args the file of flights to process and the reduce factor in the interval ]0,1[
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
					
					while(reduceFactor == 0.0f) {
						reduceFactor = random.nextFloat();
					}
				}
			}
		}
		
		// Start Spark Session (SparkContext API may also be used) 
		// master("local") indicates local execution
		SparkSession sparkSession = SparkSession
									.builder()
									.appName("FlightAnalyser")
									.config("spark.executor.memory", "70g")
							        .config("spark.driver.memory", "50g")
							        .config("spark.memory.offHeap.enabled",true)
							        .config("spark.memory.offHeap.size","16g")
									.master("local[*]")
									.getOrCreate();
		
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
		
		JavaRDD<Integer> allAirportsIndexesJavaRDD = allAirportsByIndexMap.select("index").javaRDD().map(x -> x.getInt(0));
		
		JavaPairRDD<Integer, Tuple2<Integer, Double>> minimumSpanningTreeJavaPairRDD = 
				computeMinimumSpanningTreeJavaPairRDD(sparkSession, sqlContext, allAirportsIndexesJavaRDD, coordinateAdjacencyMatrix, numAllAirports);
		
		for(Tuple2<Integer, Tuple2<Integer, Double>> minimumSpanningTreePair : minimumSpanningTreeJavaPairRDD.collect())
			System.out.println("(" + minimumSpanningTreePair._1() + "," + minimumSpanningTreePair._2()._1() + ") = " + minimumSpanningTreePair._2()._2());
		
		
		// Terminate the Spark Session
		sparkSession.stop();
	}
}