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
import java.util.List;
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

	private static final String DefaulftFile = "data/flights_6.csv";
	
	
	// The Datasets' Schemas defined as Struct Types for the Coordinate Adjacency Matrix
	private static StructType coordinateAdjacencyMatrixEntriesDatasetSchema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("row_index",  DataTypes.IntegerType, true),
            DataTypes.createStructField("column_index",  DataTypes.IntegerType, true),
            DataTypes.createStructField("distance", DataTypes.DoubleType, true)
    });
	
	// The Datasets' Schemas defined as Struct Types for the Distances and Visited flag for the Vertices
	private static StructType distancesVisitedDatasetSchema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("index",  DataTypes.IntegerType, true),
            DataTypes.createStructField("from_index",  DataTypes.IntegerType, true),
            DataTypes.createStructField("distance", DataTypes.DoubleType, true),
            DataTypes.createStructField("visited", DataTypes.BooleanType, true)
    });
	
	// The Datasets' Schemas defined as Struct Types for the Minimum Spanning Tree (M.S.T.)
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
	// (sum of the Average Departure Delays). Output the M.S.T. and its total edge weight. (STEP DONE)
	
	// 3. Identify the 'Bottleneck' Airport, i.e., the Airport with higher aggregated Average Departure Delay time
	// (sum of the Average Departure Delays of all routes going out of the Airport)
	// from the ones contained in the complement Graph of the M.S.T. previously computed. (STEP DONE)
	
	// 4. Modify the Graph to reduce by a given factor the Average Departure Delay time of
	// all routes going out of the selected airport.
	// This factor will be a parameter of your algorithm (received in the command line) and must be a value in ]0, 1[. (STEP DONE)
	
	// 5. Recompute the M.S.T. and display the changes perceived in the resulting subgraph and
	// on the sum of the total edge weight (STEP DONE)
	
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
			
			Dataset<Row> allDelaysOfTheFlightsBetweenTwoAirportsDataset = flightsDataset.select("origin_id", "destination_id", "departure_delay");
			
			Dataset<Row> allDelaysOfTheFlightsBetweenTwoAirportsInvertedDataset = allDelaysOfTheFlightsBetweenTwoAirportsDataset
																				  .select("destination_id", "origin_id", "departure_delay");
			
			
			Dataset<Row> allDelaysOfTheFlightsBetweenTwoAirportsWithDuplicatesDataset = allDelaysOfTheFlightsBetweenTwoAirportsDataset
																						.union(allDelaysOfTheFlightsBetweenTwoAirportsInvertedDataset);
			
			allDelaysOfTheFlightsBetweenTwoAirportsWithDuplicatesDataset.registerTempTable("all_delays_of_the_flights_between_any_two_airports");
			
			String sqlQueryRemovePairDuplicates = "SELECT DISTINCT * " +
					  "FROM all_delays_of_the_flights_between_any_two_airports t1 " +
				      "WHERE t1.origin_id < t1.destination_id " +
                      	"OR NOT EXISTS (" +
                      		"SELECT * " +
                      		"FROM all_delays_of_the_flights_between_any_two_airports t2 " +
                      		"WHERE t2.origin_id = t1.destination_id " + 
                      		"AND t2.destination_id = t1.origin_id" +
                      	")";
			
			Dataset<Row> allDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
					  														sqlContext.sql(sqlQueryRemovePairDuplicates);	
			
			Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
					allDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
					.groupBy("origin_id", "destination_id")
					.avg("departure_delay")
					.orderBy("origin_id");
			
			Dataset<Row> allPossibleOriginAirportsByIndexMap = allAirportsByIndexMap
															   .withColumnRenamed("id", "origin_id").withColumnRenamed("index", "origin_index");
			
			Dataset<Row> allPossibleDestinationAirportsByIndexMap = allAirportsByIndexMap
																	.withColumnRenamed("id", "destination_id").withColumnRenamed("index", "destination_index");
			
			// Defines the Dataset of All Average Departure Delays of the Flights, joined with the Possible Origin Airports IDs
			allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
					allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
					.join(allPossibleOriginAirportsByIndexMap,
						  allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset.col("origin_id")
						  .equalTo(allPossibleOriginAirportsByIndexMap.col("origin_id")), "left");
			
			// Defines the Dataset of All Average Departure Delays of the Flights, joined with the Possible Destination Airports IDs
			allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
					allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
					.join(allPossibleDestinationAirportsByIndexMap,
						  allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset.col("destination_id")
						  .equalTo(allPossibleDestinationAirportsByIndexMap.col("destination_id")), "left");
			
			// Defines the Dataset of All Average Departure Delays of The Flights
			// Between Any Two Airports Origin and Destination Dataset
			allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
					allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
							.select("origin_index", "t1.origin_id", "destination_index", "t1.destination_id", "avg(departure_delay)");
			
			// Defines the Dataset of All Average Delays of The Flights
			// Between Any Two Airports, Disregarding Origin and Destination Dataset, unified with inverted indexes
			allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
					allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
					.union(allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
							.select("destination_index", "t1.destination_id", "origin_index", "t1.origin_id", "avg(departure_delay)"));
			
			return allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset.sort(functions.asc("origin_index"));
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
	 * Returns all Airports mapped by Index, ordered by ID. 
	 * 
	 * @param allAirportsIDsDataset the Dataset of all Airports' IDs
	 * 
	 * @return all Airports mapped by Index, ordered by ID
	 */
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
	 * of mappings between Airport Origin's ID and the a tuple/pair containing other two tuples/pairs:
	 * - (row's index, column's index) of the Adjacency Coordinate Matrix
	 * - (Airport Destination's ID, Average Departure Delays of the Flights between that two Airports) 
	 * 
	 * @param allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
	 *        the Dataset (built of Rows) containing all Average Departure Delays of Flights for each route,
	 *        ordered by descending of Average field, disregarding Origin and Destination
	 * 
	 * @return a Java Pair RDD containing a collection (Disregarding Airport's Origin and Destination)
	 * 		   of mappings between Airport Origin's ID and the a tuple/pair containing other two tuples/pairs:
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
	 * of mappings between Airport Origin's ID and the a tuple/pair containing:
	 * - (Airport Destination's ID, Average Departure Delays of the Flights between that two Airports) 
	 * 
	 * @param allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
	 * 		  the Dataset (built of Rows) containing all Average Departure Delays of Flights for each route,
	 *        ordered by descending of Average field, disregarding Origin and Destination
	 * 
	 * @return a Java Pair RDD containing a collection (Disregarding Airport's Origin and Destination)
	 * 		   of mappings between Airport Origin's ID and the a tuple/pair containing:
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
	 * of mappings between Airport Origin's ID and an iterable tuple/pair containing:
	 * - (Airport Destination's ID, Average Departure Delays of the Flights between that two Airports) 
	 * 
	 * @param allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
	 * 		  the Dataset (built of Rows) containing all Average Departure Delays of Flights for each route,
	 *        ordered by descending of Average field, disregarding Origin and Destination
	 * 
	 * @return a Java Pair RDD containing a collection (Disregarding Airport's Origin and Destination)
	 * 		   of mappings between Airport Origin's ID and an iterable tuple/pair containing:
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
	
	/**
	 * Returns the computed Minimum Spanning Tree as a JavaPairRDD format,
	 * with a tuple/pair containing the origin vertex and other tuple/pair containing the destination vertex and the distance (Average Departure Delay)
	 * 
	 * @param sparkSession a given SparkSession
	 * @param sqlContext a given SQL Context
	 * @param allAiportIndexesRDD a given RDD with All Airports mapped by Index 
	 * @param coordinateAdjacencyMatrix the Coordinate Adjacency Matrix
	 * @param numAllAirports the number of all Airports
	 * 
	 * @return the computed Minimum Spanning Tree as a JavaPairRDD format,
	 * 		   with a tuple/pair containing the origin vertex and other tuple/pair containing the destination vertex and the distance (Average Departure Delay)
	 */
	@SuppressWarnings("deprecation")
	public static JavaPairRDD<Integer, Tuple2<Integer, Double>> computeMinimumSpanningTreeJavaPairRDD(SparkSession sparkSession, SQLContext sqlContext,
																									  JavaRDD<Integer> allAiportIndexesRDD,
																									  CoordinateMatrix coordinateAdjacencyMatrix,
																									  long numAllAirports) {  
	    
		JavaRDD<MatrixEntry> coordinateAdjacencyMatrixEntriesJavaRDD = coordinateAdjacencyMatrix.entries().toJavaRDD();
		
		JavaRDD<Row> coordinateAdjacencyMatrixRowsJavaRDD = coordinateAdjacencyMatrixEntriesJavaRDD
															.map(matrixEntry -> RowFactory.create((int) matrixEntry.i(), (int) matrixEntry.j(), matrixEntry.value()));
		
		Dataset<Row> coordinateAdjacencyMatrixRowsDataset = sparkSession.createDataFrame(coordinateAdjacencyMatrixRowsJavaRDD, coordinateAdjacencyMatrixEntriesDatasetSchema)
																        .sort(functions.asc("row_index"));
		
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
		
		initialVertexIndex = 6;
		
		System.out.println("The initial vertex to compute the Minimum Spanning Tree is:");
		System.out.println("- " + initialVertexIndex);
		
		JavaRDD<Row> distancesVisitedJavaRDD = allAiportIndexesRDD.map(index -> RowFactory.create(index, -1, Double.MAX_VALUE, false));
		
		Dataset<Row> distancesVisitedDataset = sparkSession.createDataFrame(distancesVisitedJavaRDD, distancesVisitedDatasetSchema).sort(functions.asc("index")).cache();
		
		distancesVisitedDataset = distancesVisitedDataset.where(distancesVisitedDataset.col("index").$bang$eq$eq(initialVertexIndex));
		
		Row initialVertexRow = RowFactory.create(initialVertexIndex, initialVertexIndex, 0.0, true);
		List<Row> initialVertexRowList = new ArrayList<>();
		initialVertexRowList.add(initialVertexRow);
		
		Dataset<Row> initialVertexRowInDistancesVisitedDataset = sparkSession
													   .createDataFrame(initialVertexRowList, distancesVisitedDatasetSchema)
													   .cache();
		
		Dataset<Row> directPathsFromInitialVertexDataset = coordinateAdjacencyMatrixRowsDataset
														   .where(coordinateAdjacencyMatrixRowsDataset.col("row_index").$eq$eq$eq(initialVertexIndex));
		
		System.out.println();
		
		System.out.println("Possible direct paths from initial vertex:");
		for(Row directPathFromInitialVertex : directPathsFromInitialVertexDataset.collectAsList())
			System.out.println("- " + directPathFromInitialVertex);
		
		System.out.println();
		System.out.println();
		
		
		List<Row> directPathsFromInitialVertexRowList = new ArrayList<>();
		
		distancesVisitedDataset = distancesVisitedDataset.join(directPathsFromInitialVertexDataset, distancesVisitedDataset.col("index")
				 .equalTo(directPathsFromInitialVertexDataset.col("column_index")), "leftanti").sort(functions.asc("index")).cache();
		
		for(Row directPathsFromInitialVertexRow: directPathsFromInitialVertexDataset.collectAsList()) {	
			Row directPathsFromInitialVertexRowToBeAddedToDataset = RowFactory.create(directPathsFromInitialVertexRow.getInt(1), initialVertexIndex, 
																					  directPathsFromInitialVertexRow.getDouble(2), false);
			
			directPathsFromInitialVertexRowList.add(directPathsFromInitialVertexRowToBeAddedToDataset);
		}
		
		Dataset<Row> directPathsFromInitialVertexRowDataset = sparkSession
				   .createDataFrame(directPathsFromInitialVertexRowList, distancesVisitedDatasetSchema)
				   .cache();
		
		distancesVisitedDataset = distancesVisitedDataset.union(initialVertexRowInDistancesVisitedDataset);
		
		distancesVisitedDataset = distancesVisitedDataset.union(directPathsFromInitialVertexRowDataset);
		
		distancesVisitedDataset = distancesVisitedDataset.sort(functions.asc("index")).cache();
		
		System.out.println("Current state of the Vector of Distances and Visited Vertices:");
		for(Row distancesVisitedRow : distancesVisitedDataset.collectAsList()) {
			System.out.println("- " + distancesVisitedRow);
		}
		
		System.out.println();
		System.out.println();
		
		int lastVertexIndex = initialVertexIndex;
		int nextLastVertexIndex = -1;
		
		int numVisitedAirports = (int) distancesVisitedDataset.select("visited").where(distancesVisitedDataset.col("visited").$eq$eq$eq(true)).count();
		
		while(numVisitedAirports < numAllAirports) {
			
			System.out.println("Already visited " + numVisitedAirports + " airports!");
			System.out.println();
			
			Row minDistanceVisitedDataset = distancesVisitedDataset.reduce(new ReduceFunction<Row>() {

				/**
				 * The default serial version UID
				 */
				private static final long serialVersionUID = 1L;

				/**
				 * TODO - ver visitados e null 
				 */
				
				/**
				 * The call method to perform the reduce for each row
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
				
				if(minInDistancesVisitedDatasetRow.getDouble(1) == Double.MAX_VALUE) {
					break;
				}
				
				System.out.println("The vertex index with minimum distance is: " + minInDistancesVisitedDatasetRow.getInt(0));
				
				nextLastVertexIndex = minInDistancesVisitedDatasetRow.getInt(0);
				
				Row nextRowToBeVisitedAndChanged;
				
				nextRowToBeVisitedAndChanged = RowFactory.create(minDistanceVisitedDataset.getInt(0), minDistanceVisitedDataset.getInt(1), minDistanceVisitedDataset.getDouble(2), true);
				
				List<Row> nextRowToBeVisitedAndChangedList = new ArrayList<>();
				nextRowToBeVisitedAndChangedList.add(nextRowToBeVisitedAndChanged);
				
				Dataset<Row> minRowInDistancesVisitedDataset = sparkSession
															   .createDataFrame(nextRowToBeVisitedAndChangedList, distancesVisitedDatasetSchema)
															   .cache();
				
				distancesVisitedDataset = distancesVisitedDataset.join(minRowInDistancesVisitedDataset, distancesVisitedDataset.col("index")
						 										 .equalTo(minRowInDistancesVisitedDataset.col("index")), "leftanti");
				
				distancesVisitedDataset = distancesVisitedDataset.union(minRowInDistancesVisitedDataset).sort(functions.asc("index")).cache();
				
				Dataset<Row> directPathsFromLastVertexIndexDataset = coordinateAdjacencyMatrixRowsDataset
						.where(coordinateAdjacencyMatrixRowsDataset.col("row_index").$eq$eq$eq(nextLastVertexIndex)
						.and(coordinateAdjacencyMatrixRowsDataset.col("column_index").$bang$eq$eq(lastVertexIndex)));
						
				System.out.println();
				System.out.println();
				
				System.out.println("Possible direct paths from the current minimum vertex index:");
				for(Row directPathFromLastVertexIndex : directPathsFromLastVertexIndexDataset.collectAsList())
					System.out.println("- " + directPathFromLastVertexIndex);
				
				System.out.println();
				System.out.println();
				
				List<Row> directPathsFromLastVertexIndexList = new ArrayList<>();
				
				for(Row directPathFromLastVertexIndexDatasetRow : directPathsFromLastVertexIndexDataset.collectAsList()) {
					
					double currentAggregatedDistance = distancesVisitedDataset.select("index", "from_index", "distance", "visited")
						   		 						   .where(distancesVisitedDataset.col("index").$eq$eq$eq(directPathFromLastVertexIndexDatasetRow.getInt(0))
						   		 						   .and(distancesVisitedDataset.col("distance").$less(Double.MAX_VALUE))).first().getDouble(2);
						
					double aggregatedDistance = directPathFromLastVertexIndexDatasetRow.getDouble(2) + currentAggregatedDistance;
					
					Row directPathsFromLastVertexRowToBeAddedToDataset = RowFactory.create(directPathFromLastVertexIndexDatasetRow.getInt(1),
																						   directPathFromLastVertexIndexDatasetRow.getInt(0), 
																						   aggregatedDistance, false);

					directPathsFromLastVertexIndexList.add(directPathsFromLastVertexRowToBeAddedToDataset);
				}
				
				Dataset<Row> possibleToBeAddedToDistancesVisitedDataset = sparkSession.createDataFrame(directPathsFromLastVertexIndexList, distancesVisitedDatasetSchema)
															   			  .cache();
				
				distancesVisitedDataset = distancesVisitedDataset.join(possibleToBeAddedToDistancesVisitedDataset, distancesVisitedDataset.col("index")
						 .equalTo(possibleToBeAddedToDistancesVisitedDataset.col("index")), "left");
				
				distancesVisitedDataset = distancesVisitedDataset.map(new MapFunction<Row, Row>() {

					/**
					 * The default serial version UID
					 */
					private static final long serialVersionUID = 1L;

					/**
					 * The call method to perform the map for each row
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
				
				System.out.println("Current state of the Vector of Distances and Visited Vertices:");
				for(Row distancesVisitedRow : distancesVisitedDataset.collectAsList()) {
					System.out.println("- " + distancesVisitedRow);
				}
				
				System.out.println();
				System.out.println();
				
				System.out.println("Next Last Vertex to be visited: " + nextLastVertexIndex);
				System.out.println("Last Vertex visited: " + lastVertexIndex);
				
				System.out.println();
				System.out.println();
				System.out.println();
				
				lastVertexIndex = nextLastVertexIndex;
				
				numVisitedAirports = (int) distancesVisitedDataset.select("visited").where(distancesVisitedDataset.col("visited").$eq$eq$eq(true)).count();	
			}
		}
		
		JavaPairRDD<Integer, Tuple2<Integer, Double>> minimumSpanningTree = distancesVisitedDataset.javaRDD().mapToPair(
				new PairFunction<Row, Integer, Tuple2<Integer, Double>> () {
					
				/**
				 * The default serial version UID
				 */
				private static final long serialVersionUID = 1L;

				/**
				 * The call method to perform the map to pair for each row
				 */
				@Override
				public Tuple2<Integer, Tuple2<Integer, Double>> call(Row row) throws Exception {
					return new Tuple2<Integer, Tuple2<Integer, Double>>(row.getInt(1), new Tuple2<Integer, Double>(row.getInt(0), row.getDouble(2)));
				}
			}
		);
		
		return minimumSpanningTree;
	}
	
	public static JavaRDD<MatrixEntry> getMinimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaRDD(SparkSession sparkSession, JavaPairRDD<Integer, Tuple2<Integer, Double>> minimumSpanningTreeJavaPairRDD,
																		    								CoordinateMatrix coordinateAdjacencyMatrix) {

		JavaRDD<MatrixEntry> coordinateAdjacencyMatrixEntriesJavaRDD = coordinateAdjacencyMatrix.entries().toJavaRDD();
		
		JavaRDD<Row> cordinateAdjacencyMatrixRowsJavaRDD = coordinateAdjacencyMatrixEntriesJavaRDD
																   .map(matrixEntry -> RowFactory.create((int) matrixEntry.i(), (int) matrixEntry.j(), matrixEntry.value()));
		
		Dataset<Row> coordinateAdjacencyMatrixRowsDataset = sparkSession.createDataFrame(cordinateAdjacencyMatrixRowsJavaRDD, coordinateAdjacencyMatrixEntriesDatasetSchema)
																        .sort(functions.asc("row_index"));
		
		JavaRDD<Row> minimumSpanningTreeRowsJavaRDD = minimumSpanningTreeJavaPairRDD.map(tuple -> RowFactory.create((int) tuple._1(), (int) tuple._2()._1(), tuple._2()._2()));
		
		Dataset<Row> minimumSpanningTreeRowsDataset = sparkSession.createDataFrame(minimumSpanningTreeRowsJavaRDD, coordinateAdjacencyMatrixEntriesDatasetSchema)
		        												  .sort(functions.asc("row_index"));
		
		Dataset<Row> minimumSpanningTreeComplementRowsDataset = coordinateAdjacencyMatrixRowsDataset.join(minimumSpanningTreeRowsDataset,
																		(coordinateAdjacencyMatrixRowsDataset.col("row_index").equalTo(minimumSpanningTreeRowsDataset.col("row_index")))
																		.and((coordinateAdjacencyMatrixRowsDataset.col("column_index").equalTo(minimumSpanningTreeRowsDataset.col("column_index")))),
				 														"leftanti");
		
		minimumSpanningTreeComplementRowsDataset = minimumSpanningTreeComplementRowsDataset.join(minimumSpanningTreeRowsDataset,
				(coordinateAdjacencyMatrixRowsDataset.col("row_index").equalTo(minimumSpanningTreeRowsDataset.col("column_index")))
				.and((coordinateAdjacencyMatrixRowsDataset.col("column_index").equalTo(minimumSpanningTreeRowsDataset.col("row_index")))), "leftanti");
		
		JavaRDD<MatrixEntry> minimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaRDD = minimumSpanningTreeComplementRowsDataset.toJavaRDD()
																							.map(row -> new MatrixEntry(row.getInt(0), row.getInt(1), row.getDouble(2)));
		
		return minimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaRDD;
	}
	
	public static JavaPairRDD<Integer, Tuple2<Integer, Double>> computeMinimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaPairRDD
																(JavaRDD<MatrixEntry> minimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaRDD) {
		
		JavaPairRDD<Integer, Tuple2<Integer, Double>> minimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaPairRDD = 
				minimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaRDD.mapToPair(
						new PairFunction<MatrixEntry, Integer, Tuple2<Integer, Double>> () {

							/**
							 * The default serial version UID
							 */
							private static final long serialVersionUID = 1L;

							/**
							 * 
							 */
							@Override
							public Tuple2<Integer, Tuple2<Integer, Double>> call(MatrixEntry matrixEntry) throws Exception {
								return new Tuple2<Integer, Tuple2<Integer, Double>>((int) matrixEntry.i(), new Tuple2<Integer, Double>((int) matrixEntry.j(), matrixEntry.value()));
							}
						}
				).cache();
		
		return minimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaPairRDD;
	}
	
	/**
	 * 
	 * @param minimumSpanningTreeComplementJavaRDD
	 * @return
	 */
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
		
		System.out.println();
		
		System.out.println("The total aggregated sum of the Distances (Average Departure Delays) for each Airport of the Mininum Spanning Tree Complement:");
		for(Tuple2<Integer, Double> tuple : sumOfDepartureDelaysByAirportFromMinimumSpanningTreeComplementJavaPairRDD.collect()) {
			System.out.println("- " + tuple._1() + " = " + tuple._2());
		}
		
		Tuple2<Integer, Double> bottleneckAirportFromMinimumSpanningTreeComplementTuple = 
				sumOfDepartureDelaysByAirportFromMinimumSpanningTreeComplementJavaPairRDD.reduce((airport1, airport2) -> airport1._2() > airport2._2() ? airport1 : airport2);
		
		return bottleneckAirportFromMinimumSpanningTreeComplementTuple;
	}
	
	/**
	 * 
	 * @param sparkSession
	 * @param initialCoordinateAdjacencyMatrix
	 * @param bottleneckAirportFromMinimumSpanningTreeComplementJavaRDD
	 * @param reduceFactor
	 * @return
	 */
	public static CoordinateMatrix buildCoordinateAdjacencyMatrixWithBottleneckAirportReducedByFactor
			(SparkSession sparkSession, CoordinateMatrix initialCoordinateAdjacencyMatrix, Tuple2<Integer, Double> bottleneckAirportFromMinimumSpanningTreeComplementJavaRDD, float reduceFactor) {
		
		JavaRDD<MatrixEntry> coordinateAdjacencyMatrixEntriesJavaRDD = initialCoordinateAdjacencyMatrix.entries().toJavaRDD();
		
		JavaRDD<Row> initialCoordinateAdjacencyMatrixRowsJavaRDD = coordinateAdjacencyMatrixEntriesJavaRDD
																   .map(matrixEntry -> RowFactory.create((int) matrixEntry.i(), (int) matrixEntry.j(), matrixEntry.value()));
		
		Dataset<Row> initialCoordinateAdjacencyMatrixRowsDataset = sparkSession.createDataFrame(initialCoordinateAdjacencyMatrixRowsJavaRDD, coordinateAdjacencyMatrixEntriesDatasetSchema)
																        .sort(functions.asc("row_index"));
		
		Dataset<Row> bottleneckRoutesFromCoordinateAdjacencyMatrixRowsDataset = initialCoordinateAdjacencyMatrixRowsDataset
																			   .filter(initialCoordinateAdjacencyMatrixRowsDataset.col("row_index").$eq$eq$eq(bottleneckAirportFromMinimumSpanningTreeComplementJavaRDD._1())
																				   .or(initialCoordinateAdjacencyMatrixRowsDataset.col("column_index").$eq$eq$eq(bottleneckAirportFromMinimumSpanningTreeComplementJavaRDD._1())))
																			   .withColumn("distance_reduced_factor", initialCoordinateAdjacencyMatrixRowsDataset.col("distance").$times(reduceFactor));
		
		Dataset<Row> bottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset = bottleneckRoutesFromCoordinateAdjacencyMatrixRowsDataset
																				  .select("row_index", "column_index", "distance_reduced_factor")
																				  .withColumnRenamed("distance_reduced_factor", "distance");
		
		Dataset<Row> coordinateAdjacencyMatrixRowsDatasetWithoutBottleneckAirport = initialCoordinateAdjacencyMatrixRowsDataset
						  .join(bottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset,
								initialCoordinateAdjacencyMatrixRowsDataset.col("row_index").equalTo(bottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset.col("row_index"))
								    .and(initialCoordinateAdjacencyMatrixRowsDataset.col("column_index").equalTo(bottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset.col("column_index")))
							   .or(initialCoordinateAdjacencyMatrixRowsDataset.col("row_index").equalTo(bottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset.col("row_index"))
									    .and(initialCoordinateAdjacencyMatrixRowsDataset.col("column_index").equalTo(bottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset.col("column_index")))), 
								  
						  "leftanti").sort(functions.asc("row_index"));
		
		Dataset<Row> coordinateAdjacencyMatrixRowsDatasetWithBottleneckRoutesReducedByFactor = coordinateAdjacencyMatrixRowsDatasetWithoutBottleneckAirport
																							   .union(bottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset)
																							   .sort(functions.asc("row_index"));
		
		RDD<MatrixEntry> coordinateAdjacencyMatrixWithBottleneckRoutesReducedByFactorRDD = 
														 coordinateAdjacencyMatrixRowsDatasetWithBottleneckRoutesReducedByFactor.toJavaRDD()
														 .map(row -> new MatrixEntry(row.getInt(0), row.getInt(1), row.getDouble(2))).rdd();
		
		return new CoordinateMatrix(coordinateAdjacencyMatrixWithBottleneckRoutesReducedByFactorRDD, initialCoordinateAdjacencyMatrix.numRows(), initialCoordinateAdjacencyMatrix.numCols());
	}

	
	/**
	 * 
	 * @param sparkSession
	 * @param minimumSpanningTreeComplementJavaRDD
	 * @param bottleneckAirport
	 * @param reduceFactor
	 * @return
	 */
	public static JavaPairRDD<Integer, Tuple2<Integer, Double>> getMinimumSpanningTreeComplementWithBottleneckAirportReducedByFactorJavaRDD
				(SparkSession sparkSession, JavaPairRDD<Integer, Tuple2<Integer, Double>> minimumSpanningTreeComplementJavaRDD,
				 Tuple2<Integer, Double> bottleneckAirport, float reduceFactor) { 
		
		JavaRDD<Row> minimumSpanningTreeComplementRowsJavaRDD = minimumSpanningTreeComplementJavaRDD
																.map(tuple -> RowFactory.create(tuple._1(), tuple._2()._1(), tuple._2()._2()));
		
		Dataset<Row> minimumSpanningTreeComplementRowsDataset = sparkSession.createDataFrame(minimumSpanningTreeComplementRowsJavaRDD, minimumSpanningTreeDatasetSchema)
																			.sort(functions.asc("origin"));
		
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
												  .equalTo(bottleneckAirportFromMinimumSpanningTreeComplementRowsDataset.col("index")), "leftanti").sort(functions.asc("index"));
		
		Dataset<Row> modifiedMinimumSpanningTreeComplementWithBottleneckAirportReducedByFactorDataset = minimumSpanningTreeComplementRowsDataset
																										.union(bottleneckAirportFromMinimumSpanningTreeComplementRowsDataset);
		
		JavaPairRDD<Integer, Tuple2<Integer, Double>> modifiedMinimumSpanningTreeComplementWithBottleneckAirportReducedByFactorJavaPairRDD = 
													  modifiedMinimumSpanningTreeComplementWithBottleneckAirportReducedByFactorDataset.javaRDD()
													  .mapToPair(row -> new Tuple2<Integer, Tuple2<Integer, Double>>(row.getInt(0), new Tuple2<Integer, Double>(row.getInt(1), row.getDouble(2))));
		
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

		reduceFactor = 0.07588053f;
		
		System.out.println();
		System.out.println("The Reduce Factor to apply to all the Routes going out from the Bottleneck Airport will be: " + reduceFactor);
		System.out.println();
		
		
		// The Dataset (built of Rows), containing All Airports' IDs
		Dataset<Row> allAirportsIDsDataset = getAllAirportsIDsDataset(flightsInfo).cache();
				
		
		// The Map of all Airports by Index
		// (to be used as row's or column's index in Coordinate Adjacency Matrix)
		Dataset<Row> allAirportsByIndexMap = mapAllAirportsByIndex(allAirportsIDsDataset).cache();
		
		
		// Printing the information (for debug) 
		
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
		System.out.println("[ with indexes (origin_num, destination_num) built from the Coordinate Adjacency Matrix ]:");
		System.out.println("- " + allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset.count());
		
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
		System.out.println("The content of the Coordinate Adjacency Matrix is:");
		for(MatrixEntry matrixEntry : matrixEntryList)
			System.out.println("(" + matrixEntry.i() + "," + matrixEntry.j() + ") = " + matrixEntry.value());
		
		System.out.println();
		System.out.println();
		
		JavaRDD<Integer> allAirportsIndexesJavaRDD = allAirportsByIndexMap.select("index").javaRDD().map(x -> x.getInt(0));
		
		JavaPairRDD<Integer, Tuple2<Integer, Double>> minimumSpanningTreeJavaPairRDD = 
				computeMinimumSpanningTreeJavaPairRDD(sparkSession, sqlContext, allAirportsIndexesJavaRDD, coordinateAdjacencyMatrix, numAllAirports).cache();
		
		System.out.println();
		
		System.out.println("The content of the Minimum Spanning Tree (Prim's Algorithm) is:");
		for(Tuple2<Integer, Tuple2<Integer, Double>> minimumSpanningTreePair : minimumSpanningTreeJavaPairRDD.collect())
			System.out.println("- (" + minimumSpanningTreePair._1() + "," + minimumSpanningTreePair._2()._1() + ") = " + minimumSpanningTreePair._2()._2());
		
		JavaRDD<MatrixEntry> minimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaRDD = 
					getMinimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaRDD(sparkSession, minimumSpanningTreeJavaPairRDD, coordinateAdjacencyMatrix).cache();
		
		Tuple2<Integer, Double> bottleneckAirportFromMinimumSpanningTreeComplementJavaRDD = 
					getBottleneckAirportFromMinimumSpanningTreeComplementJavaRDD(minimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaRDD);
		
		System.out.println();
		
		System.out.println("The Bottleneck Airport with the highest aggregated Average Departure Delay is:");
		System.out.println("- " + bottleneckAirportFromMinimumSpanningTreeComplementJavaRDD._1() + " = " + bottleneckAirportFromMinimumSpanningTreeComplementJavaRDD._2());
		
		CoordinateMatrix coordinateAdjacencyMatrixWithBottleneckAirportReducedByFactor = 
				buildCoordinateAdjacencyMatrixWithBottleneckAirportReducedByFactor(sparkSession, coordinateAdjacencyMatrix, bottleneckAirportFromMinimumSpanningTreeComplementJavaRDD, reduceFactor);
		
		// The Java RDD containing all the Matrix Entries of
		// the Adjacency Matrix (Coordinate Matrix) to represent the Graph of
		// Average Departure Delays between all two Airports (Disregarding Airport's Origin and Destination),
		// containing the Vertexes (Airports) and its weights (Average Departure Delays)
		JavaRDD<MatrixEntry> matrixEntryWithBottleneckAirportReducedByFactorJavaRDD = coordinateAdjacencyMatrixWithBottleneckAirportReducedByFactor.entries().toJavaRDD().cache();
		
		// The list containing all the Matrix Entries of
		// the Adjacency Matrix (Coordinate Matrix) to represent the Graph of
		// Average Departure Delays between all two Airports (Disregarding Airport's Origin and Destination),
		// containing the Vertexes (Airports) and its weights (Average Departure Delays)
		List<MatrixEntry> matrixEntryWithBottleneckAirportReducedByFactorList = matrixEntryWithBottleneckAirportReducedByFactorJavaRDD.collect();
		
		System.out.println();
		
		System.out.println("The content of the Coordinate Adjacency Matrix with Bootleneck Airport Reduced by Factor is:");
		for(MatrixEntry matrixEntry : matrixEntryWithBottleneckAirportReducedByFactorList) {
			System.out.println("- (" + matrixEntry.i() + "," + matrixEntry.j() + ") = " + matrixEntry.value());
		}
		
		System.out.println();
		
		JavaPairRDD<Integer, Tuple2<Integer, Double>> minimumSpanningTreeWithBottleneckAirportReducedByFactorJavaPairRDD = 
				computeMinimumSpanningTreeJavaPairRDD(sparkSession, sqlContext, allAirportsIndexesJavaRDD, coordinateAdjacencyMatrixWithBottleneckAirportReducedByFactor, numAllAirports).cache();
		
		System.out.println();
		
		System.out.println("The content of the Minimum Spanning Tree (Prim's Algorithm) with Bottleneck Airport Reduced by Factor is:");
		for(Tuple2<Integer, Tuple2<Integer, Double>> minimumSpanningTreeWithBottleneckAirportReducedByFactorPair : minimumSpanningTreeWithBottleneckAirportReducedByFactorJavaPairRDD.collect())
			System.out.println("(" + minimumSpanningTreeWithBottleneckAirportReducedByFactorPair._1() + "," + minimumSpanningTreeWithBottleneckAirportReducedByFactorPair._2()._1() + ") = " + minimumSpanningTreeWithBottleneckAirportReducedByFactorPair._2()._2());
		
		// Terminate the Spark Session
		sparkSession.stop();
	}
}