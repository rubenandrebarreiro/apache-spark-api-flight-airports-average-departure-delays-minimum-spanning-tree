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

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.sql.Column;
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
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class FlightAnalyser {

	
	// Constants/Invariables:
	
	// The file containing the sample of Flights in use
	private static final String DefaulftFile = "data/flights_3.csv";
	
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
		
		Dataset<Row> allOriginAirportDataset = flightsDataset.select("origin_id").withColumnRenamed("origin_id", "id").persist(StorageLevel.MEMORY_ONLY());
		
		Dataset<Row> allDestinationAirportDataset = flightsDataset.select("destination_id").withColumnRenamed("destination_id", "id").persist(StorageLevel.MEMORY_ONLY());
		
		Dataset<Row> allAirportsDataset = allOriginAirportDataset.union(allDestinationAirportDataset).distinct().persist(StorageLevel.MEMORY_ONLY());
		
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
			
			Dataset<Row> allDelaysOfTheFlightsBetweenTwoAirportsDataset = flightsDataset.select("origin_id", "destination_id", "departure_delay")
																						.persist(StorageLevel.MEMORY_ONLY());
			
			Dataset<Row> allDelaysOfTheFlightsBetweenTwoAirportsInvertedDataset = allDelaysOfTheFlightsBetweenTwoAirportsDataset
																				  .select("destination_id", "origin_id", "departure_delay")
																				  .persist(StorageLevel.MEMORY_ONLY());
			
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
			
			Dataset<Row> allDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = sqlContext.sql(sqlQueryRemovePairDuplicates)
																													   .persist(StorageLevel.MEMORY_ONLY());	
			
			Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
													allDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
													.groupBy("origin_id", "destination_id").avg("departure_delay").orderBy("origin_id")
													.persist(StorageLevel.MEMORY_ONLY());
		
			Dataset<Row> allPossibleOriginAirportsByIndexMap = allAirportsByIndexMap.withColumnRenamed("id", "origin_id")
																					.withColumnRenamed("index", "origin_index")
																					.persist(StorageLevel.MEMORY_ONLY());
			
			Dataset<Row> allPossibleDestinationAirportsByIndexMap = allAirportsByIndexMap.withColumnRenamed("id", "destination_id")
																						 .withColumnRenamed("index", "destination_index")
																						 .persist(StorageLevel.MEMORY_ONLY());
			
			int numPartitionsOfAllPossibleOriginAirportsByIndexMap = allPossibleOriginAirportsByIndexMap.rdd().getNumPartitions();

			// Defines the Dataset of All Average Departure Delays of the Flights, joined with the Possible Origin Airports IDs
			allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
					allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
					.repartition(numPartitionsOfAllPossibleOriginAirportsByIndexMap)
					.join(allPossibleOriginAirportsByIndexMap,
						  allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset.col("origin_id")
						  .equalTo(allPossibleOriginAirportsByIndexMap.col("origin_id")), "left").persist(StorageLevel.MEMORY_ONLY());
			
			int numPartitionsOfAllPossibleDestinationAirportsByIndexMap = allPossibleDestinationAirportsByIndexMap.rdd().getNumPartitions();
			
			// Defines the Dataset of All Average Departure Delays of the Flights, joined with the Possible Destination Airports IDs
			allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
					allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
					.repartition(numPartitionsOfAllPossibleDestinationAirportsByIndexMap)
					.join(allPossibleDestinationAirportsByIndexMap,
						  allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset.col("destination_id")
						  .equalTo(allPossibleDestinationAirportsByIndexMap.col("destination_id")), "left").persist(StorageLevel.MEMORY_ONLY());
			
			// Defines the Dataset of All Average Departure Delays of The Flights
			// Between Any Two Airports Origin and Destination Dataset
			allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
					allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
							.select("origin_index", "t1.origin_id", "destination_index", "t1.destination_id", "avg(departure_delay)")
							.persist(StorageLevel.MEMORY_ONLY());
			
			// Defines the Dataset of All Average Delays of The Flights
			// Between Any Two Airports, Disregarding Origin and Destination Dataset, unified with inverted indexes
			allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
					allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
					.union(allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
							.select("destination_index", "t1.destination_id", "origin_index", "t1.origin_id", "avg(departure_delay)"))
					.persist(StorageLevel.MEMORY_ONLY());
			
			return allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset
					.sort(functions.asc("origin_index"));
	}
	
	/**
	 * Returns all Airports mapped by Index, ordered by ID. 
	 * 
	 * @param allAirportsIDsDataset the Dataset of all Airports' IDs
	 * 
	 * @return all Airports mapped by Index, ordered by ID
	 */
	public static Dataset<Row> mapAllAirportsByIndex(Dataset<Row> allAirportsIDsDataset) {
		
		allAirportsIDsDataset = allAirportsIDsDataset.select("id").orderBy("id").distinct().persist(StorageLevel.MEMORY_ONLY());
		
		return allAirportsIDsDataset.withColumn("index", functions.row_number().over(Window.orderBy("id"))).persist(StorageLevel.MEMORY_ONLY());
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
								allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset.orderBy("origin_id")
								.javaRDD().persist(StorageLevel.MEMORY_ONLY());
					
						JavaPairRDD<Long, Tuple2<Tuple2<Long, Long>, Tuple2<Long, Double>>> 
								averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD = 
										allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationJavaRDD.mapToPair(							
												row -> new Tuple2<Long, Tuple2<Tuple2<Long, Long>, Tuple2<Long, Double>>> 
													  (row.getLong(1), new Tuple2<Tuple2<Long, Long>, Tuple2<Long, Double>>
													  				  		(new Tuple2<Long, Long>((long) row.getInt(0), (long) row.getInt(2)),
												   		 							new Tuple2<Long, Double>(row.getLong(3), row.getDouble(4))))
									    ).persist(StorageLevel.MEMORY_ONLY());
	
						return averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD;
	}
	
	/**
	 * Returns the Adjacency Matrix (Coordinate Matrix) to represent the Graph of Average Departure Delays
	 * between all two Airports (Disregarding Airport's Origin and Destination),
	 * containing the Vertexes (Airports) and its weights (Average Departure Delays).
	 * 
	 * @param averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD
	 * @param matrixDimensions the dimensions of the Adjacency Matrix (Coordinate Matrix)
	 * 
	 * @return the Adjacency Matrix (Coordinate Matrix) to represent the Graph of Average Departure Delays
	 *         between all two Airports (Disregarding Airport's Origin and Destination),
	 *         containing the Vertexes (Airports) and its weights (Average Departure Delays)
	 */
	public static CoordinateMatrix buildCoordinateAdjacencyMatrix
										(JavaPairRDD< Long, Tuple2<Tuple2<Long, Long>, Tuple2<Long, Double>> > 
										 averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD,
										 long matrixDimensions) {
		
		JavaRDD<MatrixEntry> matrixEntriesJavaRDD = 
				averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD.map(
						tuple -> {
							long row = tuple._2()._1()._1();
							long col = tuple._2()._1()._2();
							
							double matrixEntryValue = tuple._2()._2()._2();
							
							return new MatrixEntry(row, col, matrixEntryValue);
						}
				).persist(StorageLevel.MEMORY_ONLY());
		
		return new CoordinateMatrix(matrixEntriesJavaRDD.rdd(), matrixDimensions, matrixDimensions);
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
																									  JavaRDD<MatrixEntry> coordinateAdjacencyMatrixJavaRDD,
																									  long numAllAirports) {  
		
		JavaRDD<Row> coordinateAdjacencyMatrixRowsJavaRDD = coordinateAdjacencyMatrixJavaRDD
															.map(matrixEntry -> RowFactory.create((int) matrixEntry.i(), (int) matrixEntry.j(), matrixEntry.value()))
															.persist(StorageLevel.MEMORY_ONLY());
		
		//coordinateAdjacencyMatrixEntriesJavaRDD.checkpoint();
		//coordinateAdjacencyMatrixEntriesJavaRDD.unpersist();
		
		Dataset<Row> coordinateAdjacencyMatrixRowsDataset = sparkSession.createDataFrame(coordinateAdjacencyMatrixRowsJavaRDD, coordinateAdjacencyMatrixEntriesDatasetSchema)
																        .sort(functions.asc("row_index")).persist(StorageLevel.MEMORY_ONLY());
		
		//coordinateAdjacencyMatrixEntriesJavaRDD.checkpoint();
		//coordinateAdjacencyMatrixEntriesJavaRDD.unpersist();
		
		Random random = new Random();
		
		boolean validInitialVertex = false;
		int initialVertexIndex = -1;
		
		while(!validInitialVertex) {
			initialVertexIndex = random.nextInt((int) (numAllAirports + 1L));
			
			Dataset<Row> initialVertexDataset = coordinateAdjacencyMatrixRowsDataset
												.where(coordinateAdjacencyMatrixRowsDataset.col("row_index").$eq$eq$eq(initialVertexIndex))
												.persist(StorageLevel.MEMORY_ONLY());
		
			if(!initialVertexDataset.isEmpty())
				validInitialVertex = true;
			
			initialVertexDataset.unpersist();
		}
		
		// TODO - Just for debug
		//initialVertexIndex = 6;
		
		System.out.println();
		System.out.println("The initial vertex to compute the Minimum Spanning Tree (M.S.T.) is: " + initialVertexIndex);
		
		JavaRDD<Row> distancesVisitedJavaRDD = allAiportIndexesRDD.map(index -> RowFactory.create(index, -1, Double.MAX_VALUE, false))
																  .persist(StorageLevel.MEMORY_ONLY());
		
		Dataset<Row> distancesVisitedDataset = sparkSession.createDataFrame(distancesVisitedJavaRDD, distancesVisitedDatasetSchema)
														   .sort(functions.asc("index"))
														   .persist(StorageLevel.MEMORY_ONLY());
		
		//distancesVisitedJavaRDD.checkpoint();
		//distancesVisitedJavaRDD.unpersist();
		
		distancesVisitedDataset = distancesVisitedDataset.where(distancesVisitedDataset.col("index").$bang$eq$eq(initialVertexIndex))
														 .persist(StorageLevel.MEMORY_ONLY());
		
		Row initialVertexRow = RowFactory.create(initialVertexIndex, initialVertexIndex, 0.0, true);
		
		List<Row> initialVertexRowList = new ArrayList<>();
		initialVertexRowList.add(initialVertexRow);
		
		Dataset<Row> initialVertexRowInDistancesVisitedDataset = sparkSession.createDataFrame(initialVertexRowList, distancesVisitedDatasetSchema)
																			 .persist(StorageLevel.DISK_ONLY());
		
		Dataset<Row> directPathsFromInitialVertexDataset = coordinateAdjacencyMatrixRowsDataset
														   .where(coordinateAdjacencyMatrixRowsDataset.col("row_index").$eq$eq$eq(initialVertexIndex))
														   .persist(StorageLevel.MEMORY_ONLY());
		
		System.out.println();
		
		// Print all the direct paths/routes from the initial vertex
		//System.out.println("Possible direct paths from the initial vertex:");
		//for(Row directPathFromInitialVertex : directPathsFromInitialVertexDataset.collectAsList())
			//System.out.println("- " + directPathFromInitialVertex);
		
		//System.out.println();
		//System.out.println();
		
		int numOfPartitionsOfDirectPathsFromInitialVertexDataset = directPathsFromInitialVertexDataset.rdd().getNumPartitions();
		
		distancesVisitedDataset = distancesVisitedDataset.repartition(numOfPartitionsOfDirectPathsFromInitialVertexDataset)
														 .join(directPathsFromInitialVertexDataset, distancesVisitedDataset.col("index")
														 .equalTo(directPathsFromInitialVertexDataset.col("column_index")), "leftanti").sort(functions.asc("index"))
														 .persist(StorageLevel.MEMORY_ONLY());
		
		JavaRDD<Row> directPathsFromInitialVertexRDD = directPathsFromInitialVertexDataset.javaRDD()
													   .map(directPathsFromInitialVertexRow -> RowFactory.create(directPathsFromInitialVertexRow.getInt(1), 0,
																					 									 directPathsFromInitialVertexRow.getDouble(2), false))
													   .persist(StorageLevel.MEMORY_ONLY());
		
		//directPathsFromInitialVertexDataset.checkpoint();
		//directPathsFromInitialVertexDataset.unpersist();
		
		Dataset<Row> directPathsFromInitialVertexRowDataset = sparkSession.createDataFrame(directPathsFromInitialVertexRDD, distancesVisitedDatasetSchema)
																		  .persist(StorageLevel.MEMORY_ONLY());
		
		//directPathsFromInitialVertexRDD.checkpoint();
		//directPathsFromInitialVertexRDD.unpersist();
		
		Column newFromIndexColumn = directPathsFromInitialVertexRowDataset.col("from_index").$plus(initialVertexIndex);
		
		directPathsFromInitialVertexRowDataset = directPathsFromInitialVertexRowDataset
												 .select("index", "from_index", "distance", "visited").withColumn("from_index", newFromIndexColumn)
												 .persist(StorageLevel.MEMORY_ONLY());
		
		distancesVisitedDataset = distancesVisitedDataset.union(initialVertexRowInDistancesVisitedDataset).persist(StorageLevel.DISK_ONLY());
		distancesVisitedDataset = distancesVisitedDataset.union(directPathsFromInitialVertexRowDataset).persist(StorageLevel.DISK_ONLY());
		
		//initialVertexRowInDistancesVisitedDataset.checkpoint();
		//initialVertexRowInDistancesVisitedDataset.unpersist();
		
		//directPathsFromInitialVertexRowDataset.checkpoint();
		//directPathsFromInitialVertexRowDataset.unpersist();
		
		distancesVisitedDataset = distancesVisitedDataset.sort(functions.asc("index")).persist(StorageLevel.MEMORY_ONLY());
		
		// Print the current state of the Vector of Distances and Visited Vertices
		//System.out.println("Current state of the Vector of Distances and Visited Vertices:");
		//for(Row distancesVisitedRow : distancesVisitedDataset.collectAsList())
			//System.out.println("- " + distancesVisitedRow);
		
		//System.out.println();
		//System.out.println();
		
		int lastVertexIndex = initialVertexIndex;
		int nextLastVertexIndex = -1;
		
		int numVisitedAirports = (int) distancesVisitedDataset.select("visited").where(distancesVisitedDataset.col("visited").$eq$eq$eq(true))
															  .persist(StorageLevel.MEMORY_ONLY()).count();
		
		// Repeat the whole process, until all the Airports are visited
		while(numVisitedAirports < numAllAirports) {
			
			System.out.println("Already visited " + numVisitedAirports + " Airports!");
			System.out.println();
			
			Row minDistanceVisitedDataset = distancesVisitedDataset.reduce(new ReduceFunction<Row>() {

				/**
				 * The default serial version UID
				 */
				private static final long serialVersionUID = 1L;
				
				/**
				 * The call method to perform the reduce for each row
				 */
				@Override
				public Row call(Row row1, Row row2) throws Exception {
					
					double distance1 = row1.getDouble(2);
					double distance2 = row2.getDouble(2);
					
					boolean visited1 = row1.getBoolean(3);
					boolean visited2 = row2.getBoolean(3);
					
					if(!visited1 && !visited2)
						return (distance1 < distance2) ? row1 : row2;
					else
						return (visited1) ? row2 : row1;
				}
			});

			Dataset<Row> minInDistancesVisitedDataset = distancesVisitedDataset.select("index", "distance")
																			   .where(distancesVisitedDataset.col("index")
																					  .$eq$eq$eq(minDistanceVisitedDataset.getInt(0)))
																			   .persist(StorageLevel.MEMORY_ONLY());
			
			if(!minInDistancesVisitedDataset.isEmpty()) {				
				Row minInDistancesVisitedDatasetRow = minInDistancesVisitedDataset.first();
				
				//minInDistancesVisitedDataset.checkpoint();
				//minInDistancesVisitedDataset.unpersist();
				
				System.out.println("The Vertex Index with minimum distance is: " + minInDistancesVisitedDatasetRow.getInt(0));
				
				nextLastVertexIndex = minInDistancesVisitedDatasetRow.getInt(0);
				
				Row nextRowToBeVisitedAndChanged;
				
				nextRowToBeVisitedAndChanged = RowFactory.create(minDistanceVisitedDataset.getInt(0), minDistanceVisitedDataset.getInt(1),
																 minDistanceVisitedDataset.getDouble(2), true);
				
				List<Row> nextRowToBeVisitedAndChangedList = new ArrayList<>();
				nextRowToBeVisitedAndChangedList.add(nextRowToBeVisitedAndChanged);
				
				Dataset<Row> minRowInDistancesVisitedDataset = sparkSession.createDataFrame(nextRowToBeVisitedAndChangedList, distancesVisitedDatasetSchema)
																		   .persist(StorageLevel.MEMORY_ONLY());
				
				int numPartitionsOfMinRowInDistancesVisitedDataset = minRowInDistancesVisitedDataset.rdd().getNumPartitions();
				
				distancesVisitedDataset = distancesVisitedDataset.repartition(numPartitionsOfMinRowInDistancesVisitedDataset)
																 .join(minRowInDistancesVisitedDataset, distancesVisitedDataset.col("index")
						 										 .equalTo(minRowInDistancesVisitedDataset.col("index")), "leftanti")
																 .persist(StorageLevel.MEMORY_ONLY());
				
				distancesVisitedDataset = distancesVisitedDataset.union(minRowInDistancesVisitedDataset).sort(functions.asc("index"))
																 .persist(StorageLevel.MEMORY_ONLY());
				
				//minRowInDistancesVisitedDataset.checkpoint();
				//minRowInDistancesVisitedDataset.unpersist();
				
				Dataset<Row> directPathsFromLastVertexIndexInTheCoordinateAdjacencyMatrixDataset = coordinateAdjacencyMatrixRowsDataset
																								   .select("row_index", "column_index", "distance").withColumnRenamed("distance", "route_distance")
																								   .where(coordinateAdjacencyMatrixRowsDataset.col("row_index").$eq$eq$eq(nextLastVertexIndex)
																								   .and(coordinateAdjacencyMatrixRowsDataset.col("column_index").$bang$eq$eq(lastVertexIndex)))
																								   .persist(StorageLevel.MEMORY_ONLY());
				
				//coordinateAdjacencyMatrixRowsDataset.checkpoint();
				//coordinateAdjacencyMatrixRowsDataset.unpersist();
				
				Dataset<Row> directPathsFromLastVertexIndexDataset = distancesVisitedDataset.join(directPathsFromLastVertexIndexInTheCoordinateAdjacencyMatrixDataset,
																									distancesVisitedDataset.col("index")
																									.$eq$eq$eq(directPathsFromLastVertexIndexInTheCoordinateAdjacencyMatrixDataset.col("row_index"))
																							 .and(distancesVisitedDataset.col("distance")
																								  .$less(Double.MAX_VALUE)), "left")
																							 .persist(StorageLevel.MEMORY_ONLY());
				
				//minRowInDistancesVisitedDataset.checkpoint();
				//minRowInDistancesVisitedDataset.unpersist();
				
				directPathsFromLastVertexIndexDataset = directPathsFromLastVertexIndexDataset.where(directPathsFromLastVertexIndexDataset.col("row_index").isNotNull()
																								    .and(directPathsFromLastVertexIndexDataset.col("column_index").isNotNull())
																								    .and(directPathsFromLastVertexIndexDataset.col("route_distance").isNotNull()))
																							 .persist(StorageLevel.MEMORY_ONLY());
				
				JavaRDD<Row> directPathsFromLastVertexRDD = directPathsFromLastVertexIndexDataset.javaRDD()
															.map(
																	directPathsFromLastVertexRow -> {
																		
																		double aggregatedDistance = ( directPathsFromLastVertexRow.getDouble(2) + directPathsFromLastVertexRow.getDouble(6) );
																		
																		return RowFactory.create(directPathsFromLastVertexRow.getInt(5), directPathsFromLastVertexRow.getInt(4),
																								 aggregatedDistance, false);
																	}
															).persist(StorageLevel.MEMORY_ONLY());
				
				//directPathsFromLastVertexIndexDataset.checkpoint();
				//directPathsFromLastVertexIndexDataset.unpersist();
				
				Dataset<Row> directPathsFromLastVertexRDDRowDataset = sparkSession.createDataFrame(directPathsFromLastVertexRDD, distancesVisitedDatasetSchema)
																				  .persist(StorageLevel.MEMORY_ONLY());
				
				//directPathsFromLastVertexRDD.checkpoint();
				//directPathsFromLastVertexRDD.unpersist();
				
				int numPartitionsOfPossiblePathsToBeAddedToDistancesVisitedDataset = directPathsFromLastVertexRDDRowDataset.rdd().getNumPartitions();
				
				distancesVisitedDataset = distancesVisitedDataset.repartition(numPartitionsOfPossiblePathsToBeAddedToDistancesVisitedDataset)
																 .join(directPathsFromLastVertexRDDRowDataset,
																		 	distancesVisitedDataset.col("from_index").notEqual(distancesVisitedDataset.col("index"))
																		 	.and(distancesVisitedDataset.col("index")
																		 			.equalTo(directPathsFromLastVertexRDDRowDataset.col("index"))), "left")
																 .persist(StorageLevel.MEMORY_ONLY());
				
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
				RowEncoder.apply(distancesVisitedDatasetSchema)).persist(StorageLevel.MEMORY_ONLY());
				
				//System.out.println("Current state of the Vector of Distances and Visited Vertices:");
				//for(Row distancesVisitedRow : distancesVisitedDataset.collectAsList())
					//System.out.println("- " + distancesVisitedRow);
				
				//System.out.println();
				//System.out.println();
				
				System.out.println("Next Last Vertex to be visited: " + nextLastVertexIndex);
				System.out.println("Last Vertex visited: " + lastVertexIndex);
				
				System.out.println();
				System.out.println();
				System.out.println();
				
				lastVertexIndex = nextLastVertexIndex;
				
				numVisitedAirports = (int) distancesVisitedDataset.select("visited").where(distancesVisitedDataset.col("visited").$eq$eq$eq(true)).count();
				
				minInDistancesVisitedDataset.unpersist();
				directPathsFromLastVertexIndexInTheCoordinateAdjacencyMatrixDataset.unpersist();
				directPathsFromLastVertexIndexDataset.unpersist();
				directPathsFromLastVertexRDD.unpersist();
				directPathsFromLastVertexRDDRowDataset.unpersist();
			} 
		}
		
		JavaPairRDD<Integer, Tuple2<Integer, Double>> minimumSpanningTree = 
				distancesVisitedDataset.javaRDD().mapToPair(row -> new Tuple2<Integer, Tuple2<Integer, Double>>
																  (row.getInt(1), new Tuple2<Integer, Double>(row.getInt(0), row.getDouble(2))))
				.persist(StorageLevel.MEMORY_ONLY());
		
		//distancesVisitedDataset.checkpoint();
		//distancesVisitedDataset.unpersist();
		
		coordinateAdjacencyMatrixRowsJavaRDD.unpersist();
		coordinateAdjacencyMatrixRowsDataset.unpersist();
		distancesVisitedJavaRDD.unpersist();
		distancesVisitedDataset.unpersist();
		initialVertexRowInDistancesVisitedDataset.unpersist();
		directPathsFromInitialVertexDataset.unpersist();
		directPathsFromInitialVertexRDD.unpersist();
		directPathsFromInitialVertexRowDataset.unpersist();
		
		return minimumSpanningTree;
	}
	
	/**
	 * Returns the Minimum Spanning Tree's (M.S.T.) Complement, as a format of, Coordinate Adjacency Matrix, represented with a Java RDD of Matrix Entries.
	 * 
	 * @param sparkSession a Spark's Session
	 * @param minimumSpanningTreeJavaPairRDD the Minimum Spanning Tree (M.S.T.), as a format of, a JavaPair RDD
	 * @param coordinateAdjacencyMatrix the Coordinate Adjacency Matrix, as a format of, a Java RDD, to represent the graph
	 * 
	 * @return the Minimum Spanning Tree's (M.S.T.) Complement, as a format of, Coordinate Adjacency Matrix, represented with a Java RDD of Matrix Entries
	 */
	public static JavaRDD<MatrixEntry> getMinimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaRDD
		(SparkSession sparkSession, JavaPairRDD<Integer, Tuple2<Integer, Double>> minimumSpanningTreeJavaPairRDD, JavaRDD<MatrixEntry> coordinateAdjacencyMatrixJavaRDD) {
			
			JavaRDD<Row> cordinateAdjacencyMatrixRowsJavaRDD = coordinateAdjacencyMatrixJavaRDD
																	   .map(matrixEntry -> RowFactory.create((int) matrixEntry.i(), (int) matrixEntry.j(), matrixEntry.value()));		
			Dataset<Row> coordinateAdjacencyMatrixRowsDataset = sparkSession.createDataFrame(cordinateAdjacencyMatrixRowsJavaRDD, coordinateAdjacencyMatrixEntriesDatasetSchema)
																	        .sort(functions.asc("row_index")).persist(StorageLevel.MEMORY_ONLY());
			
			JavaRDD<Row> minimumSpanningTreeRowsJavaRDD = minimumSpanningTreeJavaPairRDD.map(tuple -> RowFactory.create((int) tuple._1(), (int) tuple._2()._1(), tuple._2()._2()))
																						.persist(StorageLevel.MEMORY_ONLY());
			
			Dataset<Row> minimumSpanningTreeRowsDataset = sparkSession.createDataFrame(minimumSpanningTreeRowsJavaRDD, coordinateAdjacencyMatrixEntriesDatasetSchema)
			        												  .sort(functions.asc("row_index")).persist(StorageLevel.MEMORY_ONLY());
			
			int numPartitionsOfMinimumSpanningTreeRowsDataset = minimumSpanningTreeRowsDataset.rdd().getNumPartitions();
			
			Dataset<Row> minimumSpanningTreeComplementRowsDataset = coordinateAdjacencyMatrixRowsDataset
																    .repartition(numPartitionsOfMinimumSpanningTreeRowsDataset)
																	.join(minimumSpanningTreeRowsDataset,
																			(coordinateAdjacencyMatrixRowsDataset.col("row_index")
																				.equalTo(minimumSpanningTreeRowsDataset.col("row_index")))
																			.and((coordinateAdjacencyMatrixRowsDataset.col("column_index")
																				.equalTo(minimumSpanningTreeRowsDataset.col("column_index")))),
					 														"leftanti").persist(StorageLevel.MEMORY_ONLY());
			
			minimumSpanningTreeComplementRowsDataset = minimumSpanningTreeComplementRowsDataset
													  .repartition(numPartitionsOfMinimumSpanningTreeRowsDataset)
													  .join(minimumSpanningTreeRowsDataset,
															(coordinateAdjacencyMatrixRowsDataset.col("row_index")
																.equalTo(minimumSpanningTreeRowsDataset.col("column_index")))
															.and((coordinateAdjacencyMatrixRowsDataset.col("column_index")
																.equalTo(minimumSpanningTreeRowsDataset.col("row_index")))),
															"leftanti").persist(StorageLevel.MEMORY_ONLY());
			
			JavaRDD<MatrixEntry> minimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaRDD = minimumSpanningTreeComplementRowsDataset.toJavaRDD()
																								.map(row -> new MatrixEntry(row.getInt(0), row.getInt(1), row.getDouble(2)))
																								.persist(StorageLevel.MEMORY_ONLY());
			
			cordinateAdjacencyMatrixRowsJavaRDD.unpersist();
			coordinateAdjacencyMatrixRowsDataset.unpersist();
			minimumSpanningTreeRowsJavaRDD.unpersist();
			minimumSpanningTreeRowsDataset.unpersist();
			minimumSpanningTreeComplementRowsDataset.unpersist();
			
			return minimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaRDD;
	}
	
	/**
	 * Returns the Bottleneck Airport with the highest aggregated/sum of the Distances (Average Departure Delays).
	 * 
	 * @param minimumSpanningTreeComplementJavaRDD the Minimum Spanning Tree (M.S.T.), as a format of Java RDD
	 * 
	 * @return the Bottleneck Airport with the highest aggregated/sum of the Distances (Average Departure Delays)
	 */
	public static Tuple2<Integer, Double> getBottleneckAirportFromMinimumSpanningTreeComplementJavaRDD(JavaRDD<MatrixEntry> minimumSpanningTreeComplementJavaRDD) {

		JavaPairRDD<Integer, Double> minimumSpanningTreeComplementJavaPairRDD = minimumSpanningTreeComplementJavaRDD
																			    .mapToPair(matrixEntry -> new Tuple2<Integer, Double>((int) matrixEntry.i(), matrixEntry.value()))
																			    .persist(StorageLevel.MEMORY_ONLY());
		
		JavaPairRDD<Integer, Double> sumOfDepartureDelaysByAirportFromMinimumSpanningTreeComplementJavaPairRDD = 
				minimumSpanningTreeComplementJavaPairRDD.reduceByKey((departureDelay1, departureDelay2) -> departureDelay1 + departureDelay2)
												        .persist(StorageLevel.MEMORY_ONLY());
		
		//System.out.println();
		
		//System.out.println("The total aggregated/sum of the Distances (Average Departure Delays) for each Airport of the Mininum Spanning Tree Complement:");
		//for(Tuple2<Integer, Double> tuple : sumOfDepartureDelaysByAirportFromMinimumSpanningTreeComplementJavaPairRDD.collect())
			//System.out.println("- " + tuple._1() + " = " + tuple._2());
		
		Tuple2<Integer, Double> bottleneckAirportFromMinimumSpanningTreeComplementTuple = sumOfDepartureDelaysByAirportFromMinimumSpanningTreeComplementJavaPairRDD
																						  .reduce((airport1, airport2) -> airport1._2() > airport2._2() ? airport1 : airport2);
		
		minimumSpanningTreeComplementJavaRDD.unpersist();
		sumOfDepartureDelaysByAirportFromMinimumSpanningTreeComplementJavaPairRDD.unpersist();
		
		return bottleneckAirportFromMinimumSpanningTreeComplementTuple;
	}
	
	/**
	 * Returns the Coordinate Adjacency Matrix to represent the initial graph
	 * with all the routes going out from the Bottleneck Airport reduced by a given factor.
	 * 
	 * @param sparkSession a Spark's Session
	 * @param initialCoordinateAdjacencyMatrix the Coordinate Adjacency Matrix to represent the initial graph, which will be applied the reduction
	 * @param bottleneckAirportFromMinimumSpanningTreeComplementJavaRDD the Bottleneck Airport with the highest aggregated/sum of Average Departure Delay,
	 * 		  computed from the Minimum Spanning Tree's Complement
	 * @param reduceFactor the Reduce Factor to be applied to all the routes going out from the Bottleneck Airport
	 * 
	 * @return the Coordinate Adjacency Matrix to represent the initial graph
	 *         with all the routes going out from the Bottleneck Airport reduced by a given factor
	 */
	public static JavaRDD<MatrixEntry> buildCoordinateAdjacencyMatrixWithBottleneckAirportReducedByFactor
			(SparkSession sparkSession, JavaRDD<MatrixEntry> initialCoordinateAdjacencyMatrixJavaRDD, 
			 Tuple2<Integer, Double> bottleneckAirportFromMinimumSpanningTreeComplementJavaRDD, float reduceFactor) {
		
		JavaRDD<Row> initialCoordinateAdjacencyMatrixRowsJavaRDD = initialCoordinateAdjacencyMatrixJavaRDD
																   .map(matrixEntry -> RowFactory.create((int) matrixEntry.i(), (int) matrixEntry.j(), matrixEntry.value()))
																   .persist(StorageLevel.MEMORY_ONLY());
		
		Dataset<Row> initialCoordinateAdjacencyMatrixRowsDataset = sparkSession.createDataFrame(initialCoordinateAdjacencyMatrixRowsJavaRDD, 
																								coordinateAdjacencyMatrixEntriesDatasetSchema)
																        						.sort(functions.asc("row_index")).persist(StorageLevel.MEMORY_ONLY());
		
		Dataset<Row> bottleneckRoutesFromCoordinateAdjacencyMatrixRowsDataset = initialCoordinateAdjacencyMatrixRowsDataset
																			   .filter(initialCoordinateAdjacencyMatrixRowsDataset.col("row_index")
																					   .$eq$eq$eq(bottleneckAirportFromMinimumSpanningTreeComplementJavaRDD._1())
																				   .or(initialCoordinateAdjacencyMatrixRowsDataset.col("column_index")
																						.$eq$eq$eq(bottleneckAirportFromMinimumSpanningTreeComplementJavaRDD._1())))
																			   .withColumn("distance_reduced_factor", 
																					       initialCoordinateAdjacencyMatrixRowsDataset.col("distance").$times(reduceFactor))
																			   .persist(StorageLevel.MEMORY_ONLY());
		
		// The Bottleneck Airport's routes with the applied reduce factor
		Dataset<Row> bottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset = bottleneckRoutesFromCoordinateAdjacencyMatrixRowsDataset
																							   .select("row_index", "column_index", "distance_reduced_factor")
																							   .withColumnRenamed("distance_reduced_factor", "distance")
																							   .persist(StorageLevel.MEMORY_ONLY());
		
		int numPartitionsOfBottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset = bottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset
																									.rdd().getNumPartitions();
		
		// The Coordinate Adjacency Matrix, as a format of Dataset (built of rows), without all the routes going out from the Bottleneck Airport
		Dataset<Row> coordinateAdjacencyMatrixRowsDatasetWithoutBottleneckAirport = initialCoordinateAdjacencyMatrixRowsDataset
						  .repartition(numPartitionsOfBottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset)
						  .join(bottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset,
								initialCoordinateAdjacencyMatrixRowsDataset.col("row_index")
									.equalTo(bottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset.col("row_index"))
								    	.and(initialCoordinateAdjacencyMatrixRowsDataset.col("column_index")
					    			.equalTo(bottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset.col("column_index")))
							    .or(initialCoordinateAdjacencyMatrixRowsDataset.col("row_index")
							    	.equalTo(bottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset.col("row_index"))
									    .and(initialCoordinateAdjacencyMatrixRowsDataset.col("column_index")
						    		.equalTo(bottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset.col("column_index")))),
						  "leftanti").sort(functions.asc("row_index")).persist(StorageLevel.MEMORY_ONLY());
		
		// The Coordinate Adjacency Matrix, as a format of Dataset (built of rows), with all the routes going out from the Bottleneck Airport,
		// already with the applied reduce factor
		Dataset<Row> coordinateAdjacencyMatrixRowsDatasetWithBottleneckRoutesReducedByFactor = coordinateAdjacencyMatrixRowsDatasetWithoutBottleneckAirport
																							   .union(bottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset)
																							   .sort(functions.asc("row_index")).persist(StorageLevel.MEMORY_ONLY());
		
		JavaRDD<MatrixEntry> coordinateAdjacencyMatrixWithBottleneckRoutesReducedByFactorRDD = 
														 coordinateAdjacencyMatrixRowsDatasetWithBottleneckRoutesReducedByFactor.toJavaRDD()
														 .map(row -> new MatrixEntry(row.getInt(0), row.getInt(1), row.getDouble(2)))
														 .persist(StorageLevel.MEMORY_ONLY());
		
		initialCoordinateAdjacencyMatrixRowsJavaRDD.unpersist();
		initialCoordinateAdjacencyMatrixRowsDataset.unpersist();
		bottleneckRoutesFromCoordinateAdjacencyMatrixRowsDataset.unpersist();
		bottleneckRoutesReducedByFactorFromCoordinateAdjacencyMatrixRowsDataset.unpersist();
		coordinateAdjacencyMatrixRowsDatasetWithoutBottleneckAirport.unpersist();
		coordinateAdjacencyMatrixRowsDatasetWithBottleneckRoutesReducedByFactor.unpersist();
		
		return coordinateAdjacencyMatrixWithBottleneckRoutesReducedByFactorRDD;
	}
	
	/**
	 * Main method to process the flights' file and analyse it.
	 * 
	 * @param args the file of flights to process and the reduce factor in the interval ]0,1[
	 *        (if no args, uses the file flights.csv, by default)
	 */
	public static void main(String[] args) {
		
		long startExecutionTime = System.currentTimeMillis();
		
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
				
				if(Float.parseFloat(args[1]) > 0.0 && Float.parseFloat(args[1]) < 1.0)
					reduceFactor = Float.parseFloat(args[1]);
				else {
					reduceFactor = 0.0f;
					
					while(reduceFactor == 0.0f) 
						reduceFactor = random.nextFloat();
				}
			}
		}
		
		// Start Spark Session (SparkContext API may also be used) 
		// master("local") indicates local execution
		SparkSession sparkSession = SparkSession.builder().appName("FlightAnalyser")
														  .master("local[*]")
														  .config("spark.driver.memory", "40g")
														  .config("spark.executor.memory", "60g")
														  .config("spark.memory.fraction", "0.3")
														  .config("spark.memory.offHeap.enabled", true)
														  .config("spark.memory.offHeap.size","16g")
														  .config("spark.scheduler.listenerbus.eventqueue.capacity", 20000)
														  .getOrCreate();
		
		// The Spark Context
		SparkContext sparkContext = sparkSession.sparkContext();
		
		// Set the Spark's Context Checkpoint Directory
		sparkContext.setCheckpointDir("checkpoints");
		
		// The SQL Context
		@SuppressWarnings("deprecation")
		SQLContext sqlContext = new SQLContext(sparkContext);
		
		// Only error messages are logged from this point onward
		// comment (or change configuration) if you want the entire log
		sparkContext.setLogLevel("ERROR");
		
		// The Dataset (built of Strings) of
		// the information read from the file by the Spark Session
		Dataset<String> flightsTextFile = sparkSession.read().textFile(fileName).as(Encoders.STRING()).persist(StorageLevel.MEMORY_ONLY());	
		
		// The Dataset (built of Rows) of
		// the information read, previously, from the file by the Spark Session
		Dataset<Row> flightsInfo = 
				flightsTextFile.map((MapFunction<String, Row>) l -> Flight.parseFlight(l), 
				Flight.encoder()).persist(StorageLevel.MEMORY_ONLY());
		
		//flightsTextFile.checkpoint(false);
		//flightsTextFile.unpersist();
		
		// TODO - Just for debug
		//reduceFactor = 0.07588053f;
				
		// The Dataset (built of Rows), containing All Airports' IDs
		Dataset<Row> allAirportsIDsDataset = getAllAirportsIDsDataset(flightsInfo).persist(StorageLevel.MEMORY_ONLY());

		// The maximum number between the Origin and Destination IDs (to be used as the dimensions of Coordinate Adjacency Matrix)
		long numAllAirports = allAirportsIDsDataset.orderBy("id").count();
		
		// The Map of all Airports by Index
		// (to be used as row's or column's index in Coordinate Adjacency Matrix)
		Dataset<Row> allAirportsByIndexMap = mapAllAirportsByIndex(allAirportsIDsDataset).persist(StorageLevel.MEMORY_ONLY());
		
		//allAirportsIDsDataset.checkpoint(false);
		//allAirportsIDsDataset.unpersist();
		
		// Printing the information (for debug)
		System.out.println();
		System.out.println("The number of Airports that will be processed by the Algorithm: " + numAllAirports);
		System.out.println();
		System.out.println();
		System.out.println("The Reduce Factor to apply to all the Routes going out from the Bottleneck Airport will be: " + reduceFactor);
		System.out.println();
		
		// The Dataset (built of Rows), containing all Average Departure Delays of the Flights Between Any Airports
		// (Disregarding Origin and Destination Airports) with Indexes, organised by the corresponding fields
		// with Indexes (origin_num, destination_num) to build the Coordinate Adjacency Matrix
		Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset = 
				getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset(sqlContext, flightsInfo, allAirportsByIndexMap)
				.persist(StorageLevel.MEMORY_ONLY());

		//flightsInfo.checkpoint(false);
		//flightsInfo.unpersist();
		
		// Printing the information (for debug)
		
		// The information about all Average Departure Delays of the Flights Between Any Two Airports,
		// (Disregarding Origin and Destination Airports) with Indexes, organised by the corresponding fields
		// with indexes (origin_num, destination_num) build the Coordinate Adjacency Matrix
		//System.out.println("The Number of Rows/Entries in the Dataset of All Average Departure Delays Of The Flights Between Any Two Airports, Disregarding Origin and Destination");
		//System.out.println("[ with indexes (origin_num, destination_num) built from the Coordinate Adjacency Matrix ]:");
		//System.out.println("- " + allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset.count());
		
		//System.out.println();
		
		// The Java Pair RDD containing a collection (Disregarding Airport's Origin and Destination)
		// of mappings between Airport Origin's ID and a tuple/pair containing other two tuples:
		// - (row's index, column's index) of the Adjacency Coordinate Matrix
		// - (Airport Destination's ID, Average Departure Delays of the Flights between that two Airports)
		JavaPairRDD<Long, Tuple2<Tuple2<Long, Long>, Tuple2<Long, Double>>> 
				allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD = 
						getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD
								(allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset)
								.persist(StorageLevel.MEMORY_ONLY());
		
		//allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset.checkpoint(false);
		//allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset.unpersist();
		
		// The Adjacency Matrix (Coordinate Matrix) to represent the Graph of
		// Average Departure Delays between all two Airports (Disregarding Airport's Origin and Destination),
		// containing the Vertexes (Airports) and its weights (Average Departure Delays)
		CoordinateMatrix coordinateAdjacencyMatrix = buildCoordinateAdjacencyMatrix
									 				(allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesJavaPairRDD,
													 numAllAirports);
		
		JavaRDD<MatrixEntry> coordinateAdjacencyMatrixJavaRDD = coordinateAdjacencyMatrix.entries().toJavaRDD().persist(StorageLevel.MEMORY_ONLY());
				 	
		// The Java RDD containing all the Matrix Entries of
		// the Adjacency Matrix (Coordinate Matrix) to represent the Graph of
		// Average Departure Delays between all two Airports (Disregarding Airport's Origin and Destination),
		// containing the Vertexes (Airports) and its weights (Average Departure Delays)
		//JavaRDD<MatrixEntry> matrixEntryJavaRDD = coordinateAdjacencyMatrix.entries().toJavaRDD().persist(StorageLevel.MEMORY_ONLY());
		
		// The list containing all the Matrix Entries of
		// the Adjacency Matrix (Coordinate Matrix) to represent the Graph of
		// Average Departure Delays between all two Airports (Disregarding Airport's Origin and Destination),
		// containing the Vertexes (Airports) and its weights (Average Departure Delays)
		//List<MatrixEntry> matrixEntryList = matrixEntryJavaRDD.collect();
		
		
		// Printing the information (for debug) 
		
		// The information about all the Matrix Entries of
		// the Adjacency Matrix (Coordinate Matrix) to represent the Graph of
		// Average Departure Delays between all two Airports (Disregarding Airport's Origin and Destination),
		// containing the Vertexes (Airports) and its weights (Average Departure Delays)
		//System.out.println("The content of the Coordinate Adjacency Matrix is:");
		//for(MatrixEntry matrixEntry : matrixEntryList)
			//System.out.println("(" + matrixEntry.i() + "," + matrixEntry.j() + ") = " + matrixEntry.value());
		
		//System.out.println();
		//System.out.println();
		
		// The all Airports' Indexes JavaRDD
		JavaRDD<Integer> allAirportsIndexesJavaRDD = allAirportsByIndexMap.select("index").javaRDD().map(x -> x.getInt(0))
																		  .persist(StorageLevel.MEMORY_ONLY());
		
		//allAirportsByIndexMap.checkpoint(false);
		//allAirportsByIndexMap.unpersist();
		
		flightsTextFile.unpersist();
		allAirportsIDsDataset.unpersist();
		flightsInfo.unpersist();
		allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationWithIndexesDataset.unpersist();
		allAirportsByIndexMap.unpersist();
		
		System.out.println();
		System.out.println();
		
		System.out.println("Starting the computing of the Minimum Spanning Tree - M.S.T. (Prim's Algorithm)...");
		
		// The Minimum Spanning Tree (M.S.T.) as a format of JavaPairRDD
		JavaPairRDD<Integer, Tuple2<Integer, Double>> minimumSpanningTreeJavaPairRDD = 
														computeMinimumSpanningTreeJavaPairRDD(sparkSession, sqlContext, allAirportsIndexesJavaRDD,
																							  coordinateAdjacencyMatrixJavaRDD, numAllAirports)
														.persist(StorageLevel.MEMORY_ONLY());
		
		System.out.println();
		
		System.out.println("The content of the Minimum Spanning Tree (Prim's Algorithm) is:");
		for(Tuple2<Integer, Tuple2<Integer, Double>> minimumSpanningTreePair : minimumSpanningTreeJavaPairRDD.collect())
			System.out.println("- (" + minimumSpanningTreePair._1() + "," + minimumSpanningTreePair._2()._1() + ") = " + minimumSpanningTreePair._2()._2());
		
		
		// The Minimum Spanning Tree's (M.S.T.) Complement as a format of a Coordinate Adjacency Matrix JavaRDD
		JavaRDD<MatrixEntry> minimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaRDD = 
					getMinimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaRDD(sparkSession, minimumSpanningTreeJavaPairRDD, coordinateAdjacencyMatrixJavaRDD)
					.persist(StorageLevel.MEMORY_ONLY());
		
		minimumSpanningTreeJavaPairRDD.unpersist();
	
		//minimumSpanningTreeJavaPairRDD.checkpoint();
		//minimumSpanningTreeJavaPairRDD.unpersist();
		
		// The Bottleneck Airport with the highest aggregated/sum of Average Departure Delay,
		// computed from the Minimum Spanning Tree's Complement
		Tuple2<Integer, Double> bottleneckAirportFromMinimumSpanningTreeComplementJavaRDD = 
					getBottleneckAirportFromMinimumSpanningTreeComplementJavaRDD(minimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaRDD);
		
		minimumSpanningTreeComplementCoordinateAdjacencyMatrixJavaRDD.unpersist();
		
		System.out.println();
		System.out.println();
		
		System.out.println("The Bottleneck Airport with the highest aggregated/sum of Average Departure Delay is:");
		System.out.println("- " + bottleneckAirportFromMinimumSpanningTreeComplementJavaRDD._1() + " => " + bottleneckAirportFromMinimumSpanningTreeComplementJavaRDD._2());
		
		System.out.println();
		System.out.println();
		
		// The Coordinate Adjacency Matrix with Bottleneck Airport reduced by Factor
		JavaRDD<MatrixEntry> coordinateAdjacencyMatrixWithBottleneckAirportReducedByFactorJavaRDD = 
											buildCoordinateAdjacencyMatrixWithBottleneckAirportReducedByFactor(sparkSession, coordinateAdjacencyMatrixJavaRDD,
																											   bottleneckAirportFromMinimumSpanningTreeComplementJavaRDD, reduceFactor)
											.persist(StorageLevel.MEMORY_ONLY());
		
		coordinateAdjacencyMatrixJavaRDD.unpersist();
		
		// The Java RDD containing all the Matrix Entries of
		// the Adjacency Matrix (Coordinate Matrix) to represent the Graph of Average Departure Delays
		// between all two Airports (Disregarding Airport's Origin and Destination),
		// containing the Vertexes (Airports) and its weights (Average Departure Delays)
		//JavaRDD<MatrixEntry> matrixEntryWithBottleneckAirportReducedByFactorJavaRDD = coordinateAdjacencyMatrixWithBottleneckAirportReducedByFactor
																					  //.entries().toJavaRDD().persist(StorageLevel.MEMORY_ONLY());
		
		//System.out.println();
		
		// The list containing all the Matrix Entries of
		// the Adjacency Matrix (Coordinate Matrix) to represent the Graph of Average Departure Delays
		// between all two Airports (Disregarding Airport's Origin and Destination),
		// containing the Vertexes (Airports) and its weights (Average Departure Delays),
		// reduced with a factor applied to all the routes going out of the Bottleneck Airport	
		//System.out.println("The content of the Coordinate Adjacency Matrix with Bootleneck Airport Reduced by Factor is:");
		//for(MatrixEntry matrixEntry : matrixEntryWithBottleneckAirportReducedByFactorJavaRDD.collect())
			//System.out.println("- (" + matrixEntry.i() + "," + matrixEntry.j() + ") = " + matrixEntry.value());
		
		//System.out.println();
		
		// The JavaPair RDD containing all the tuples/pairs of the Minimum Spanning Tree (M.S.T.) recomputed and reduced with
		// a factor applied to all the routes going out of the Bottleneck Airport 
		JavaPairRDD<Integer, Tuple2<Integer, Double>> minimumSpanningTreeWithBottleneckAirportReducedByFactorJavaPairRDD = 
				computeMinimumSpanningTreeJavaPairRDD(sparkSession, sqlContext, allAirportsIndexesJavaRDD,
												      coordinateAdjacencyMatrixWithBottleneckAirportReducedByFactorJavaRDD, numAllAirports)
				.persist(StorageLevel.MEMORY_ONLY());
		
		System.out.println();
		
		//allAirportsIndexesJavaRDD.checkpoint();
		//allAirportsIndexesJavaRDD.unpersist();
		
		// The list containing all the tuples/pairs of the Minimum Spanning Tree (M.S.T.) recomputed and reduced with 
		// a factor applied to all the routes going out of the Bottleneck Airport
		System.out.println("The content of the Minimum Spanning Tree (Prim's Algorithm) with Bottleneck Airport Reduced by Factor is:");
		for(Tuple2<Integer, Tuple2<Integer, Double>> minimumSpanningTreeWithBottleneckAirportReducedByFactorPair : 
				minimumSpanningTreeWithBottleneckAirportReducedByFactorJavaPairRDD.collect())
						System.out.println("(" + minimumSpanningTreeWithBottleneckAirportReducedByFactorPair._1() + ","
											   + minimumSpanningTreeWithBottleneckAirportReducedByFactorPair._2()._1() + ") = " 
											   + minimumSpanningTreeWithBottleneckAirportReducedByFactorPair._2()._2());
		
		//minimumSpanningTreeWithBottleneckAirportReducedByFactorJavaPairRDD.checkpoint();
		//minimumSpanningTreeWithBottleneckAirportReducedByFactorJavaPairRDD.unpersist();
		
		System.out.println();
		System.out.println();
		
		long endExecutionTime = System.currentTimeMillis();
		
		long elapsedExecutionTime = endExecutionTime - startExecutionTime;
		
		String elapsedExecutionTimeInHoursMinutesAndSeconds = 
				String.format("%02dh:%02dm:%02ds",
							  TimeUnit.MILLISECONDS.toHours(elapsedExecutionTime),
							  TimeUnit.MILLISECONDS.toMinutes(elapsedExecutionTime) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(elapsedExecutionTime)),
	            			  TimeUnit.MILLISECONDS.toSeconds(elapsedExecutionTime) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(elapsedExecutionTime)));
	    
		System.out.println("It took " + elapsedExecutionTimeInHoursMinutesAndSeconds + " to process and execute the whole Algorithm with a Dataset with " + numAllAirports + " Airports!");
		
		// Terminate the Spark's Context and Session
		sparkContext.stop();
		sparkSession.stop();
		
		System.out.println();
		System.out.println();
		
		System.out.println("Terminating Spark's Session...");
	}
}