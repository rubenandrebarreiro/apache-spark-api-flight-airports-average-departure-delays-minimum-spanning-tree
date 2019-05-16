package flight_airports_minimum_spanning_tree;

import org.apache.spark.api.java.function.Function;

/**
 * High Performance Computing
 * 
 * Practical Lab #3.
 * 
 * Integrated Master of Computer Science and Engineering
 * Faculty of Science and Technology of New University of Lisbon
 * 
 * Authors (Professors):
 * @author Herve Miguel Paulino - herve.paulino@fct.unl.pt
 * 
 * Adapted by:
 * @author Ruben Andre Barreiro - r.barreiro@campus.fct.unl.pt
 *
 */

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import scala.Tuple2;

public class FlightAnalyser {

	private static final String DefaulftFile = "data/flights.csv";
	
	//IMPLEMENTATION STEPS
	
	// 1. Build an undirected graph to represent the average delay of the flights between any two airports (the average must
	// consider all flights between the both airports, disregarding the origin and the destination). The graph’s nodes are
	// thus the airports, and the edges represent direct routes between airports, labeled with the average delays of the
	// routes’ flights.
	
	// Suggestion: represent the graph through an adjacency matrix.
	// See https://spark.apache.org/docs/latest/mllib-data-types.html#distributed-matrix
	
	// You will have to add the following dependency to file build.gradle:
	// - implementation 'org.apache.spark:spark-mllib_2.11:2.3.0'
	
	// 2. Compute the graph’s Minimum Spanning Tree (MST) by implementing the parallel version of Prim’s Algorithm
	// (available from CLIP). The MST will be the subgraph of the original with the minimum total edge weight (sum of the
	// average delays). Output the MST and its total edge weight.
	
	// 3. Identify the bottleneck airport, i.e. the airport with higher aggregated delay time (sum of the delays of all routes
	// going out of the airport) from the ones contained in the complement graph of the MST previously computed.
	
	// 4. Modify the graph to reduce by a given factor the delay time of all routes going out of the selected airport. This factor
	// will be a parameter of your algorithm (received in the command line) and must be a value in ]0, 1[.
	
	// 5. Recompute the MST and display the changes perceived in the resulting subgraph and on the sum of the total edge
	// weight
	
	/*
	createStructField("day_of_month", StringType, false),
    createStructField("day_of_week", StringType, false),
    createStructField("carrier", StringType, false),
    createStructField("tail_num", StringType, false),
    createStructField("flight_num", IntegerType, false),
    createStructField("origin_id", LongType, false),
    createStructField("origin_name", StringType, false),
    createStructField("destination_id", LongType, false),
    createStructField("destination_name", StringType, false),
    createStructField("scheduled_departure_time", DoubleType, false),
    createStructField("departure_real_time", DoubleType, false),
    createStructField("departure_delay", DoubleType, false),
    createStructField("scheduled_arrival_time", DoubleType, false),
    createStructField("arrival_real_time", DoubleType, false),
    createStructField("arrival_delay", DoubleType, false),
    createStructField("elapsed_time", DoubleType, false),
    createStructField("distance", IntegerType, false),	  
	*/
	
	/**
	 * Returns the average of arrival delays of flights for each route, ordered by descending of Average field.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the average of arrival delays of flights for each route, ordered by descending of Average field
	 */
	public static Dataset<Row> getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset(Dataset<Row> flightsDataset) {
		Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset = 
					 flightsDataset.groupBy("origin_id", "destination_id")
					 			   .avg("departure_delay")
					 			   .orderBy("origin_id");
		
		return allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset;
	}
	
	@SuppressWarnings("deprecation")
	public static Dataset<Row> 
			getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset(SQLContext sqlContext, Dataset<Row> flightsDataset) {
		
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
	
	public static long numMaxBetweenOriginIDsAndDestinationIDsCount(Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset) {
		
		Dataset<Row> originIDsCount = allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset.groupBy("origin_id").count().orderBy("origin_id");
		Dataset<Row> destinationIDsCount = allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset.groupBy("destination_id").count().orderBy("destination_id");
		
		long numTotalOrigins = originIDsCount.sort(functions.desc("count")).first().getLong(1);
		long numTotalDestinations = destinationIDsCount.sort(functions.desc("count")).first().getLong(1);
		
		return Long.max(numTotalOrigins, numTotalDestinations);
	}
	
	public static JavaPairRDD<Long, Iterable<Tuple2<Long, Double>>> 
		getAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationGroupByKeyOriginJavaPairRDD(Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset) {
		
		JavaRDD<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationJavaRDD = 
									allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset.orderBy("origin_id").javaRDD();
		
		JavaPairRDD<Long, Tuple2<Long, Double>> averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationJavaPairRDD = 
				allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationJavaRDD.mapToPair(
						new PairFunction<Row, Long, Tuple2 <Long, Double> > () {

							/**
							 * 
							 */
							private static final long serialVersionUID = 1L;
		 
							@Override
							public Tuple2<Long, Tuple2<Long, Double>> call(Row row) throws Exception {
		
								return new Tuple2<Long, Tuple2<Long, Double>>(row.getLong(0), new Tuple2<Long, Double>(row.getLong(1), row.getDouble(2)));
							}
						}
				);
		
		return averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationJavaPairRDD.groupByKey().sortByKey();
	}
	
	public static CoordinateMatrix buildCoordinateAdjacencyMatrix
										(JavaPairRDD<Long, Iterable<Tuple2<Long, Double>>> 
										 averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationGroupByKeyOriginJavaPairRDD, long matrixDimensions) {
		
		JavaRDD<MatrixEntry> matrixEntriesJavaRDD = averageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationGroupByKeyOriginJavaPairRDD.map(
				new Function<Tuple2<Long, Iterable<Tuple2<Long, Double>>>, MatrixEntry>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public MatrixEntry call(Tuple2<Long, Iterable<Tuple2<Long, Double>>> tuple) throws Exception {	
						
						// TODO - como calcular row e col correctos?!
						long row = 0;
						long col = 0;
						
						double matrixEntryValue = tuple._2().iterator().next()._2();
						
						return new MatrixEntry(row, col, matrixEntryValue);
					}
				}
		);
		
		RDD<MatrixEntry> matrixEntriesRDD = matrixEntriesJavaRDD.rdd();
		
		return new CoordinateMatrix(matrixEntriesRDD, matrixDimensions, matrixDimensions);
	}
	
	/**
	 * Returns the average of arrival delays of flights for each route, ordered by descending of Average field.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the average of arrival delays of flights for each route, ordered by descending of Average field
	 */
	@SuppressWarnings("deprecation")
	public static Dataset<Row> arrivalDelaysAverageForEachRouteByDescAvg(Dataset<Row> data) {
		 Dataset<Row> result = data.select("origin", "dest", "arrival_delay")
				 				   .where(data.col("arrival_delay").$bang$eq$eq(0.0));
		 
		 result = result.groupBy("origin", "dest").avg("arrival_delay");
		 
		 result.printSchema();
		 
		 result = result.sort(functions.desc("avg(arrival_delay)"));
		 		 		 
		 return result;
	}
	
	
	/**
	 * Main method to process the flights' file and analyze it.
	 * 
	 * @param args the file of flights to process
	 *        (if no args, uses the file flights.csv, by default)
	 */
	public static void main(String[] args) {
		
		String fileName = (args.length < 1) ? DefaulftFile : args[0];
		
		// start Spark session (SparkContext API may also be used) 
		// master("local") indicates local execution
		SparkSession sparkSession = SparkSession.builder().
				appName("FlightAnalyser").
				master("local[*]").
				getOrCreate();
		
		// TODO
		SparkContext sparkContext = sparkSession.sparkContext();
		
		// TODO
		JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
		
		// TODO
		@SuppressWarnings("deprecation")
		SQLContext sqlContext = new SQLContext(sparkContext);
		
		// only error messages are logged from this point onward
		// comment (or change configuration) if you want the entire log
		sparkContext.setLogLevel("ERROR");

		
		Dataset<String> flightsTextFile = sparkSession.read().textFile(fileName).as(Encoders.STRING());	
		
		Dataset<Row> flightsInfo = 
				flightsTextFile.map((MapFunction<String, Row>) l -> Flight.parseFlight(l), 
				Flight.encoder()).cache();
		
		// All the information about the flights,
		// organised by the corresponding fields
		for(Row r : flightsInfo.collectAsList())
			System.out.println(r);
		
		System.out.println();
		System.out.println();
		
		Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset = getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset(flightsInfo);
		
		System.out.println("The Number of Rows/Entries in the Dataset of All Average Delays Of The Flights Between Any Two Airports:");
		System.out.println("- " + allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset.count());
		
		System.out.println();
		
		for(Row r : allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDataset.collectAsList())
			System.out.println(r);
		
		System.out.println();
		System.out.println();
		
		Dataset<Row> allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset = 
									getAllAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset(sqlContext, flightsInfo);
		
		System.out.println("The Number of Rows/Entries in the Dataset of All Average Delays Of The Flights Between Any Two Airports, Disregarding Origin and Destination:");
		System.out.println("- " + allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset.count());
		
		System.out.println();
		
		for(Row r : allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset.collectAsList())
			System.out.println(r);
		
		System.out.println();
		System.out.println();
		
		JavaPairRDD<Long, Iterable<Tuple2<Long, Double>>> a = getAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationGroupByKeyOriginJavaPairRDD(allAverageDelaysOfTheFlightsBetweenAnyTwoAirportsDisregardingOriginAndDestinationDataset);
		
		for (Entry<Long, Iterable<Tuple2<Long, Double>>> entry : a.collectAsMap().entrySet()) {
		    System.out.println("Average Delays Of The Flights From Origin ID: " + entry.getKey());
		    
		    Iterable<Tuple2<Long, Double>> c = entry.getValue();
		    
		    Iterator<Tuple2<Long, Double>> d = c.iterator();
		    
		    Tuple2<Long, Double> e;
		    
			while(d.hasNext()) {
		    	e = d.next();
		    	
		    	System.out.println("- (" + e._1() + ", " + e._2 + ");");
			}
			
			System.out.println();
			System.out.println();
		}
		
		// Terminate the Spark Session
		sparkSession.stop();
	}
}

//class TupleComparator implements Comparator<Entry<Long, Iterable<Tuple2<Long, Double>>>>, Serializable {
	/**
	 * 
	 */
/*	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Entry<Long, Iterable<Tuple2<Long, Double>>> o1, Entry<Long, Iterable<Tuple2<Long, Double>>> o2) {
		if (o1.getKey() == o2.getKey()) {
			Iterable<Tuple2<Long, Double>> a = o1.getValue();
			Iterable<Tuple2<Long, Double>> b = o2.getValue();
		}
		
		return (int) (o1.getKey() - o2.getKey());
	}
}
*/