package classTutorial;

//import static spark.Spark.*;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import static org.apache.spark.sql.functions.avg;
import org.apache.spark.sql.Encoders;


public class SparkDistributionSQLExamples {

	private static String PATH = Messages.getString("LearningSparkIntroExamples.0"); //$NON-NLS-1$
	private static String peopleFile = "people.json";

	static SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
	static JavaSparkContext sc = new JavaSparkContext(conf);
	static SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
			.config("spark.some.config.option", "some-value").getOrCreate();

	public static void main(String[] args) {

		LogManager.getLogger("org").setLevel(Level.ERROR);

		Dataset<Row> df = RunCreatingDataframes();

		RunDataFrameOperations(df);
		
		RunProgrammaticSQLQueries(df);
		
		spark.stop();
	}

	private static void RunProgrammaticSQLQueries(Dataset<Row> df) {
		// 
		// Register the DataFrame as a SQL temporary view
		df.createOrReplaceTempView("people");

		Dataset<Row> allPeopleDF = spark.sql("SELECT * FROM people");
		allPeopleDF.show();
		
	    // SQL statements can be run by using the sql methods provided by spark
	    Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
	    teenagersDF.show();
	    
	    // The columns of a row in the result can be accessed by field index
	    Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
	        (MapFunction<Row, String>) row -> "Name: " + row.getString(0), Encoders.STRING());
	    
	    System.out.println("teenagerNamesByIndexDF:");
	    teenagerNamesByIndexDF.show();
	    
	    Dataset<Long> namesDS = allPeopleDF.map(
	            (MapFunction<Row, Long>) row -> (row.get(0) != null) ? row.getLong(0) :0, Encoders.LONG());
	        namesDS.show();
	        
	    // average age
	        allPeopleDF.groupBy().agg(avg("age")).show();

	}

	
	private static void RunDataFrameOperations(Dataset<Row> df) {
		/**
		 * DataFrame Operations
		 */

		df.select("name").show();

		// Select everybody, but increment the age by 1
		df.select(col("name"), col("age").plus(1)).show();

		// Select people older than 21
		df.filter(col("age").gt(21)).show();

		// Count people by age
		df.groupBy("age").count().show();
	}

	/***
	 * Creating DataFrames
	 */
	private static Dataset<Row> RunCreatingDataframes() {
		Dataset<Row> df = spark.read().json(PATH + peopleFile);

		df.show();
		df.printSchema();

		return df;
	}
}
