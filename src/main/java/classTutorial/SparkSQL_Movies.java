package classTutorial;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class SparkSQL_Movies {

	private static String PATH = Messages.getString("LearningSparkIntroExamples.0"); //$NON-NLS-1$

	private static String MOVIES_METADATA = "movies_metadata.csv";
	private static String RATINGS         = "ratings_small.csv";
	
	static SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
	static JavaSparkContext sc = new JavaSparkContext(conf);
	static SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
			.config("spark.some.config.option", "some-value").getOrCreate();
	    		
	
	public static void main(String[] args) {

		LogManager.getLogger("org").setLevel(Level.ERROR);

		Dataset<Row> mmDF = LoadMoviesMetadata();
		mmDF.printSchema();

		/**
		 * 1. produce a DF that only contains the movie title and budget,
		 * display the first 10 rows
		 */
		mmDF.select("id", "title", "budget").show(50);

		/**
		 * 3. show the 10 most expensive movies under 5000000
		 */
		mmDF.filter(col("budget").$less(5000000)).orderBy(col("budget").desc()).select("title", "budget").show();

		/*
		 * however this is not correct because "budget" is a string. First we
		 * need to convert it to a Long:
		 */
		mmDF = mmDF.withColumn("tmp", mmDF.col("budget").cast(DataTypes.LongType)).drop("budget").withColumnRenamed("tmp", "budget");
		
		mmDF.filter(col("budget").$less(5000000)).orderBy(col("budget").desc()).select("title", "budget").show();
		
		// selection using filter
		// 15602
		String movieId = "15602";		
		System.out.println("movies with id "+movieId +": "+mmDF.filter(col("id").equalTo(movieId)).count());
		
		// same selection using SQL
		mmDF.createOrReplaceTempView("movies_metadata");

		String sql1 = "SELECT * FROM movies_metadata WHERE id ='"+movieId+"'";
		System.out.println("Where query: \n"+sql1);

		Dataset<Row> sqlResult = spark.sql(sql1);		
		System.out.println("Where query returns "+sqlResult.count()+" rows");
		
		/**
		 * JOINS
		 * load a ratings file and then join the ratings with the movie_metadata
		 * ratings_small is also a csv file
		 */
		Dataset<Row> ratingsDF = LoadRatingsMetadata();
		ratingsDF.printSchema();
		System.out.println("ratings file contains "+ratingsDF.count()+" rows");

		// try and join mmDF with ratingsDF on the movie ID
		Dataset<Row> joinedDF = ratingsDF.join(mmDF, mmDF.col("id").equalTo(ratingsDF.col("movieId")));
		System.out.println("count of ratingsDF join mmDF: "+joinedDF.count());
		
		// achieve the same join using SQL:
		ratingsDF.createOrReplaceTempView("ratings");
		
		String sql2 = "SELECT * FROM ratings r JOIN movies_metadata mm on r.movieid = mm.id";
		System.out.println("Join query: \n"+sql2);
		
		joinedDF = spark.sql(sql2);
		System.out.println("count of ratingsDF join mmDF / SQL: "+joinedDF.count());

		// now let us try an outer join:
		Dataset<Row> leftOuterJoinedDF = ratingsDF.join(mmDF, mmDF.col("id").equalTo(ratingsDF.col("movieId")), "left_outer");
		System.out.println("count of ratingsDF left outer join mmDF: "+leftOuterJoinedDF.count());

		Dataset<Row> rightOuterJoinedDF = ratingsDF.join(mmDF, mmDF.col("id").equalTo(ratingsDF.col("movieId")), "right_outer");
		System.out.println("count of ratingsDF right outer join mmDF: "+rightOuterJoinedDF.count());
		
/**
 * more aggregations		
 */
		
		// count the number of ratings for each movie 
		Dataset<Row> ratingsCounts = ratingsDF.groupBy(col("movieId")).count().orderBy(col("count").desc());
		
		// rename column count to "ratingsCount"
		Dataset<Row> ratingsCounts2 = ratingsCounts.withColumnRenamed("count", "ratingsCount");
		
		ratingsCounts2.show(10);
		
		// now print the count along with the movie title. note this requires a join with the movies_metadata DF:
		ratingsCounts2.join(
				mmDF, 
				mmDF.col("id").equalTo(ratingsCounts2.col("movieId"))).
				select(col("title"), col("ratingsCount")).orderBy(col("ratingsCount").desc()).show();
		
		
		spark.stop();
	}


	
	private static Dataset<Row> LoadMoviesMetadata() {
		return spark.read().option("inferSchema", true).option("header", true).option("multLine", true)
				.option("mode", "DROPMALFORMED").csv(PATH + MOVIES_METADATA);
	}

	private static Dataset<Row> LoadRatingsMetadata() {
		return spark.read().option("inferSchema", true).option("header", true).option("multLine", true)
				.option("mode", "DROPMALFORMED").csv(PATH + RATINGS);
	}

}
