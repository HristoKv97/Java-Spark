package classTutorial;

import java.io.FileWriter;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;


import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.variance;
public  class b6035937  {
	
	
	private static String PATH = ("/H:/workspace/Spark-SQL-examples/ml-latest");

	static SparkConf conf = new SparkConf().setMaster("local").setAppName("My App"); //$NON-NLS-1$ //$NON-NLS-2$
	static JavaSparkContext sc = new JavaSparkContext(conf);
	static SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example") //$NON-NLS-1$
			.config("spark.some.config.option", "some-value").getOrCreate(); //$NON-NLS-1$ //$NON-NLS-2$
	static SQLContext sqlContext;
	
	static Dataset<Row> movies;
	static Dataset<Row> movieGenres;
	static Dataset<Row> movieGenres1;
	static Dataset<Row> ratings;
	static Dataset<Row> movies1;
	static Dataset<Row> genrePopularity;
	static Dataset<Row> exploded2;
	
	 static void step1(){
		 
		 //Printing out the schema for ratings
		System.out.println("-------------------Task 1-------------------");
		 ratings = spark.read()
				.option("inferSchema", true)
				.option("header", true).option("multLine", true)
				.option("mode", "DROPMALFORMED")
				.csv(PATH + "/ratings.csv");
				ratings.printSchema();
				
				 //Printing out the schema for movies
		movies = spark.read()
				.option("inferSchema", true)
				.option("header", true).option("multLine", true)
				.option("mode", "DROPMALFORMED")
				.csv(PATH + "/movies.csv");
				movies.printSchema();
		}
	
	 	//// Makes list for every film with its genres then expands it and writes it in a file
	 static void step2(){
		System.out.println("-------------------Task 2-------------------");
		
		movies1 = movies.selectExpr("movieId","split(genres, '\\\\|') as result");	
		Dataset<Row> expanded  = movies1.withColumn("genres", explode(movies1.col("result"))).drop("result");

			try {
					FileWriter fw=new FileWriter("/H:/workspace/Spark-SQL-examples/ml-latest/movieGenres.csv");  
							fw.write("movieId,genres"+"\n");
								for(Row r:expanded.collectAsList()){
										fw.write(r.getAs("movieId")+","+r.getAs("genres")+"\n");
									}
									fw.close();    
					} 			catch(Exception e){  
	
				}
}
	// Prints the table in descending order
	public static void step3(){
		System.out.println("-------------------Task 3-------------------");

				movieGenres= spark.read()
				.option("inferSchema", true)
				.option("header", true).option("multLine", true)
				.option("mode", "DROPMALFORMED")
				.csv(PATH + "/movieGenres.csv");
						movieGenres.printSchema();
						movieGenres.select("movieId", "genres").orderBy(col("movieId").desc()).show(50, false);;
}
	
	
	//Counts the most films with one genre
	public static void step4(){
		System.out.println("-------------------Task 4-------------------");

			 genrePopularity = movieGenres;
				genrePopularity.select("genres").groupBy("genres").count().orderBy(col("count").desc()).show(10, false);
		}
	
	//The user with most votesd for each genre
	public static void step5(){
		System.out.println("-------------------Task 5-------------------");
				Dataset<Row> joined = ratings.join(movies1, ratings.col("movieId").equalTo(movies1.col("movieId"))).drop("movieId", "timestamp", "rating");



		 exploded2 = joined.withColumn(
		  "result", explode(movies1.col("result")));


		Dataset<Row> group = exploded2.groupBy("UserId", "result").count().orderBy(col("count").desc()).dropDuplicates("result");



		Dataset<Row> popularity = genrePopularity.select("genres").groupBy("genres").count().orderBy(col("count").desc()).limit(10);


		Dataset<Row> finalTable = group.orderBy((col("count").desc()));





Dataset<Row> newestTest1 = popularity.join(finalTable, finalTable.col("result").equalTo(popularity.col("genres")))
.orderBy(finalTable.col("count").desc())
.drop(popularity.col("count")).drop(finalTable.col("result")).drop(finalTable.col("count")).limit(10);
newestTest1.show(false);
}
	
	public static void step6(){
		System.out.println("Step 6.1:\n");

		Dataset<Row> ratingsCounts = ratings.groupBy(col("userId")).count().orderBy(col("count").desc());
	
		System.out.println("Step 6.2:\n");
		
		Dataset<Row> genresUsers = exploded2.groupBy("userId", "result").count().orderBy(col("count").desc()).dropDuplicates("userId").orderBy(col("count").desc());

		
		System.out.println("Step 6.3.1:\n");

		Dataset<Row> itsWorking = ratingsCounts.join(genresUsers, genresUsers.col("userId")
				.equalTo(ratingsCounts.col("userId"))).orderBy(ratingsCounts.col("count")
						.desc()).drop(genresUsers.col("userId")).drop(genresUsers.col("count")).limit(10);
		
 		
		Dataset<Row> itsWorking2 = itsWorking.withColumnRenamed("count", "ratingsCount").withColumnRenamed("result", "mostCommonGenres");

		itsWorking2.show(false);
		System.out.println("Step 6.4:\n");

		Dataset<Row> newestWorking = itsWorking2.select("userId","ratingsCount","mostCommonGenres");
		newestWorking.show(10,false);
	}

	public static void step7(){
		
		System.out.println("-------------------Task 7-------------------");
		Dataset<Row> avgMovieRating = movies.join(ratings, "movieId")
                .groupBy(col("title"),col("movieId")).avg("rating");
        avgMovieRating.printSchema();
        avgMovieRating.show();        
        Dataset<Row> varMovieRating = movies.join(ratings, "movieId")
                .groupBy(col("title"),col("movieId")).agg(variance(col("rating")));
        varMovieRating.printSchema();
        varMovieRating.show();       
        Dataset<Row> avgVarCombined = avgMovieRating.join(varMovieRating, varMovieRating.col("movieId").equalTo(avgMovieRating.col("movieId")),"right_outer");
        avgVarCombined.printSchema();
        avgVarCombined.select(avgMovieRating.col("movieId"),avgMovieRating.col("title"), avgMovieRating.col("avg(rating)"), varMovieRating.col("var_samp(rating)")).orderBy(col("avg(rating)").desc()).show(10);


	}
	
	public static void main(String[] args) {
		LogManager.getLogger("org").setLevel(Level.ERROR);
		
		step1();
		step2();
		step3();
		step4();
		step5();
		step6();
		step7();
	
	
		
		

}
}