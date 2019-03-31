package classTutorial;

import static spark.Spark.*;

import java.util.Arrays;
import org.apache.spark.api.java.function.Function;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;


public class LearningSparkIntroExamples {

	private static String PATH = Messages.getString("LearningSparkIntroExamples.0"); //$NON-NLS-1$

	static SparkConf conf = new SparkConf().setMaster("local").setAppName("My App"); //$NON-NLS-1$ //$NON-NLS-2$
	static JavaSparkContext sc = new JavaSparkContext(conf);
	static SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example") //$NON-NLS-1$
			.config("spark.some.config.option", "some-value").getOrCreate(); //$NON-NLS-1$ //$NON-NLS-2$

	public static void main(String[] args) {

		LogManager.getLogger("org").setLevel(Level.ERROR); //$NON-NLS-1$

		// create a new RDD from a list of objects
		JavaRDD<String> myData = sc.parallelize(Arrays.asList("pandas", "i like pandas")); //$NON-NLS-1$ //$NON-NLS-2$

		// Example (3.1): create a new RDD by loading a text file using the SparkContext sc
		JavaRDD<String> lines = sc.textFile(PATH+"/Dante-Inferno.txt");  //$NON-NLS-1$
		System.out.println("total lines count: "+lines.count()); //$NON-NLS-1$
		System.out.println(lines.take(10));

/**
 * RDD transformation: filtering
 */
		System.out.println("filtering an RDD produces a new RDD. "); //$NON-NLS-1$
		System.out.println("this requires passing a function as argument to the filter() method"); //$NON-NLS-1$
		/**
		 * In Java, functions are specified as objects that implement one of Spark’s function interfaces 
		 * from the org.apache.spark.api.java.function package. 
		 */

		// option 1: define an inline function class as anonymous inner classes
		JavaRDD<String> infernoLines = lines.filter(
				new Function<String, Boolean>() {
					public Boolean call(String x) { return x.contains("inferno"); } //$NON-NLS-1$
				});

		// option 2: create a named class
		class ContainsInferno implements Function<String, Boolean> {
			public Boolean call(String x) { return x.contains("inferno"); } //$NON-NLS-1$
		}

		infernoLines = lines.filter(new ContainsInferno());
				
		// option 3: lambda functions (Java 8 only)
		infernoLines = lines.filter(s -> s.contains("inferno")); //$NON-NLS-1$
		
		System.out.println("inferno lines count: "+infernoLines.count()); //$NON-NLS-1$
		System.out.println(infernoLines.take(10));
		
		
		// the MAP() operator applies a function to each element of a list
		JavaRDD<String> uppercaseLines = infernoLines.map(s -> s.toUpperCase());
		System.out.println("first 10 uppercase inferno lines: "+uppercaseLines.take(10)); //$NON-NLS-1$

		// we may chain together transformations that produce RDDs, creating "pipelines"
		// lines.filter(lambda x: "inferno" in x).map(lambda s: s.upper()).take(5)
		JavaRDD<String> infernoUppercaseLines = lines.filter(s -> s.contains("inferno")).map(s -> s.toUpperCase()); //$NON-NLS-1$
		
		System.out.println("first 10 uppercase inferno lines from pipeline: \n"+ //$NON-NLS-1$
				infernoUppercaseLines.take(10));
		
		// set operations
		JavaRDD<String> amoreLowerCaseLines = lines.filter(s -> s.contains("amore")).map(s -> s.toLowerCase()); //$NON-NLS-1$

		JavaRDD<String> unionRDD = infernoUppercaseLines.union(amoreLowerCaseLines);
		System.out.println("counts:" //$NON-NLS-1$
				+ "\ninferno lines count: "+infernoUppercaseLines.count() //$NON-NLS-1$
				+ "\namore lines count: "+amoreLowerCaseLines.count() //$NON-NLS-1$
				+ "\nunion count: "+unionRDD.count()); //$NON-NLS-1$
		
				
		JavaRDD<String> intersectRDD = infernoUppercaseLines.intersection(amoreLowerCaseLines);
		System.out.println("counts:" //$NON-NLS-1$
				+ "\ninferno lines count: "+infernoUppercaseLines.count() //$NON-NLS-1$
				+ "\namore lines count: "+amoreLowerCaseLines.count() //$NON-NLS-1$
				+ "\nintersect count: "+intersectRDD.count()); //$NON-NLS-1$
		
	}

}
