package naveen.bigdata.assignment


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.evaluation.RegressionMetrics


object Assignment4d {

case class Person(userId: Int, movieId: Int, rating: Float, timestamp: Long)

def mapper(line:String): Person = {
	val fields = line.split(',') 

			val person:Person = Person(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
			return person
}


def main(args: Array[String]) {


	Logger.getLogger("org").setLevel(Level.ERROR)

	// Create a SparkContext using every core of the local machine
	val sc = new SparkContext("local[*]", "Assignment4a-Classficiation");
	val data = sc.textFile("../AssignmentDataSets/Question4/ratings.csv");

	val spark = SparkSession
			.builder
			.appName("SparkSQL")
			.master("local[*]")
			.config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
			.getOrCreate()

			import spark.implicits._
			val dataset=data.filter(x=> x!="userId,movieId,rating,timestamp").map(mapper).filter(x=> x.userId<50).toDF();

	    val Array(training, test) = dataset.randomSplit(Array(0.8, 0.2))

// Build the recommendation model using ALS on the training data
val als = new ALS()
  .setMaxIter(5)
  .setRegParam(0.01)
  .setUserCol("userId")
  .setItemCol("movieId")
  .setRatingCol("rating")

val model = als.fit(training)

// Evaluate the model by computing the RMSE on the test data
val predictions = model.transform(test)

val evaluator = new RegressionEvaluator()
  .setMetricName("rmse")
  .setLabelCol("rating")
  .setPredictionCol("prediction")
val rmse = evaluator.evaluate(predictions)


val result=predictions.rdd.map(x=> (x(0).toString().toDouble,x(2).toString().toDouble));
val metrics = new RegressionMetrics(result);  



println("Root Mean Squre error "+metrics.rootMeanSquaredError);   
/*println("R2 "+metrics.r2);
println("Explaind Variance "+metrics.rootMeanSquaredError);
*/
}

}