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
import org.apache.spark.ml.clustering.KMeans


object Assignment4b {

case class Person(features:Vector)

def mapper(line:String): Person = {
	val fields = line.split(',') 

			val person:Person = Person(Vectors.dense(fields(0).toDouble, fields(1).toDouble, fields(2).toDouble,fields(3).toDouble, fields(4).toDouble))
			return person
}


def main(args: Array[String]) {


	Logger.getLogger("org").setLevel(Level.ERROR)

	// Create a SparkContext using every core of the local machine
	val sc = new SparkContext("local[*]", "Assignment4a-Classficiation");
	val data = sc.textFile("../AssignmentDataSets/Question2/Data_User_Modeling_Dataset_Hamd.csv");

	val spark = SparkSession
			.builder
			.appName("SparkSQL")
			.master("local[*]")
			.config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
			.getOrCreate()

			import spark.implicits._
			val dataset=data.map(mapper).toDF();

	    val kmeans = new KMeans().setK(4).setSeed(1L)
			val model = kmeans.fit(dataset)

			// Evaluate clustering by computing Within Set Sum of Squared Errors.
			val WSSSE = model.computeCost(dataset)
			println("Within Set Sum of Squared Errors ="+WSSSE)

			println("Cluster Centers: ")
			model.clusterCenters.foreach(println)

}

}