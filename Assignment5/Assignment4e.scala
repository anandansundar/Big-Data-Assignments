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
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD


object Assignment4e {


def main(args: Array[String]) {


	Logger.getLogger("org").setLevel(Level.ERROR)

	// Create a SparkContext using every core of the local machine
	val sc = new SparkContext("local[*]", "Assignment4a-Classficiation");
	val data = sc.textFile("../AssignmentDataSets/Question5/kosarak.dat.txt");
 
	val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))

val fpg = new FPGrowth()
  .setMinSupport(0.2)
  .setNumPartitions(100)

val model = fpg.run(transactions.map(_.toSet.toArray))

model.freqItemsets.collect().foreach { itemset =>
  println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
}

val minConfidence = .5
model.generateAssociationRules(minConfidence).collect().foreach { rule =>
  println(
    rule.antecedent.mkString("[", ",", "]")
      + " => " + rule.consequent .mkString("[", ",", "]")
      + ", " + rule.confidence)
}

}

}