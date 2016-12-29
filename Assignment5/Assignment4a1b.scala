package naveen.bigdata.assignment


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.mllib.tree.configuration.Algo.Classification
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.feature.{StringIndexer, IndexToString, VectorIndexer}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.ml.feature.PCA

object Assignment4a1b {
  
  case class Person(classify:Int, feat:org.apache.spark.ml.linalg.Vector)
  
  def mapper(line:String): Person = {
    val fields = line.split(',') ;
    
    val person:Person = Person(fields(9).toInt,org.apache.spark.ml.linalg.Vectors.dense(fields(1).toDouble, fields(2).toDouble,fields(3).toDouble, fields(4).toDouble,fields(5).toDouble, fields(6).toDouble,fields(7).toDouble, fields(8).toDouble));
    return person
  }
  
    def mapper1(line:String): org.apache.spark.mllib.linalg.Vector = {
    val fields = line.split(',')  
    
    
    return org.apache.spark.mllib.linalg.Vectors.dense(fields(1).toDouble, fields(2).toDouble,fields(3).toDouble, fields(4).toDouble,fields(5).toDouble, fields(6).toDouble,fields(7).toDouble, fields(8).toDouble,fields(9).toInt)
  }
  
     
  def main(args: Array[String]) {
    
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    


     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Assignment4a-Classficiation");
    val data = sc.textFile("../AssignmentDataSets/Question1/cmc.data.txt");
   
        val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
    import spark.implicits._
    val dataframe=data.map(mapper).toDF();
    
    val featureIndexerPCA=new PCA()
                  .setInputCol("feat")
                  .setK(4)
                  .fit(dataframe)
                  .setOutputCol("indexedFeatures")

    val labelIndexer = new StringIndexer()
                       .setInputCol("classify")
                       .setOutputCol("indexedLabel")
                       .fit(dataframe);

   val Array(trainingData, testData) = dataframe.randomSplit(Array(0.7, 0.3));
    
   val dt = new DecisionTreeClassifier()
               .setLabelCol("indexedLabel")
               .setFeaturesCol("indexedFeatures")

// Convert indexed labels back to original labels.
  val labelConverter = new IndexToString()
               .setInputCol("prediction")
               .setOutputCol("predictedLabel")
               .setLabels(labelIndexer.labels)
  
  
      val pipeline = new Pipeline()
               .setStages(Array(labelIndexer, featureIndexerPCA, dt, labelConverter));
   
   
       val model = pipeline.fit(trainingData)

       val predictions = model.transform(testData)
       
       val evaluator = new MulticlassClassificationEvaluator()
                          .setLabelCol("indexedLabel")
                          .setPredictionCol("prediction")
                          .setMetricName("accuracy")
       val accuracy = evaluator.evaluate(predictions)
       println("Accuracy = " + (accuracy))
       
       val result=predictions.rdd.map(x=> (x(0).toString().toDouble,x(7).toString().toDouble));
       
       val metrics = new BinaryClassificationMetrics(result);

// Precision by threshold
    val precision = metrics.precisionByThreshold
                  precision.foreach { case (t, p) =>
                  println(s"Threshold: $t, Precision: $p")
    }
    
    val recall = metrics.recallByThreshold
                 recall.foreach { case (t, r) =>
                 println(s"Threshold: $t, Recall: $r")
    }
    
    val PRC = metrics.pr

// F-measure
   val f1Score = metrics.fMeasureByThreshold
                 f1Score.foreach { case (t, f) =>
                 println(s"Threshold: $t, F-score: $f, Beta = 1")
                }
     
    
  }
 
}