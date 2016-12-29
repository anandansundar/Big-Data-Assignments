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
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.sql.catalyst.expressions.ToDate
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.mllib.evaluation.RegressionMetrics

object Assignment4c1 {
  
  case class Person(label:Double, features:Vector)
  
  def mapper(line:String): Person = {
    val fields = line.split(',') 
    
    
    val person:Person = Person(fields(8).toDouble, Vectors.dense(fields(0).toDouble, fields(1).toDouble, fields(2).toDouble,fields(3).toDouble, fields(4).toDouble, fields(5).toDouble, fields(6).toDouble, fields(7).toDouble))
    return person
  }
  
     
  def main(args: Array[String]) {
    
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Assignment4a-Classficiation");
    val data = sc.textFile("../AssignmentDataSets/Question3/Concrete_Data.csv");
   
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
    import spark.implicits._
    val dataset=data.map(mapper).toDF();
  
    val Array(trainingData, testData) = dataset.randomSplit(Array(0.7, 0.3));
     val glr = new GeneralizedLinearRegression()
  .setFamily("gaussian")
  .setLink("identity")
  .setMaxIter(10)
  .setRegParam(0.3)

// Fit the model
val model = glr.fit(trainingData)

val predictions=model.transform(testData)
// Print the coefficients and intercept for generalized linear regression model
println(s"Coefficients: ${model.coefficients}")
println(s"Intercept: ${model.intercept}")

println(model.evaluate(dataset))

// Summarize the model over the training set and print out some metrics
val summary = model.summary
println(s"Coefficient Standard Errors: ${summary.coefficientStandardErrors.mkString(",")}")
println(s"T Values: ${summary.tValues.mkString(",")}")
println(s"P Values: ${summary.pValues.mkString(",")}")
println(s"Dispersion: ${summary.dispersion}")
println(s"Null Deviance: ${summary.nullDeviance}")
println(s"Residual Degree Of Freedom Null: ${summary.residualDegreeOfFreedomNull}")
println(s"Deviance: ${summary.deviance}")
println(s"Residual Degree Of Freedom: ${summary.residualDegreeOfFreedom}")
println(s"AIC: ${summary.aic}")
println("Deviance Residuals: ")
summary.residuals().show()

val eval=new RegressionEvaluator();

val rmse=eval.evaluate(predictions)

val result=predictions.rdd.map(x=> (x(0).toString().toDouble,x(2).toString().toDouble));
val metrics = new RegressionMetrics(result);  



println("Root Mean Squre error "+metrics.rootMeanSquaredError);   
println("R2 "+metrics.r2);
println("Explaind Variance "+metrics.explainedVariance);
  }
 
}