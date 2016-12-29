package naveen.bigdata.assignment

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.ml.{Pipeline,PipelineModel}
import org.apache.spark.ml.feature.{HashingTF,Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark

object Assignment3a {
   
   def main(args: Array[String]) { 

     Logger.getLogger("org").setLevel(Level.ERROR);
     
     
     val sc=new SparkContext("local[*]","Assignment3a");
     
     val business=sc.textFile("../business.csv");
     
     val review=sc.textFile("../review.csv");
     
     val business1=business.map { x => x.toString().split('^') };
     val review1=review.map { x => x.toString().split('^') };
     
     val businessData=business1.map ( line => ( line(0).toString(),line(1))).distinct   
      val businessData1=business1.map ( line => ( line(0).toString(),line(2))).distinct   
     val reviewData=review1.map ( line => ( line(0).toString(), line(1),line(2).toString(),line(3))).distinct    
     
     val reviewDataCount=review1.map(line => (line(2).toString(), 1 )).reduceByKey(_+_);
     
     val reviewDataCount1=review1.map(line => (line(2).toString(), line(3).toDouble )).reduceByKey(_+_);
     
     val sumAndCOunt=reviewDataCount.join(reviewDataCount1);
     
     val result=sumAndCOunt.map(a=> ( a._1.toString(),a._2._1/a._2._2));
     
     val finalresult=businessData.join(businessData1);
     
     val finalresult1=finalresult.join(result);
     
     val list=finalresult1.collect();
     
     val sortedList=list.sortWith(_._2._2 > _._2._2);
     
     sortedList.take(10).foreach(println);
     
     
 
  }
}