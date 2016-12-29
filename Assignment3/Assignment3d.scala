package naveen.bigdata.assignment

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object Assignment3d {
  
  def main(args: Array[String]) { 

     Logger.getLogger("org").setLevel(Level.ERROR);
     
     val sc=new SparkContext("local[*]","Assignment3a");
     
     val user=sc.textFile("../user.csv");
     
     val review=sc.textFile("../review.csv");
     
     val review1=review.map(line => line.toString().split('^')).map(line => (line(1).toString(),1)).reduceByKey(_+_)
     
     val user1=user.map(line => line.toString().split('^')).map(line => (line(0).toString(),line(1).toString()))
     
     val coimbned=review1.join(user1).map(line=> ( line._1,(line._2._1,line._2._2)));
     
     val result=coimbned.collect()
     
     val sorted = result.sortWith(_._2._1> _._2._1).take(10);
     
     val finalResult=sorted.map(line => ( line._1,line._2._2));
    
     finalResult.take(10).foreach(println);
     
  }
  
}