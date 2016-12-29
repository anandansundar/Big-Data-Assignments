package naveen.bigdata.assignment

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object Assignment3e {
  
    def main(args: Array[String]) { 
      
       
     Logger.getLogger("org").setLevel(Level.ERROR);
     
     val sc=new SparkContext("local[*]","Assignment3a");
     
     val business=sc.textFile("../business.csv");
     
     val review=sc.textFile("../review.csv");
     
     val business1=business.map(line => line.toString().split('^')).map(line => (line(0).toString(),line(1))).filter(line => line._2.contains("TX"));
      
     val review1=review.map(line => line.toString().split('^')).map(line => (line(2).toString(),1)).reduceByKey(_+_)
     
     val coimbained=business1.join(review1).map(line => (line._1,line._2._2))
     
     coimbained.foreach(println);
      
    }
  
}