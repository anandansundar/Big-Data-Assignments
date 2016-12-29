package naveen.bigdata.assignment

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object Assignment3b {
  
  
   def main(args: Array[String]) { 

     Logger.getLogger("org").setLevel(Level.ERROR);
     
     val sc=new SparkContext("local[*]","Assignment3b");
      
     val review=sc.textFile("../review.csv");
     
     val user= sc.textFile("../user.csv");
     
     val review1=review.map { x => x.toString().split('^') };
     
     val review2=review1.map(line => (line(1),(line(0),line(2),line(3)))).distinct();
     
     val user1=user.map(x => x.toString().split('^'));
     
     val user2=user1.map(line => (line(0),(line(1),line(2)))).distinct();
     
     val combained=user2.join(review2);
     
     val needed=combained.map(line => (line._2._1._1,line._2._2._3.toDouble));
     
     val filtered=needed.filter(line => ( line._1.toString().equals("Lisa B.") ))
     
     val count =filtered.count();
     
     val sum = filtered.reduceByKey(_+_);
     
     val result= sum.map(line => ( line._1,line._2/count))
     
     result.foreach(println);
     
   }
  
}