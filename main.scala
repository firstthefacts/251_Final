import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

//

object HoneyPotApp{
  def main(args: Array[String]) {
     // create SparkConf
  
     val conf = new SparkConf().setAppName("honey-pot")
  
     val sc = new SparkContext(conf)

     val sqlContext = new SQLContext(sc)
     import sqlContext.implicits._
     val spark =sqlContext.sparkSession


     // read the session json file from hdfs
     //  since we are reading from an app executed as root, we will read from that directory
     val rawDF = spark.read.format("json").json("hdfs:/user/root/session.json")

     // flatten the fields from the original layered output format - and then rename the fields  
     //   to get rid of the dollar-signs in the field names
   
     val flatDF=rawDF.select($"_id.*",$"destination_port",$"honeypot",$"hpfeed_id.*",$"identifier",$"protocol",$"source_ip",$"source_port",$"timestamp.*")

     val renamedFlatDF =  flatDF.toDF("oid","destination_port","honeypot","hpfeed_oid","identifier","protocol","source_ip","source_port","date")

     renamedFlatDF.groupBy("honeypot").count().orderBy($"count".desc).show()
     renamedFlatDF.groupBy("destination_port").count().orderBy($"count".desc).show()
     renamedFlatDF.groupBy("protocol").count().orderBy($"count".desc).show()
     renamedFlatDF.groupBy("identifier").count().orderBy($"count".desc).show()
     renamedFlatDF.groupBy("source_ip").count().orderBy($"count".desc).show()

     renamedFlatDF.take(10).foreach(println)
 
     renamedFlatDF.coalesce(1).write.mode("append").format("json").save("hdfs:/user/root/honey4_out.json")
   }
}


