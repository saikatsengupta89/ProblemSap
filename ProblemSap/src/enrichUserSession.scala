import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import java.sql.Timestamp
import java.time.Instant

object enrichUserSession {
   
    def getTimeStamp(s:String):Timestamp= {
        Timestamp.from(Instant.parse(s))
    }
  
    def main (args : Array[String]) {
        
        val sparkConf = new SparkConf().setAppName("enrichUserSession")
                                       .setMaster("local[*]")
        
        val sc        = new SparkContext(sparkConf)
        val sqlContext= new SQLContext (sc)
        val scc       = new StreamingContext(sc, Seconds(2))
        
        import sqlContext.implicits._
        
        val dataset= sc.textFile("C:/Scala Workspace/Practice/userData/user_data.txt", 2)
        val header = dataset.first
        
        val dataset_t1 = dataset.filter (line => line != header)
        
        val dataset_t2 = dataset_t1.map(line => {
                                        val k= line.split("\t")
                                        val strDateTime= k(0).toString()
                                        val tsDateTime = getTimeStamp(k(0))
                                        val userId = k(1).toString()
                                        
                                        (strDateTime, tsDateTime, userId)
                                    }).toDF("strDateTime", "dateTime", "userId")
        
         dataset_t2.registerTempTable("user_data")
         //dataset_t2.show()
         
         val dataset_t3 = sqlContext.sql("select "+
                                         "strDateTime, "+
                                         "dateTime, "+
                                         "unix_timestamp(dateTime, 'yyyy-MM-dd HH:MM:SS') tsDateTime, "+
                                         "nvl(unix_timestamp(lag(dateTime) over(partition by userId order by dateTime), 'yyyy-MM-dd HH:MM:SS'),0) prevTsDateTime, "+
                                         "userId " +
                                         "from user_data")
         dataset_t3.registerTempTable("user_data_t1")
         //dataset_t3.show()
         
         val dataset_t4 = sqlContext.sql("select "+
                                         "strDateTime, "+
                                         "dateTime, "+
                                         "tsDateTime, "+
                                         "prevTsDateTime, "+
                                         "cast(case when prevTsDateTime =0 then 0 else (tsDateTime - prevTsDateTime) end as int) time_diff, "+
                                         "case when prevTsDateTime=0 then 0 "+
                                         "     when (tsDateTime - prevTsDateTime) =< 1800 then 0 "+
                                         "     else 1 end  as session_flag, "+
                                         "row_number() over (partition by userId order by 1) RN, "+
                                         "userId " +
                                         "from user_data_t1 "+
                                         "order by userId, tsDateTime")
         dataset_t4.registerTempTable("user_data_t2")
         
         val dataset_t5 =  sqlContext.sql("select "+
                                          "strDateTime, "+
                                          "dateTime, "+
                                          "userId, "+
                                          "concat('S', cast(sum(session_flag) over (partition by userId order by tsDateTime) as string)) as session_id "+
                                          "from ( "+
                                          "select "+
                                          "strDateTime, "+
                                          "dateTime, "+
                                          "tsDateTime, "+
                                          "prevTsDateTime, "+
                                          "case when RN=1 then 1 "+
                                          "     when session_flag=1 then 1 "+
                                          "     else 0 end session_flag, "+
                                          "userId " +
                                          "from user_data_t2 "+
                                          "order by userId, tsDateTime "+
                                          ") Q1")
         dataset_t5.repartition(1)
                   .write
                   .mode("overwrite")
                   .parquet("C:/Scala Workspace/Practice/userData/enrichedDataWithSession")
    }
}