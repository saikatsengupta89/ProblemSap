import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.sql.Timestamp
import java.time.Instant

object modelUserSession {
  
    def main (args: Array[String]) {
        
        val sparkConf = new SparkConf().setAppName("enrichUserSession")
                                       .setMaster("local[*]")
        
        val sc        = new SparkContext(sparkConf)
        val sqlContext= new SQLContext (sc)
        
        import sqlContext.implicits._
        
        val dataset= sqlContext.read.parquet("C:/Scala Workspace/Practice/userData/enrichedDataWithSession")
        dataset.registerTempTable("user_data")
        
        //enrich the dataset with total time spent in each session assuming each session has a minimum time span of 30 mins
        //if sum(duration over a session) < 30 then 30 mins
        //if sum(duration over a session) < 1 hr 30 mins, then take duration
        //if sum(duration over a session) > 1 hr 30 mins, then take 2 hours duration
        val dataset_t1 = sqlContext.sql (
            "select "+
            "from_unixtime(unix_timestamp(dateTime), 'yyyy') yearId, "+
            "from_unixtime(unix_timestamp(dateTime), 'MMM') monthId, "+
            "cast(from_unixtime(unix_timestamp(dateTime), 'yyyyMMdd') as int) timeKey, "+
            "strDateTime, "+
            "userId, "+
            "session_id, "+
            "unix_timestamp(dateTime) ts_curr, "+
            "nvl(lag(unix_timestamp(dateTime)) over (partition by userId, session_id order by userId, dateTime),0) ts_prev "+
            "from user_data"
        )
        
        dataset_t1.registerTempTable("user_data_t1")
        
        val dataset_t2 = sqlContext.sql (
            "select "+
            "yearId, "+
            "monthId, "+
            "timeKey, "+
            "strDateTime, "+
            "userId, "+
            "session_id, "+
            "ts_curr, "+
            "ts_prev, "+
            "case when ts_prev=0 then 0 else (ts_curr - ts_prev)/60 end duration "+
            "from user_data_t1 "+
            "order by userId, ts_curr"
        )
        dataset_t2.registerTempTable("user_data_t2")
        
        val dataset_t3 = sqlContext.sql (
            "select "+
            "yearId, "+
            "monthId, "+
            "timeKey, "+
            "strDateTime, "+
            "userId, "+
            "session_id, "+
            "duration, "+
            "case when duration <= 30 then 30 "+
            "     when duration > 30 and duration <= 90 then duration "+
            "     when duration > 90 then 120 end total_duration "+
            "from ( "+
            "select "+
            "yearId, "+
            "monthId, "+
            "timeKey, "+
            "strDateTime, "+
            "userId, "+
            "session_id, "+
            "ts_curr, "+
            "ts_prev, "+
            "sum(duration) over (partition by userId, session_id) duration "+
            "from user_data_t2) Q1 "+
            "order by userId, ts_curr"
        )
        
        dataset_t3.show()
    }
}