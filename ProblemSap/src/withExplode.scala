import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, MapType, ArrayType, StructType}
import scala.collection.mutable.StringBuilder

object withExplode {
  
    def main (args :Array[String]) {
        val spark = SparkSession.builder()
                                .appName("parseJSONData")
                                .master("local[*]")
                                .getOrCreate()

        import spark.implicits._
        spark.sparkContext.setLogLevel("ERROR")
        
        val arrayData = List (
            Row("James",List("Java","Scala"),Map("hair"->"black","eye"->"brown")),
            Row("Michael",List("Spark","Java",null),Map("hair"->"brown","eye"->null)),
            Row("Robert",List("CSharp",""),Map("hair"->"red","eye"->"")),
            Row("Washington",null,null),
            Row("Jefferson",List(),Map())
        )
        
        val arrayData2 = Tuple5 (
            Row("James",List("Java","Scala"),Map("hair"->"black","eye"->"brown")),
            Row("Michael",List("Spark","Java",null),Map("hair"->"brown","eye"->null)),
            Row("Robert",List("CSharp",""),Map("hair"->"red","eye"->"")),
            Row("Washington",null,null),
            Row("Jefferson",List(),Map())
        )
        
        
        val arraySchema = new StructType().add("name",StringType)
                                          .add("knownLanguages", ArrayType(StringType))
                                          .add("properties", MapType(StringType, StringType))                                
        println(arrayData)
        println(arrayData2)
                                          
        val sample_rdd= spark.sparkContext.parallelize(arrayData)
        val sample_df= spark.createDataFrame(sample_rdd, schema=arraySchema)
        
        //false= to show full content of each column without trunc
        sample_df.show(false)
        sample_df.select(col("name"),explode(col("knownLanguages")).alias("Languages")).show()
        sample_df.select($"name",explode($"properties")).show()     
        
        val final_df= sample_df.select($"name", explode($"knownLanguages").alias("languages"), $"properties")
                               .select($"name", $"languages", explode($"properties"))
                               .select(col("name").alias("student_name"), 
                                       col("languages").alias("programLang"), 
                                       col("key").alias("features"), 
                                       col("value").alias("values")
                                      )
        final_df.show(20,false)
        sample_df.select($"name",explode_outer($"knownLanguages").alias("Languages")).show()
        
    }
}
