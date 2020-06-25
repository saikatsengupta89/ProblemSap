import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object parseNestedJSON {
  
  def expand_nested_column (json_data:DataFrame): DataFrame = {
      //keep the select list clause empty initially
      var selectClauseList = List.empty[String]
      var jsonDataDF = json_data
      
      for (columnName <- jsonDataDF.schema.fieldNames){
            if((jsonDataDF.schema(columnName).dataType.isInstanceOf[ArrayType])) {
                jsonDataDF= jsonDataDF.withColumn(columnName, explode(jsonDataDF(columnName))).alias(columnName)
                selectClauseList :+= columnName
            }
            else if ((jsonDataDF.schema(columnName).dataType.isInstanceOf[StructType])) {
                for (field <- jsonDataDF.schema(columnName).dataType.asInstanceOf[StructType].fields){
                        selectClauseList :+= columnName + "." + field.name
                }
            }
            else {
              selectClauseList :+= columnName
            }
            
        }
      
      val columnNames = selectClauseList.map(name => col(name).alias(name.replace('.', '_')))
      print(columnNames)
      // Selecting columns using select_clause_list from dataframe: json_data_df
      jsonDataDF.select(columnNames:_*)
  }
  
  def main (args: Array[String]) {
        
        val spark = SparkSession.builder()
                                .appName("parseJSONData")
                                .master("local[*]")
                                .getOrCreate()

        import spark.implicits._
        spark.sparkContext.setLogLevel("ERROR")
        
        var jsonDataDF= spark.read.option("multiline",true).json("C:/Data Engineer/sampleJson.json")
        jsonDataDF.printSchema()
        jsonDataDF.show()
        var nestedCounter =1
        
        while (nestedCounter != 0) {
            var nested_counter_tmp =0        
            for (columnName <- jsonDataDF.schema.fieldNames) {
                if((jsonDataDF.schema(columnName).dataType.isInstanceOf[ArrayType]) || (jsonDataDF.schema(columnName).dataType.isInstanceOf[StructType])) {
                  nested_counter_tmp += 1
                }
            }
            if (nested_counter_tmp != 0){
                jsonDataDF = expand_nested_column(jsonDataDF)
                jsonDataDF.show(10, false)
            } 
            
            println("Priniting nested column count : "+ nested_counter_tmp)
            nestedCounter = nested_counter_tmp
        
        }
        
        jsonDataDF.show(20, false)
        jsonDataDF.printSchema()
        spark.stop()
  
  }
  
}
//https://www.youtube.com/watch?v=esxb0f5oeXk