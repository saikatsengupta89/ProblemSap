import scala.io.Source

object useDictionary {
  
   def main (args: Array[String]) {
          val data= Source.fromFile("C:/Scala Workspace/Practice/userData/SampleData.txt").getLines().toList
          val header= data(0)
          val dataset= data.filter(x=> x!=header)
          
          val output=
          dataset.map(x=> {
            val k= x.split(",")
            val name= k(0).toString()
            val age= k(1).toString()
            (name, age)
          })
          .groupBy(x=> (x._1, x._2)).map(x=> x._1)
          
          output.foreach(println)
      }
}