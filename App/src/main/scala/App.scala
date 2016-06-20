import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SQLContext
object App{
  def main(args: Array[String]): Unit = {
  
    val conf = new SparkConf().setAppName("github push counter").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    //collemos do entorno o home
    val home = System.getenv("HOME")
    val inputPath = home+"/spark-in-action/github‐archive/2015-01-01-0.json"

    val ghLog = sqlContext.read.json(inputPath)

    val pushJson = ghLog.filter("type = 'PushEvent'")

    //imprimimos o schema do json
    pushJson.printSchema()
    println("Totais "+ghLog.count)
    println("Filtrados "+pushJson.count)
    //amosamos unha mostra de 5
    pushJson.show(5)

    //agrupamolos polo campo
    val gruped = pushJson.groupBy("actor.login")
    //reducimos o agrupamento
    val counted = gruped.count

    //sacamos unha mostra de 5
    counted.show(5)
  }
}
