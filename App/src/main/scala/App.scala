import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SQLContext
import scala.io.Source
object App{
  def main(args: Array[String]): Unit = {

    if(args.length < 2) {
      println("usage:\nApp.jar routeJson routeTxt saveRoute formatSave")
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val ghLog = sqlContext.read.json(args(0))

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
    //ordenamos polo campo count descendente
    val ordered = counted.orderBy(counted("count").desc)
    //sacamos unha mostra de 5
    ordered.show(5)

    val arquivo = Source.fromFile(args(1))
    val liñas = arquivo.getLines().map(_.trim).toSet
    import sqlContext.implicits._ //importamos as conversions implicitas que nos van a facilitar o traballo
    //creamos unha variable de broadcast para que non envie de cada vez a variable os executores
    val broadcastLiñas = sc.broadcast(liñas)
    val udfIsEmp = user => broadcastLiñas.value.contains(user)

    //rexistramos no contexto unha nova funcion
    val eEmpregado = sqlContext.udf.register("isEmpUDF",udfIsEmp)

    //un dos implicitos permite crear unha columna asi
    val filtered = ordered.filter(eEmpregado($"login"))
    filtered.write.format(args(3)).save(args(2))

  }
}
