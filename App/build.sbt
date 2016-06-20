scalaVersion := "2.10.5"
version := "0.1"
name := "GitHub_push_counter"

libraryDependencies ++= Seq(
  //dependencias do core
  "org.apache.spark" % "spark-core_2.10" % "1.6.1",
  //dependencias de spark-sql
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1"
)
