import sbt.Keys.libraryDependencies

name := "mapreduce_project"
version := "0.1"
scalaVersion := "2.12.0"



lazy val Spark = project.settings(
  libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
)

lazy val MapReduce = project