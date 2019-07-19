package src.main.scala.es1

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

class MainSpark1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Es1 Spark").getOrCreate()
    import spark.sqlContext.implicits._

    //letto a mano per via di righe con campi contenenti LF, non supportati dal lettore di csv di Spark
    val csv = spark.sparkContext.textFile("/user/rgjinaj/animelists_cleaned.csv")
      .map(line => line.split(","))
      .filter(_.length > 1)
      .map(_.take(2))
      .filter(couple => couple(0) != "" && couple(1) != "")
    val rows = csv.filter(_(0) != "username").map(row => Row(row(0), row(1)))
    val schema = new StructType().add("username", StringType, false).add("anime_id",StringType, false)
    val animelists = spark.createDataFrame(rows, schema)

    //val animelists = spark.read.option("header","true").csv("/user/rgjinaj/animelists_cleaned.csv").select("username","anime_id")
    val anime = spark.read.option("header",true).csv("/user/rgjinaj/anime_cleaned.csv").select("anime_id","title","source")
    val users = spark.read.option("header",true).csv("/user/rgjinaj/users_cleaned.csv").select("username","birth_date").map{row=> (row.getString(0),zone(row.getString(1)))}.toDF("username","zone")
    val firstJoin= animelists.join(anime, "anime_id")
    val secondJoin = firstJoin.join(users,"username")

    import org.apache.spark.sql.expressions.Window
    val counts = secondJoin.groupBy("source", "zone","title").agg(count("username").alias("count"))
    val w = Window.partitionBy("source","zone").orderBy(desc("count"))
    val finalResult = counts.withColumn("row",row_number().over(w)).where("row < 6").drop("row").orderBy(asc("source"),asc("zone"),desc("count"))
    finalResult.write.mode(SaveMode.Overwrite).csv("spark/es1")
  }
  import scala.util.Try
  def zone(birthdate:String):String = {
    2019 - Try (birthdate.take(4).toInt).getOrElse(2002) match {
      case x if x< 25=> "0-24"
      case x if x< 50=> "25-49"
      case _=> "50-99"
    }
  }
}
