package src.main.scala.es2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class MainSpark2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Es1 Spark").getOrCreate()
    import spark.sqlContext.implicits._

    //Numero medio di episodi e durata media delle puntate degli anime raggruppati per tipologia
    val anime = spark.sparkContext.textFile("/user/rgjinaj/anime_cleaned.csv")
      .map(line=> clearESplit(line))
      .filter(line=>line(7)!="source")
      .map(line=>(line(7),Integer.parseInt(line(8)),convertDuration(line(13)))).toDF("source","episodes","duration")
      .groupBy("source").agg(avg("episodes").as("episodes"),avg("duration").as("duration"))
      .select(col("source"),round(col("episodes"),2) as "episode",round(col("duration"),2) as "duration")
      .orderBy("duration")
  }

   def convertDuration(s: String) = {
    val parts = s.replaceAll(" per ep.", "").split(" ")
    if (parts.length > 1) {
      var result = 0
      var i = 0
      while ( {
        i < parts.length
      }) {
        val um = parts(i + 1)
        if (um == "hr.") result += parts(i).toInt * 60
        else if (um == "min.") result += parts(i).toInt
        i += 2
      }
      result
    }
    else -1
  }


  def clearESplit(s: String): Array[String] = {
    val sb = new StringBuilder(s)
    var quote = false
    var i = 0
    while ( {
      i < s.length
    }) {
      if (s.charAt(i) == '\"') quote = !quote
      else if (s.charAt(i) == ',' && quote) sb.setCharAt(i, '\u00B8') //bug virogla nei titoli, al posto della virgola si mette la chedil

      {
        i += 1; i - 1
      }
    }
    sb.toString.split(",")
  }

}
