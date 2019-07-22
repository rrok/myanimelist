package src.main.scala.es2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class MainSpark2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Es1 Spark").getOrCreate()
    import spark.sqlContext.implicits._

    // Average number of episodes and average duration of anime bets grouped by type
    val anime = spark.sparkContext.textFile("/user/rgjinaj/anime_cleaned.csv")
      .map(line=> clearESplit(line))
      .filter(line=>line(7)!="source")
      .map(line=>(line(7),Integer.parseInt(line(8)),convertDuration(line(13)))).toDF("source","episodes","duration")
      .groupBy("source").agg(avg("episodes").as("episodes"),avg("duration").as("duration"))
      .select(col("source"),round(col("episodes"),2) as "episode",round(col("duration"),2) as "duration")
      .orderBy("duration")
  }

  /**
    * Data Duration for this case study
    *
    * @param duration anime duration in hr,min or sec
    * @return converted value in minutes
    */
   def convertDuration(duration: String) = {
    val parts = duration.replaceAll(" per ep.", "").split(" ")
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


  /**
    * since the comma creates problema for the csv reading, \u00B8 charachter ("chedilla") is used instead of it. Next it will be possible to add again comma
    * just replacing it
    * @param uncleanedString
    * @return Cleaned Array[String]
    */
  def clearESplit(uncleanedString: String): Array[String] = {
    val sb = new StringBuilder(uncleanedString)
    var quote = false
    var i = 0
    while ( {
      i < uncleanedString.length
    }) {
      if (uncleanedString.charAt(i) == '\"') quote = !quote
      else if (uncleanedString.charAt(i) == ',' && quote) sb.setCharAt(i, '\u00B8') //fixed comma bug in the titles, instead of the comma the chedilla is inserted

      {
        i += 1; i - 1
      }
    }
    sb.toString.split(",")
  }

}
