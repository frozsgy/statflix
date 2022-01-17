package tr.edu.metu.ceng.statflix

import org.apache.spark.sql.SparkSession

object Run {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("StatFlix").config("spark.master", "local[*]").getOrCreate()
    // show_id,type,title,director,cast,produced_by,date_added,release_year,viewer_rating,duration,listed_in,description
    val kaggleData = spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .load("data/netflix_titles.csv")

    kaggleData.show(10)

    val unogsData = spark.read
      .format("json")
      .option("multiline","true")
      .json("data/unogs-data-formatted.json")

    unogsData.show(10)
    //

  }

}
