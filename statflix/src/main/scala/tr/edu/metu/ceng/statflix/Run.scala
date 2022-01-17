package tr.edu.metu.ceng.statflix

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Run {

  case class Country(fields: Array[String]) {
    val country: String = fields(0)
    val availableShows: Array[String] = java.net.URLDecoder.decode(fields(1), "UTF-8").split("; ")
  }

  case class Show(fields: Array[String]) {
    val title: String = fields(0)
    val showType: String = fields(1)
    val directors: Array[String] = {
        if (fields(2) != null) {java.net.URLDecoder.decode(fields(2), "UTF-8").split(",")}
        else {Array("")}
    }
    val cast: Array[String] = {
      if (fields(3) != null) {java.net.URLDecoder.decode(fields(3), "UTF-8").split(",")}
      else {Array("")}
    }
    val producingCountries: Array[String] = {
      if (fields(4) != null) {java.net.URLDecoder.decode(fields(4), "UTF-8").split(",")}
      else {Array("")}
    }
    val viewerRating: String = fields(5)
    val listed_in: Array[String] = java.net.URLDecoder.decode(fields(6), "UTF-8").split(",")
    val description: String = fields(7)
    val availableCountries: Array[String] = {
      if (fields(8) != "") {
        java.net.URLDecoder.decode(fields(8), "UTF-8")
        .split(", ")
        .map(c => c.split(": ")(1).replaceAll("\"", ""))}
      else {Array("")}
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("StatFlix").config("spark.master", "local[*]").getOrCreate()

    // show_id,type,title,director,cast,produced_by,date_added,release_year,viewer_rating,duration,listed_in,description
    val kaggleData = spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .load("data/netflix_titles.csv")

    val unogsData = spark.read
      .format("json")
      .option("multiline","true")
      .json("data/out-cleaned-string.json")

    val titlesWithCountries = unogsData.select("title", "clist")
    val joined = kaggleData.join(titlesWithCountries,"title")

    /*
    val fancyPrintForPresentation = joined
      .drop("show_id")
      .drop("date_added")
      .drop("viewer_rating")
      .drop("release_year")
      .drop("duration")
      .drop("viewer_rating")
      .drop("description")
      .drop("produced_by")
    fancyPrintForPresentation.show()
    */

    val shows = joined.rdd.map(row => {
      val title = row.getString(0)
      val showType = row.getString(2)
      val directors = row.getString(3)
      val cast = row.getString(4)
      val producingCountries = row.getString(5)
      val viewerRating = row.getString(8)
      val listed_in = row.getString(10)
      val description = row.getString(11)
      val availableCountries = row.getString(12)
      Array(title, showType, directors, cast, producingCountries, viewerRating, listed_in, description, availableCountries)
    }).collect()

    // ARRAY OF ALL SHOW OBJECTS
    val allShows = shows.map(show => Show(show))

    // MAP OF SHOW TITLE -> SHOW OBJECT
    val showMap = allShows.map(show => (show.title, show)).toMap
    println("shows in the map: ", showMap.size)

    val countriesShowPairs = allShows.toSeq
      .map(s => (s.availableCountries, s.title))
      .flatMap(p => p._1.map(c => (c, p._2)))

    val showsOfCountries = countriesShowPairs
      .groupBy(p => p._1)
      .mapValues(l => l.map(p => p._2))
      .mapValues(l => l.mkString("; "))
      .toArray

    // ARRAY OF ALL COUNTRY OBJECTS
    val allCountries = showsOfCountries.map(country => Country(Array(country._1,country._2)))

    // MAP OF COUNTRY NAME -> COUNTRY OBJECT
    val countryMap = allCountries.map(country => (country.country, country)).toMap
    println("countries in the map: ", countryMap.size)

    // MAP OF COUNTRY OBJECT -> [SHOW OBJECT]
    val countryShowMap = allCountries
      .map(c => (c, c.availableShows)).toMap
      .mapValues(a => a.map(s => showMap.getOrElse(s, null)))
      .mapValues(s => s.filter(s => s != null))
    
  }

}
