package tr.edu.metu.ceng.statflix

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TitlesInWeeklyLists {

  case class Country(fields: Array[String]) {
    val country: String = fields(0)
    val popularShows: Array[String] = java.net.URLDecoder.decode(fields(1), "UTF-8").split("; ")
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
    val listed_in: Array[String] = java.net.URLDecoder.decode(fields(6), "UTF-8").split(", ")
    val description: String = fields(7)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("StatFlix").config("spark.master", "local[*]").getOrCreate()

    // show_id,type,title,director,cast,produced_by,date_added,release_year,viewer_rating,duration,listed_in,description
    val kaggleData = spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .load("data/netflix_titles.csv")


    val weeklyData = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .load("data/all-weeks-countries.tsv")

    val countryTops = weeklyData
      .select("country_name", "week","show_title")
      .withColumnRenamed("show_title", "title")
    val countriesWithShowInfo = countryTops.join(kaggleData, "title")
    val commonShowCount = countriesWithShowInfo.select("title").distinct().count()
    println(commonShowCount) // only 680 common titles with country

    countriesWithShowInfo.show(10)

    val shows = countriesWithShowInfo.rdd.map(row => {
      val title = row.getString(0)
      val showType = row.getString(4)
      val directors = row.getString(5)
      val cast = row.getString(6)
      val producingCountries = row.getString(7)
      val viewerRating = row.getString(10)
      val listed_in = row.getString(12)
      val description = row.getString(13)
      Array(title, showType, directors, cast, producingCountries, viewerRating, listed_in, description)
    }).collect()

    // ARRAY OF ALL SHOW OBJECTS
    val allShows = shows.map(show => Show(show))

    // MAP OF SHOW TITLE -> SHOW OBJECT
    val showMap = allShows.map(show => (show.title, show)).toMap

    val countryShowPairs = countriesWithShowInfo
      .select("country_name", "title")
      .rdd.map(row => {
        val country = row.getString(0)
        val title = row.getString(1)
        Array(country, title)
      }).collect()

    val countries = countryShowPairs
      .map(pair => (pair(0), pair(1)))
      .groupBy(p => p._1)
      .mapValues(a => a.map(ss => ss._2))
      .mapValues(s => s.mkString("; "))
      .toArray

    val allCountries = countries
      .map(country => Country(Array(country._1,country._2)))
      .filter(c => c.country != "")

    // MAP OF COUNTRY OBJECT -> [SHOW OBJECT] FOR AVAILABLE COUNTRIES
    val countryShowMap = allCountries
      .map(c => (c, c.popularShows)).toMap
      .mapValues(a => a.map(s => showMap.getOrElse(s, null)))
      .mapValues(s => s.filter(s => s != null))
    //countryShowMap.take(10).foreach(cs => println(cs._1.country, cs._1.popularShows.mkString("; ")))

    val countryGenres = countryShowMap
      //.filter(c => (c._1.country == "Turkey" || c._1.country == "United States" ||
      //  c._1.country == "Germany" || c._1.country == "India"))
      .mapValues(a => a.flatMap(s => s.listed_in))
      .mapValues(a => a.groupBy(s => s))
      .mapValues(a => a
        .map(s => (s._1, s._2.length))
        .toList.sortBy(l => l._2)(Ordering[Int].reverse)
        .take(70))
    countryGenres.foreach(c => println(c._1.country, c._2))

    val lgbtMoviePerCountry = countryGenres
      .map(c => (c._1,c._2.filter(s => s._1 == "LGBTQ Movies")))
    //lgbtMoviePerCountry.foreach(c => println(c._1.country, c._2))

    val countryDirectors = countryShowMap
      .mapValues(a => a.flatMap(s => s.directors))
      .mapValues(a => a.groupBy(s => s))
      .mapValues(a => a
        .map(s => (s._1, s._2.length))
        .toList.sortBy(l => l._2)(Ordering[Int].reverse)
        .take(45))
    //countryDirectors.foreach(c => println(c._1.country, c._2))

    val countryCasts = countryShowMap
      .mapValues(a => a.flatMap(s => s.cast))
      .mapValues(a => a.groupBy(s => s))
      .mapValues(a => a
        .map(s => (s._1, s._2.length))
        .toList.sortBy(l => l._2)(Ordering[Int].reverse)
        .take(10))
    //countryCasts.foreach(c => println(c._1.country, c._2))

  }

}
