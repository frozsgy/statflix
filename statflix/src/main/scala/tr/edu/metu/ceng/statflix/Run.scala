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
    val listed_in: Array[String] = java.net.URLDecoder.decode(fields(6), "UTF-8").split(", ")
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

    val countriesShowPairs = allShows.toSeq
      .map(s => (s.availableCountries, s.title))
      .flatMap(p => p._1.map(c => (c, p._2)))

    val showsOfCountries = countriesShowPairs
      .groupBy(p => p._1)
      .mapValues(l => l.map(p => p._2))
      .mapValues(l => l.mkString("; "))
      .toArray

    // ARRAY OF ALL COUNTRY OBJECTS
    val allCountries = showsOfCountries
      .map(country => Country(Array(country._1,country._2)))
      .filter(c => c.country != "")

    // MAP OF COUNTRY NAME -> COUNTRY OBJECT
    val countryMap = allCountries.map(country => (country.country, country)).toMap

    // MAP OF COUNTRY OBJECT -> [SHOW OBJECT] FOR AVAILABLE COUNTRIES
    val countryShowMap = allCountries
      .map(c => (c, c.availableShows)).toMap
      .mapValues(a => a.map(s => showMap.getOrElse(s, null)))
      .mapValues(s => s.filter(s => s != null))
    // countryShowMap.foreach(cs => println(cs._1.country, cs._2.length))

    val countryGenres = countryShowMap
      //.filter(c => (c._1.country == "Turkey" || c._1.country == "United States" ||
      //  c._1.country == "Germany" || c._1.country == "India"))
      .mapValues(a => a.flatMap(s => s.listed_in))
      .mapValues(a => a.groupBy(s => s))
      .mapValues(a => a
        .map(s => (s._1, s._2.length))
        .toList.sortBy(l => l._2)(Ordering[Int].reverse)
        .take(70))
    //countryGenres.foreach(c => println(c._1.country, c._2))

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

    // MAP OF COUNTRY OBJECT -> [SHOW OBJECT] FOR PRODUCING COUNTRIES
    val producerCountryShowMap = allShows
      .map(s => (s, s.producingCountries)).toMap
      .mapValues(a => a.map(c => countryMap.getOrElse(c, null)))
      .mapValues(c => c.filter(c => c != null))
      .map(p => p._2.map(c => (c, p._1))).flatten.toArray
      .groupBy(c => c._1)
      .mapValues(a => a.map(s => s._2))

    val producerCountryGenres = producerCountryShowMap
      //.filter(c => (c._1.country == "Turkey" || c._1.country == "United States" ||
      //  c._1.country == "Germany" || c._1.country == "India"))
      .mapValues(a => a.flatMap(s => s.listed_in))
      .mapValues(a => a.groupBy(s => s))
      .mapValues(a => a
        .map(s => (s._1, s._2.length))
        .toList.sortBy(l => l._2)(Ordering[Int].reverse)
        .take(70))
    //producerCountryGenres.foreach(c => println(c._1.country, c._2))

    val lgbtMoviePerProducerCountry = producerCountryGenres
      .map(c => (c._1,c._2.filter(s => s._1 == "LGBTQ Movies")))
    lgbtMoviePerProducerCountry.foreach(c => println(c._1.country, c._2))

    val producerCountryDirectors = producerCountryShowMap
      .mapValues(a => a.flatMap(s => s.directors))
      .mapValues(a => a.groupBy(s => s))
      .mapValues(a => a
        .map(s => (s._1, s._2.length))
        .toList.sortBy(l => l._2)(Ordering[Int].reverse)
        .take(45))
    //producerCountryDirectors.foreach(c => println(c._1.country, c._2))

    val producerCountryCasts = producerCountryShowMap
      .mapValues(a => a.flatMap(s => s.cast))
      .mapValues(a => a.groupBy(s => s))
      .mapValues(a => a
        .map(s => (s._1, s._2.length))
        .toList.sortBy(l => l._2)(Ordering[Int].reverse)
        .take(10))
    //producerCountryCasts.foreach(c => println(c._1.country, c._2))




  }

}
