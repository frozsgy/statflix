package tr.edu.metu.ceng.statflix

import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, broadcast, col, desc, expr, udf}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.expressions.Window.orderBy


object ContentBasedRecommendation {

  def main(args: Array[String]): Unit = {
    var sparkSession: SparkSession = null
    try{
      sparkSession = SparkSession.builder().appName("Content Based Movie Recommendation").config("spark.master", "local[*]").getOrCreate()
      val movies = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "true")
        .load("../data/netflix_titles.csv").toDF()
      val columnName = Seq("title","director","cast","produced_by","listed_in")
      val colNames = columnName.map(name => col(name))
      var moviesSelected = movies.select(colNames: _*)
      moviesSelected = moviesSelected.na.fill("")
      moviesSelected.show()
      val features = moviesSelected.withColumn("columnArray", array("director","cast","produced_by","listed_in"))
      val feature_df =features.withColumn("features",expr("filter(columnArray, x -> x != '')"))
      feature_df.show()
      val cv = new CountVectorizer().setInputCol("features").setOutputCol("cv").setVocabSize(5000)
      val cvModel = cv.fit(feature_df.select("features")).transform(feature_df).select("title","features","cv")

      val cosinSim = udf {(v1: SparseVector, v2: SparseVector) =>
          v1.indices.intersect(v2.indices).map(v => v1(v) * v2(v)).reduceOption(_ + _).getOrElse(0.0)
      }
      val cvModel2 = cvModel.selectExpr("title as title2", "features as features2", "cv as cv2")

      val df = cvModel.crossJoin(broadcast(cvModel2)).withColumn("dot", cosinSim(col("cv"), col("cv2")))
      df.show()
      val user_title = scala.io.StdIn.readLine("Give me a TV Show or movie, then I will give you 10 similar ones!\n")
      val movieByUser = df.filter(df("title")===user_title).orderBy(desc("dot"))
      val recommendation = movieByUser.select("title2").limit(11).collectAsList()
      recommendation.forEach(println)


    }
    catch {
      case e: Exception => throw e
    }
    finally {
      sparkSession.stop()
    }
  }

}
