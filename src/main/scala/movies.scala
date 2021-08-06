import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession,Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.regexp_extract


object movies {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession
      .builder()
      .appName("movies database")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val moviesRDD = spark.sparkContext.textFile(("data/movies.dat"))
      .map(line => line.split("::"))
      .map(x => (x(0),x(1),x(2)))
    val moviesDF = spark.createDataFrame(moviesRDD).toDF("movie_ID", "title", "genre")

    val usersRDD = spark.sparkContext.textFile("data/users.dat")
      .map(line => line.split("::"))
      .map(x => (x(0),x(1),x(2),x(3),x(4)))
    val usersDF = spark.createDataFrame(usersRDD).toDF("user_ID","gender","age","occupation","zip")

    val ratingsRDD = spark.sparkContext.textFile(("data/ratings.dat"))
      .map(line => line.split("::"))
      .map(x => (x(0),x(1),x(2),x(3)))
    val ratingsDF = spark.createDataFrame(ratingsRDD).toDF("user_ID","movie_ID","rating","timestamp")

    moviesDF.createOrReplaceTempView("movies")
    usersDF.createOrReplaceTempView("users")
    ratingsDF.createOrReplaceTempView("ratings")

    val genreDF = moviesRDD.flatMap(x => x._3.split("\\|")).toDF("genre")
    genreDF.dropDuplicates.createOrReplaceTempView("genres")

    val yearsDF = moviesDF
      .withColumn("year", regexp_extract($"title", "\\(\\d\\d\\d\\d\\)", 0))
    yearsDF.createOrReplaceTempView("years")

//    spark.sql("SELECT COUNT(movie_ID) FROM movies").show()
//    spark.sql("SELECT * FROM users").show()
//    spark.sql("SELECT * FROM ratings").show()
//    spark.sql("SELECT * FROM genres").show()
//    spark.sql("SELECT * FROM years").show()

    println("What are the top 10 most viewed(rated) movies?")
    spark.sql("SELECT COUNT(m.movie_ID) AS count, m.title " +
      "FROM movies m " +
      "JOIN ratings r ON (m.movie_ID = r.movie_ID) " +
      "GROUP BY m.title " +
      "ORDER BY count DESC " +
      "LIMIT 10").show()

    println("What are the distinct list of genres available?")
    spark.sql("SELECT DISTINCT(genre) FROM genres ORDER BY genre ASC").show()

    println("How many movies for each genre?")
    spark.sql("SELECT l.genre, COUNT(l.genre) AS count " +
      "FROM genres l " +
      "INNER JOIN movies r ON (l.genre = r.genre) " +
      "GROUP BY l.genre " +
      "ORDER BY l.genre").show()

    println("How many movies are starting with numbers or letters?")
    val movieAlphaNumDF = moviesRDD.filter(x => x._2.matches("^[0-9a-zA-Z] .+"))
    println(movieAlphaNumDF.count())

    println("List the latest released movies")
    spark.sql("SELECT title, year " +
      "FROM years " +
      "ORDER BY year DESC").show()

    println("Find the list of the oldest released movies")
    spark.sql("SELECT title, year " +
      "FROM years " +
      "ORDER BY year ASC").show()

    println("How many movies are released each year?")
    spark.sql("SELECT year, COUNT(year) AS count " +
      "FROM years " +
      "GROUP BY year " +
      "ORDER BY count DESC").show()

    println("How many number of movies are there for each rating?")
    spark.sql("SELECT rating, COUNT(rating) AS count " +
      "FROM ratings " +
      "GROUP BY rating " +
      "ORDER BY rating").show()

    println("What is the total rating for each movie?")
    spark.sql("SELECT m.title, SUM(r.rating) AS total_rating " +
      "FROM movies m JOIN ratings r ON (m.movie_ID = r.movie_ID) " +
      "GROUP BY m.title " +
      "ORDER BY total_rating DESC").show()

    println("What is the average rating for each movie?")
    spark.sql("SELECT m.title, ROUND(AVG(r.rating),2) AS total_rating " +
      "FROM movies m JOIN ratings r ON (m.movie_ID = r.movie_ID) " +
      "GROUP BY m.title " +
      "ORDER BY total_rating DESC").show()
  }
}
