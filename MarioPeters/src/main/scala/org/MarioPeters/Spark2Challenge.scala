package org.MarioPeters

 import org.apache.spark.SparkContext
 import org.apache.spark.SparkConf
 import org.apache.spark.sql.SparkSession


object Spark2Challenge {
  def main (args: Array[String]) = {
    
    val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("SparkChallengeMarioCorreia")
    .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    println("Spark Session Created")
    
    val apps = spark.read.option("header", true).option("inferSchema",true).csv("googleplaystore.csv")
    val reviews = spark.read.option("header", true).option("inferSchema",true).csv("googleplaystore_user_reviews.csv")
    
    //Views
    apps.createOrReplaceTempView("appsView")
    reviews.createOrReplaceTempView("reviewsView")
    //Views
    
    //Part 1
      val df_1_1 = spark.sql("SELECT App, AVG(CAST(Sentiment_Polarity AS double)) AS Average_Sentiment_Polarity FROM reviewsView GROUP BY App")
      val df_1 = df_1_1.na.fill(0,Seq("Average_Sentiment_Polarity"))
      df_1.show(20, false)
    //Part 1
    
    //Part 2
      val df_2 = spark.sql("SELECT * FROM appsView WHERE Rating>=4 ORDER BY Rating DESC")
      df_2.repartition(1).write.format("com.databricks.spark.csv").option("header",true).save("best_apps")
    //Part 2
    
    //Part3 
      val df_3 = spark.sql("SELECT App, collect_set(Category) AS Categories, MAX(CAST(Rating AS double)) AS Rating, MAX(CAST(Reviews AS long)) AS Reviews,"+
          " MAX(CASE WHEN Size LIKE '%k%' THEN CAST(REPLACE(Size, 'k', '')AS double)/1000 WHEN Size LIKE '%M%' THEN CAST(REPLACE(Size, 'M', '')AS double) END) AS Size, MAX(Installs) AS Installs,"+
          " MAX(Type) AS Type, MAX(CAST(REPLACE(Price, '$', '')AS double)*0.9) AS Price, MAX(`Content Rating`) AS Content_Rating, collect_set(concat_ws(';',Genres)) AS Genres,"+
          " MAX(`Last Updated`) AS Last_Updated, MAX(`Current Ver`) AS Current_Version,"+
          " MIN(`Android Ver`) AS Minimum_Android_Version FROM appsView GROUP BY App")
      df_3.show(20,false)
      // I wasn't able to make every value (in case of app duplicates) to have the value in the row of the best reviewed version of the App
      // And for some reason I couldn't use change Last Updated to date type, it was always giving me null values
    //Part 3
    
    //Views
      df_1.createOrReplaceTempView("df1View")
      df_3.createOrReplaceTempView("df3View")
    //Views
    
    //Part 4
      val df_cleaned = spark.sql("SELECT df3.App, Categories, Rating, Reviews, Size, Installs, Type, Price, Content_Rating, Genres, Last_Updated, Current_Version, Minimum_Android_Version, df1.Average_Sentiment_Polarity"+
          " FROM df3View df3 INNER JOIN df1View df1 WHERE df3.App = df1.App")
      df_cleaned.repartition(1).write.option("compression", "gzip").option("header",true).parquet("googleplaystore_cleaned")
    //Part 4

    //Part 5
      val df_4 = spark.sql("SELECT Genres, count(dff3.App) AS Count,AVG(Rating) AS Average_Rating, AVG(Average_Sentiment_Polarity) AS Average_Sentiment_Polarity"+
          " FROM df3View dff3 INNER JOIN df1View dff1 WHERE dff3.App == dff1.App GROUP BY Genres")
      df_4.repartition(1).write.option("compression", "gzip").option("header",true).parquet("googleplaystore_metrics")
    //Part 5
  
    spark.stop()
   
  }    
}