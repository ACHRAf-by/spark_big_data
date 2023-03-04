from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


input_path = "s3://nahle-bucket-datalake/emr/input/movielens/"
output_path = "s3://s3-de1-abenyahya/output/"
def main():
	
    print("-----------Start ETL process --------------")
    print("-----------Start ETL process --------------")
    
    ## Part 1
    ##Extract
    spark = SparkSession.builder.appName('abenyahyaSession').getOrCreate()
    movies  = spark.read.csv(input_path+"/movies.csv", header = True, inferSchema = True)
    ratings  = spark.read.csv(input_path+"/ratings.csv", header = True, inferSchema = True)
    
    ##Transform
    ratings_date = ratings.withColumn("date", from_unixtime(ratings.timestamp)) 
    movie_with_year = movies.withColumn("year", regexp_extract(movies.title, "\((\d{4})\)", 1))    
    
    movie_ratings = ratings.groupBy("movieId").agg(count("*").alias("num_ratings"), avg("rating").alias("avg_rating"))    
    
    joinedDf = movie_with_year.join(movie_ratings, movie_with_year.movieId==movie_ratings.movieId, "inner").drop(movie_ratings.movieId)
    
    ##Load
    joinedDf.write.csv(output_path+"movielens/newDataFrame", header = True)
    
    print("-----------ETL process finished--------------")
    print("-----------ETL process finished--------------")
    
    print("-----------PART 2 Starting queries--------------")
    ## Part 2
    ## Reload new data from s3 storage
    df = spark.read.csv("s3://s3-de1-abenyahya/output/movielens/newDataFrame/*.csv", header = True, inferSchema = True)
    df.createOrReplaceTempView("movielens_View")
    # And now we can Query the SQL View 
    
    # Best movie by year (spark dataframes)
    # window Specification for the ranking
    window_spec = Window.partitionBy("year").orderBy(desc("num_ratings"), desc("avg_rating"))
    ranked_movies = df.select("year", "title", "num_ratings", "avg_rating", rank().over(window_spec).alias("rank")).filter("rank=1").orderBy(desc("year")).drop("rank")
    
    # Best movie per genre (Spark SQL)
    query2 = spark.sql("SELECT title, genres, num_ratings, avg_rating FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY genres ORDER BY avg_rating DESC, num_ratings DESC) AS rank FROM movielens_View ) WHERE rank = 1")
    
    # Best action movie per year (dataframes)
    window_spec = Window.partitionBy("year").orderBy(desc("num_ratings"), desc("avg_rating"))
    action_movies_df = df.filter(col("genres").contains("Action"))
    best_action_movies = action_movies_df.select("year", "title","genres", "num_ratings", "avg_rating", rank().over(window_spec).alias("rank")).filter("rank=1").orderBy(desc("year")).drop("rank")
    
    # Best romance movie per year (Spark SQL)
    query4 = spark.sql("SELECT title, year, genres, num_ratings, avg_rating FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY year ORDER BY avg_rating DESC, num_ratings DESC ) AS rank FROM movielens_View) WHERE rank = 1 AND genres LIKE '%Romance%'")
    print("-----------PART 2 Finished --------------")    
    print("----------- Finished --------------")


if __name__ == '__main__':
    main()