from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date, isnan
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,  DoubleType , FloatType

#CREATING A SPARK SESSION :
spark = SparkSession.builder.appName("Playstore_Applications").getOrCreate()

# LOADING THE DATA SOURCE IN SPARK:
#The file used is a csv file, hence the format is 'csv', also the dataset has a heading that's why the header is True.
#The dataset contains applications with double quotation mark in them and hence we have escape set to '"'.
Playstore_Applications = spark.read.load("C:/SparkCourse/SparkCourse/googleplaystore.csv" , format = 'csv' , escape = '"' ,\
 sep = "," , header = "true" , inferschema = "true" , encoding = "utf-8")


#Removing the null values rows:
Playstore_Applications = Playstore_Applications.dropna(how='any')


#Printing the schema:
Playstore_Applications.printSchema()
#root
# |-- App: string (nullable = true)
# |-- Category: string (nullable = true)
# |-- Rating: double (nullable = true)
# |-- Reviews: string (nullable = true)
# |-- Size: string (nullable = true)
# |-- Installs: string (nullable = true)
# |-- Type: string (nullable = true)
# |-- Price: string (nullable = true)
# |-- Content Rating: string (nullable = true)
# |-- Genres: string (nullable = true)
# |-- Last Updated: string (nullable = true)
# |-- Current Ver: string (nullable = true)
# |-- Android Ver: string (nullable = true)


print(Playstore_Applications.count())
#9365



#Checking out the dataframe:
Playstore_Applications.show()
#   +--------------------+--------------+------+-------+----+-----------+----+-----+--------------+--------------------+------------------+------------------+------------+
#   |                 App|      Category|Rating|Reviews|Size|   Installs|Type|Price|Content Rating|              Genres|      Last Updated|       Current Ver| Android Ver|
#   +--------------------+--------------+------+-------+----+-----------+----+-----+--------------+--------------------+------------------+------------------+------------+
#   |Photo Editor & Ca...|ART_AND_DESIGN|   4.1|    159| 19M|    10,000+|Free|    0|      Everyone|        Art & Design|   January 7, 2018|             1.0.0|4.0.3 and up|
#   | Coloring book moana|ART_AND_DESIGN|   3.9|    967| 14M|   500,000+|Free|    0|      Everyone|Art & Design;Pret...|  January 15, 2018|             2.0.0|4.0.3 and up|
#   |U Launcher Lite û...|ART_AND_DESIGN|   4.7|  87510|8.7M| 5,000,000+|Free|    0|      Everyone|        Art & Design|    August 1, 2018|             1.2.4|4.0.3 and up|
#   |Sketch - Draw & P...|ART_AND_DESIGN|   4.5| 215644| 25M|50,000,000+|Free|    0|          Teen|        Art & Design|      June 8, 2018|Varies with device|  4.2 and up|
#   |Pixel Draw - Numb...|ART_AND_DESIGN|   4.3|    967|2.8M|   100,000+|Free|    0|      Everyone|Art & Design;Crea...|     June 20, 2018|               1.1|  4.4 and up|
#   |Paper flowers ins...|ART_AND_DESIGN|   4.4|    167|5.6M|    50,000+|Free|    0|      Everyone|        Art & Design|    March 26, 2017|               1.0|  2.3 and up|
#   |Smoke Effect Phot...|ART_AND_DESIGN|   3.8|    178| 19M|    50,000+|Free|    0|      Everyone|        Art & Design|    April 26, 2018|               1.1|4.0.3 and up|
#   |    Infinite Painter|ART_AND_DESIGN|   4.1|  36815| 29M| 1,000,000+|Free|    0|      Everyone|        Art & Design|     June 14, 2018|          6.1.61.1|  4.2 and up|
#   |Garden Coloring Book|ART_AND_DESIGN|   4.4|  13791| 33M| 1,000,000+|Free|    0|      Everyone|        Art & Design|September 20, 2017|             2.9.2|  3.0 and up|
#   |Kids Paint Free -...|ART_AND_DESIGN|   4.7|    121|3.1M|    10,000+|Free|    0|      Everyone|Art & Design;Crea...|      July 3, 2018|               2.8|4.0.3 and up|
#   |Text on Photo - F...|ART_AND_DESIGN|   4.4|  13880| 28M| 1,000,000+|Free|    0|      Everyone|        Art & Design|  October 27, 2017|             1.0.4|  4.1 and up|
#   |Name Art Photo Ed...|ART_AND_DESIGN|   4.4|   8788| 12M| 1,000,000+|Free|    0|      Everyone|        Art & Design|     July 31, 2018|            1.0.15|  4.0 and up|
#   |Tattoo Name On My...|ART_AND_DESIGN|   4.2|  44829| 20M|10,000,000+|Free|    0|          Teen|        Art & Design|     April 2, 2018|               3.8|  4.1 and up|
#   |Mandala Coloring ...|ART_AND_DESIGN|   4.6|   4326| 21M|   100,000+|Free|    0|      Everyone|        Art & Design|     June 26, 2018|             1.0.4|  4.4 and up|
#   |3D Color Pixel by...|ART_AND_DESIGN|   4.4|   1518| 37M|   100,000+|Free|    0|      Everyone|        Art & Design|    August 3, 2018|             1.2.3|  2.3 and up|
#   |Learn To Draw Kaw...|ART_AND_DESIGN|   3.2|     55|2.7M|     5,000+|Free|    0|      Everyone|        Art & Design|      June 6, 2018|               NaN|  4.2 and up|
#   |Photo Designer - ...|ART_AND_DESIGN|   4.7|   3632|5.5M|   500,000+|Free|    0|      Everyone|        Art & Design|     July 31, 2018|               3.1|  4.1 and up|
#   |350 Diy Room Deco...|ART_AND_DESIGN|   4.5|     27| 17M|    10,000+|Free|    0|      Everyone|        Art & Design|  November 7, 2017|               1.0|  2.3 and up|
#   |FlipaClip - Carto...|ART_AND_DESIGN|   4.3| 194216| 39M| 5,000,000+|Free|    0|      Everyone|        Art & Design|    August 3, 2018|             2.2.5|4.0.3 and up|
#   |        ibis Paint X|ART_AND_DESIGN|   4.6| 224399| 31M|10,000,000+|Free|    0|      Everyone|        Art & Design|     July 30, 2018|             5.5.4|  4.1 and up|
#   +--------------------+--------------+------+-------+----+-----------+----+-----+--------------+--------------------+------------------+------------------+------------+
#only showing top 20 rows



#Dropping less useful data for more clearity:
Playstore_Applications = Playstore_Applications.drop('Size', 'Content rating', 'Genres' ,'Type')



#Since many columns in out dataframe contains spaces in their names which is undesirable for executing SQL commands.
#Hence renaming the columns becomes our priority.
# STEPS:
# |-- App: string (nullable = true)
# |-- Category: string (nullable = true)
# |-- Rating: double (nullable = true)
# |-- Reviews :string (nullable = true) -> No_Reviews -> No_Reviews : Integer
# |-- Installs :string (nullable = true) -> No_Installs -> No_Installs : Integer(nullable = true)
# |-- Price: string (nullable = true) -> Removing dollar sign from paid apps -> Price : Integer(nullable = true)
# |-- Last Updated: string (nullable = true) -> Last_Updated -> Last_Updated : date(nullable = true)
# |-- Current Ver: string (nullable = true) -> Current_Version
# |-- Android Ver: string (nullable = true) -> Android_Version

#Changing type of Reviews column from string to integer.
Playstore_Applications = Playstore_Applications.withColumnRenamed("Reviews" , "No_Reviews").withColumn("No_Reviews", \
    col("No_Reviews").cast(IntegerType()))

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

Playstore_Applications = Playstore_Applications.withColumn("Rating", Playstore_Applications["Rating"].cast(FloatType()))


#Changing type of Installs to integer but before that remoivng the "+" sign at the end from it.
# the [^0-9] expression is used to find any character that is NOT a digit and then we'll remove it.
Playstore_Applications = Playstore_Applications.withColumnRenamed("Installs" , "No_Installs").withColumn("No_Installs" , \
    regexp_replace(col("No_Installs") , "[^0-9]" , "")).withColumn("No_Installs", col("No_Installs").cast(IntegerType()))


#Changing type of Price from string to integer but before that removing the dollar sign from paid applications.
Playstore_Applications = Playstore_Applications.withColumn("Price" , regexp_replace(col("Price") \
     , "[$]" , "")).withColumn("Price", col("Price").cast(IntegerType()))


#Renaming the Last Updated column and changing it's datatype to date.
Playstore_Applications = Playstore_Applications.withColumnRenamed("Last Updated" , "Last_Updated").withColumn("Last_Updated" , \
    to_date("Last_Updated" , "MMM d, yyyy"))


#Renaming the Android Ver table to remove spaces.
Playstore_Applications = Playstore_Applications.withColumnRenamed("Android Ver" , "Android_Version")


#Renaming the Current Ver table to remove spaces.
Playstore_Applications = Playstore_Applications.withColumnRenamed("Current Ver" , "Current_Version")




#Replacing null values from price column with 0.
Playstore_Applications = Playstore_Applications.fillna(0 , "Price")



#Reviewing the new schema:
Playstore_Applications.printSchema()
"""
root
 |-- App: string (nullable = true)
 |-- Category: string (nullable = true)
 |-- Rating: float (nullable = true)
 |-- No_Reviews: integer (nullable = true)
 |-- No_Installs: integer (nullable = true)
 |-- Price: integer (nullable = true)
 |-- Last_Updated: date (nullable = true)
 |-- Current_Version: string (nullable = true)
 |-- Android_Version: string (nullable = true)
 """



#Creating a view named "Applications":
Playstore_Applications.createOrReplaceTempView("Applications")




#Checking the total number of rows in our data currently:
print(Playstore_Applications.count())
#9365



#Viewing our data:
Playstore_Applications.show()
"""
+--------------------+--------------+------+----------+-----------+-----+------------+------------------+---------------+
|                 App|      Category|Rating|No_Reviews|No_Installs|Price|Last_Updated|   Current_Version|Android_Version|
+--------------------+--------------+------+----------+-----------+-----+------------+------------------+---------------+
|Photo Editor & Ca...|ART_AND_DESIGN|   4.1|       159|      10000|    0|  2018-01-07|             1.0.0|   4.0.3 and up|
| Coloring book moana|ART_AND_DESIGN|   3.9|       967|     500000|    0|  2018-01-15|             2.0.0|   4.0.3 and up|
|U Launcher Lite û...|ART_AND_DESIGN|   4.7|     87510|    5000000|    0|  2018-08-01|             1.2.4|   4.0.3 and up|
|Sketch - Draw & P...|ART_AND_DESIGN|   4.5|    215644|   50000000|    0|  2018-06-08|Varies with device|     4.2 and up|
|Pixel Draw - Numb...|ART_AND_DESIGN|   4.3|       967|     100000|    0|  2018-06-20|               1.1|     4.4 and up|
|Paper flowers ins...|ART_AND_DESIGN|   4.4|       167|      50000|    0|  2017-03-26|               1.0|     2.3 and up|
|Smoke Effect Phot...|ART_AND_DESIGN|   3.8|       178|      50000|    0|  2018-04-26|               1.1|   4.0.3 and up|
|    Infinite Painter|ART_AND_DESIGN|   4.1|     36815|    1000000|    0|  2018-06-14|          6.1.61.1|     4.2 and up|
|Garden Coloring Book|ART_AND_DESIGN|   4.4|     13791|    1000000|    0|  2017-09-20|             2.9.2|     3.0 and up|
|Kids Paint Free -...|ART_AND_DESIGN|   4.7|       121|      10000|    0|  2018-07-03|               2.8|   4.0.3 and up|
|Text on Photo - F...|ART_AND_DESIGN|   4.4|     13880|    1000000|    0|  2017-10-27|             1.0.4|     4.1 and up|
|Name Art Photo Ed...|ART_AND_DESIGN|   4.4|      8788|    1000000|    0|  2018-07-31|            1.0.15|     4.0 and up|
|Tattoo Name On My...|ART_AND_DESIGN|   4.2|     44829|   10000000|    0|  2018-04-02|               3.8|     4.1 and up|
|Mandala Coloring ...|ART_AND_DESIGN|   4.6|      4326|     100000|    0|  2018-06-26|             1.0.4|     4.4 and up|
|3D Color Pixel by...|ART_AND_DESIGN|   4.4|      1518|     100000|    0|  2018-08-03|             1.2.3|     2.3 and up|
|Learn To Draw Kaw...|ART_AND_DESIGN|   3.2|        55|       5000|    0|  2018-06-06|               NaN|     4.2 and up|
|Photo Designer - ...|ART_AND_DESIGN|   4.7|      3632|     500000|    0|  2018-07-31|               3.1|     4.1 and up|
|350 Diy Room Deco...|ART_AND_DESIGN|   4.5|        27|      10000|    0|  2017-11-07|               1.0|     2.3 and up|
|FlipaClip - Carto...|ART_AND_DESIGN|   4.3|    194216|    5000000|    0|  2018-08-03|             2.2.5|   4.0.3 and up|
|        ibis Paint X|ART_AND_DESIGN|   4.6|    224399|   10000000|    0|  2018-07-30|             5.5.4|     4.1 and up|
+--------------------+--------------+------+----------+-----------+-----+------------+------------------+---------------+
only showing top 20 rows
"""


# Now to check whether there are any duplicates or not?
spark.sql("SELECT App, Category, Rating, No_Reviews, No_Installs, Price , Android_Version , count(*) AS Duplicates \
FROM Applications \
GROUP BY App , Category , Rating , No_Reviews , No_Installs , Price , Android_Version \
HAVING count(*) > 1 \
ORDER BY Duplicates").show()

'''
+--------------------+-------------+------+----------+-----------+-----+------------------+----------+
|                 App|     Category|Rating|No_Reviews|No_Installs|Price|   Android_Version|Duplicates|
+--------------------+-------------+------+----------+-----------+-----+------------------+----------+
|        Google Voice|COMMUNICATION|   4.2|    171031|   10000000|    0|Varies with device|         2|
|Viki: Asian TV Dr...|ENTERTAINMENT|   4.3|    407719|   10000000|    0|Varies with device|         2|
| Nighty Night Circus|       FAMILY|   4.3|       382|      10000|    2|        2.3 and up|         2|
|TickTick: To Do L...| PRODUCTIVITY|   4.6|     25370|    1000000|    0|Varies with device|         2|
|Kids Balloon Pop ...|       FAMILY|   4.1|     38021|   10000000|    0|      4.0.3 and up|         2|
|Cardiac diagnosis...|      MEDICAL|   4.4|         8|        100|   12|        3.0 and up|         2|
|Disney Magic King...|       FAMILY|   4.3|    472584|   10000000|    0|      4.0.3 and up|         2|
|Messenger û Text ...|COMMUNICATION|   4.0|  56642847| 1000000000|    0|Varies with device|         2|
|BBW Dating & Plus...|       DATING|   4.4|     12632|    1000000|    0|        4.1 and up|         2|
|Notepad & To do list| PRODUCTIVITY|   4.3|    226295|   10000000|    0|      2.3.3 and up|         2|
|Any.do: To-do lis...| PRODUCTIVITY|   4.5|    298854|   10000000|    0|Varies with device|         2|
|                 NFL|       SPORTS|   4.1|    459795|   50000000|    0|Varies with device|         2|
|     EMT Review Plus|      MEDICAL|   4.5|       199|      10000|   11|       4.4W and up|         2|
|Super Hearing Sup...|      MEDICAL|   4.1|        21|       1000|    0|        4.1 and up|         2|
|               Gmail|COMMUNICATION|   4.3|   4604324| 1000000000|    0|Varies with device|         2|
|Simple - Better B...|      FINANCE|   4.4|      7731|     100000|    0|        5.0 and up|         2|
|BP Journal - Bloo...|      MEDICAL|   5.0|         6|       1000|    0|        4.4 and up|         2|
|Crew - Free Messa...|     BUSINESS|   4.6|      4159|     500000|    0|      4.0.3 and up|         2|
|imo free video ca...|COMMUNICATION|   4.3|   4785892|  500000000|    0|        4.0 and up|         2|
|Casual Dating & A...|       DATING|   4.5|     61637|    5000000|    0|        4.1 and up|         2|
+--------------------+-------------+------+----------+-----------+-----+------------------+----------+
only showing top 20 rows
'''


#Since we can see that there are duplicates present but there are two kinds of duplicates one with different android version's
#And other are completely same.
spark.sql("SELECT App, Category , Rating , No_Reviews , No_Installs , Price , Android_Version \
    FROM Applications \
    WHERE App = 'Telemundo Now' ").show()

'''
+-------------+-------------+------+----------+-----------+-----+---------------+
|          App|     Category|Rating|No_Reviews|No_Installs|Price|Android_Version|
+-------------+-------------+------+----------+-----------+-----+---------------+
|Telemundo Now|ENTERTAINMENT|   3.9|      8674|    1000000|    0|     4.4 and up|
|Telemundo Now|ENTERTAINMENT|   3.9|      8674|    1000000|    0|     4.4 and up|
+-------------+-------------+------+----------+-----------+-----+---------------+
'''


#Checking the total number of rows in our data currently:
print(Playstore_Applications.count())


#Since we can see that there are some duplicates present with only difference they have is of the Android version.
#We'll only keep the latest version data and remove every other data.
Applications = spark.sql("WITH Application AS \
    (SELECT App, Category , Rating, No_Reviews, No_installs, Price, last_updated, current_version, android_version, \
            ROW_NUMBER() OVER (PARTITION BY app ORDER BY current_version DESC, last_updated DESC, No_Reviews DESC) AS ranking \
            FROM Applications) \
    SELECT app, Category, rating, No_Reviews, No_installs, price, Last_Updated, Current_Version, Android_Version \
    FROM Application \
    WHERE ranking = 1")


#Verifying for the exact duplicates by calling out all the completly duplicate rows:
Applications.createOrReplaceTempView("Applications")
spark.sql(" SELECT App, rating, No_reviews, No_installs, price, android_version, count(*) AS Duplicates \
            FROM Applications \
            GROUP BY app, category, rating, No_reviews, No_installs, price, android_version \
            HAVING count(*) > 1").show()

"""
+---+------+----------+-----------+-----+---------------+----------+
|App|rating|No_reviews|No_installs|price|android_version|Duplicates|
+---+------+----------+-----------+-----+---------------+----------+
+---+------+----------+-----------+-----+---------------+----------+
"""



#Checking to verify the total number of rows in our data currently:
print(Applications.count())
#8195


#TASK IN HAND:
#1. Dead Applications : Appliations that haven't beed updated in 5 year, considering the dataset was updated last at year 2018.
#2. Most demanding category : Comparison between the different category by number of installations per category.
#3. Need of Improvement: Category of application where good applications are yet to be created.


#1. Dead Applications:
# Checking Applications where no improvement has been made in a year considering the current date to be 31 July, 2018.
# Basically comparing if Last_Updated < 31 July, 2013.

spark.sql("SELECT * FROM Applications \
           WHERE Last_Updated NOT BETWEEN CAST('JUL 31,2013' AS date) AND CAST('JUL 31, 2019' AS date)").show()





#2. Most demanding category:
spark.sql("SELECT Category, SUM(No_Installs) FROM Applications \
           WHERE Category != '1.9'\
           GROUP BY Category \
           ORDER BY SUM(No_Installs) DESC ").show()

"""
+-------------------+----------------+
|           Category|sum(No_Installs)|
+-------------------+----------------+
|               GAME|     13452762717|
|      COMMUNICATION|     11039241530|
|              TOOLS|      8100724500|
|             FAMILY|      6215979590|
|       PRODUCTIVITY|      5793070180|
|             SOCIAL|      5487841475|
|        PHOTOGRAPHY|      4649143130|
|      VIDEO_PLAYERS|      3931797200|
|   TRAVEL_AND_LOCAL|      2894859300|
| NEWS_AND_MAGAZINES|      2369110650|
|BOOKS_AND_REFERENCE|      1665791655|
|    PERSONALIZATION|      1532352930|
|           SHOPPING|      1400331540|
| HEALTH_AND_FITNESS|      1144006220|
|             SPORTS|      1096431465|
|      ENTERTAINMENT|       989660000|
|           BUSINESS|       692018120|
|          LIFESTYLE|       503741120|
|MAPS_AND_NAVIGATION|       503267560|
|            FINANCE|       455312400|
+-------------------+----------------+
only showing top 20 rows
"""




#3. Need od Improvement:
spark.sql("SELECT Category, round(AVG(Rating),3) FROM Applications \
           WHERE Category != '1.9' \
           GROUP BY Category \
           ORDER BY AVG(Rating)").show()

'''
+-------------------+-------------------------------------+
|           Category|round(avg(CAST(Rating AS DOUBLE)), 3)|
+-------------------+-------------------------------------+
|             DATING|                                 3.98|
|MAPS_AND_NAVIGATION|                                4.036|
|              TOOLS|                                4.041|
|      VIDEO_PLAYERS|                                4.045|
|   TRAVEL_AND_LOCAL|                                 4.07|
|          LIFESTYLE|                                4.093|
|           BUSINESS|                                4.097|
|            FINANCE|                                4.116|
|      COMMUNICATION|                                4.121|
| NEWS_AND_MAGAZINES|                                4.122|
|      ENTERTAINMENT|                                 4.13|
|     HOUSE_AND_HOME|                                4.141|
|        PHOTOGRAPHY|                                4.157|
|            MEDICAL|                                4.168|
|     FOOD_AND_DRINK|                                4.171|
| LIBRARIES_AND_DEMO|                                4.179|
|             COMICS|                                4.181|
|             FAMILY|                                4.183|
|       PRODUCTIVITY|                                4.183|
|  AUTO_AND_VEHICLES|                                 4.19|
+-------------------+-------------------------------------+
only showing top 20 rows
'''


#4. Applications average rating with respect to their android versions.

spark.sql("SELECT android_version, ROUND(AVG(rating) , 1) \
           FROM Applications \
           GROUP BY android_version \
           ORDER BY AVG(rating)").show()


'''
+---------------+-------------------------------------+
|android_version|round(avg(CAST(rating AS DOUBLE)), 1)|
+---------------+-------------------------------------+
|     6.0 and up|                                  3.9|
|     4.3 and up|                                  4.0|
|     3.1 and up|                                  4.0|
|     7.1 and up|                                  4.0|
|     2.2 and up|                                  4.0|
|     3.2 and up|                                  4.0|
|     1.5 and up|                                  4.0|
|   2.3.3 and up|                                  4.0|
|     1.0 and up|                                  4.0|
|     1.6 and up|                                  4.1|
|     2.3 and up|                                  4.1|
|     8.0 and up|                                  4.1|
|     3.0 and up|                                  4.1|
|     7.0 and up|                                  4.1|
|     4.4 and up|                                  4.1|
|     5.0 and up|                                  4.1|
|     2.1 and up|                                  4.1|
|  4.0.3 - 7.1.1|                                  4.2|
|     4.0 and up|                                  4.2|
|     4.2 and up|                                  4.2|
+---------------+-------------------------------------+
only showing top 20 rows
'''


# 5. How many applications in each category of playstore.

spark.sql("SELECT Category, count(Category) \
           FROM applications \
           GROUP BY Category \
           ORDER BY COUNT(Category)").show()

'''
+-------------------+---------------+
|           Category|count(Category)|
+-------------------+---------------+
|             BEAUTY|             42|
|             EVENTS|             45|
|          PARENTING|             50|
|             COMICS|             54|
|     ART_AND_DESIGN|             59|
|     HOUSE_AND_HOME|             61|
| LIBRARIES_AND_DEMO|             63|
|            WEATHER|             72|
|  AUTO_AND_VEHICLES|             73|
|      ENTERTAINMENT|             87|
|     FOOD_AND_DRINK|             94|
|          EDUCATION|            107|
|MAPS_AND_NAVIGATION|            118|
|             DATING|            133|
|      VIDEO_PLAYERS|            149|
|BOOKS_AND_REFERENCE|            169|
|           SHOPPING|            180|
|   TRAVEL_AND_LOCAL|            187|
|             SOCIAL|            203|
| NEWS_AND_MAGAZINES|            204|
+-------------------+---------------+
only showing top 20 rows
'''

spark.sql("SELECT Category, count(Category) \
           FROM applications \
           GROUP BY Category \
           ORDER BY COUNT(Category) DESC").show()


'''
+-------------------+---------------+
|           Category|count(Category)|
+-------------------+---------------+
|             FAMILY|           1648|
|               GAME|            899|
|              TOOLS|            720|
|            FINANCE|            302|
|          LIFESTYLE|            301|
|       PRODUCTIVITY|            301|
|    PERSONALIZATION|            298|
|            MEDICAL|            290|
|        PHOTOGRAPHY|            263|
|           BUSINESS|            262|
|             SPORTS|            260|
|      COMMUNICATION|            257|
| HEALTH_AND_FITNESS|            244|
| NEWS_AND_MAGAZINES|            204|
|             SOCIAL|            203|
|   TRAVEL_AND_LOCAL|            187|
|           SHOPPING|            180|
|BOOKS_AND_REFERENCE|            169|
|      VIDEO_PLAYERS|            149|
|             DATING|            133|
+-------------------+---------------+
only showing top 20 rows
'''

