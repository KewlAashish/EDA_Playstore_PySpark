from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date, isnan 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,  DoubleType , FloatType
from pyspark import SparkConf,SparkContext

#CREATING A SPARK SESSION :
spark = SparkSession.builder.appName("Playstore_Applications").getOrCreate()

# LOADING THE DATA SOURCE IN SPARK:
#The file used is a csv file, hence the format is 'csv', also the dataset has a heading that's why the header is True. 
#The dataset contains applications with double quotation mark in them and hence we have escape set to '"'.
Playstore_Applications = spark.read.load("C:/SparkCourse/SparkCourse/googleplaystore.csv" , format = 'csv' , escape = '"' ,\
 sep = "," , header = "true" , inferschema = "true" , encoding = "utf-8")


#Removing the null values:
Playstore_Applications = Playstore_Applications.dropna(how='any')


#Printing the schema:
Playstore_Applications.printSchema()


print(Playstore_Applications.count())



#Checking out the dataframe:
Playstore_Applications.show()

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


#Removing rows with NaN values in the rating section:
#Playstore_Applications = Playstore_Applications.filter(isnan(col("Rating")))


#Replacing null values from price column with 0.
Playstore_Applications = Playstore_Applications.fillna(0 , "Price")


#Reviewing the new schema:
Playstore_Applications.printSchema()


#Creating a view named "Applications":
Playstore_Applications.createOrReplaceTempView("Applications")


#Checking the total number of rows in our data currently:
print(Playstore_Applications.count())


#Viewing our data:
Playstore_Applications.show()


# Now to check whether there are any duplicates or not?
spark.sql("SELECT App, Category, Rating, No_Reviews, No_Installs, Price , Android_Version , count(*) AS Duplicates \
FROM Applications \
GROUP BY App , Category , Rating , No_Reviews , No_Installs , Price , Android_Version \
HAVING count(*) > 1 \
ORDER BY Duplicates").show()


#Since we can see that there are duplicates present but there are two kinds of duplicates one with different android version's
#And other are completely same.
spark.sql("SELECT App, Category , Rating , No_Reviews , No_Installs , Price , Android_Version \
    FROM Applications \
    WHERE App = 'Telemundo Now' ").show()


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



#Checking to verify the total number of rows in our data currently:
print(Applications.count())


#TASK IN HAND:
#1. Dead Applications : Appliations that haven't beed updated in 5 year, considering the dataset was updated last at year 2018.
#2. Most demanding category : Comparison between the different category by number of installations per category.
#3. Need of Improvement: Category of application where good applications are yet to be created.


#1. Dead Applications:
# Checking Applications where no improvement has been made in a year considering the current date to be 31 July, 2018.
# Basically comparing if Last_Updated < 31 July, 2013.

spark.sql("SELECT * FROM Applications \
           WHERE Last_Updated NOT BETWEEN CAST('July 31,2013' AS date) AND CAST('July 31, 2019' AS date)").show()


#2. Most demanding category:
spark.sql("SELECT Category, SUM(No_Installs) FROM Applications \
           WHERE Category != '1.9'\
           GROUP BY Category \
           ORDER BY SUM(No_Installs) DESC ").show()


    
#3. Need od Improvement:
spark.sql("SELECT Category, AVG(Rating) FROM Applications \
           WHERE Category != '1.9' \
           GROUP BY Category \
           ORDER BY AVG(Rating)").show()







