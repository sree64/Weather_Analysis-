 package com.att


import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType,StructField}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object weather_data extends Serializable {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("WeatherReport")
    val spark = new SparkContext(sparkConf)
    val sqlcon = SparkSession.builder().getOrCreate()

    //Defining Schema
    def weather_schema(columnNames: Seq[String]): StructType ={ StructType(Seq(
      StructField("year",StringType, true),
      StructField("month",StringType , true),
      StructField("day",StringType , true),
      StructField("time",StringType , true),
      StructField("Airtemp",StringType , true),
      StructField("Dewpoint",StringType , true),
      StructField("sealevel",StringType , true),
      StructField("winddirection",StringType , true),
      StructField("windspeedrate",StringType , true),
      StructField("skycondition",StringType , true),
      StructField("precip_1",StringType , true),
      StructField("precp_6",StringType , true)
    ))}


    def createList(input : String) :List[String] ={
    val schemaArray = new Array[String](12)
      schemaArray(0)=input.substring(0,4)
      schemaArray(1)=input.substring(5,7)
      schemaArray(2)=input.substring(8,10)
      schemaArray(3)=input.substring(11,13)
      schemaArray(4)=input.substring(14,19)
      schemaArray(5)=input.substring(20,25)
      schemaArray(6)=input.substring(26,31)
      schemaArray(7)=input.substring(32,37)
      schemaArray(8)=input.substring(38,43)
      schemaArray(9)=input.substring(44,49)
      schemaArray(10)=input.substring(50,55)
      schemaArray(11)=input.substring(56,61)
      schemaArray.toList

    }
    def row(line: Seq[String]): Row = Row(line(0), line(1), line(2), line(3), line(4), line(5),line(6), line(7), line(8), line(9), line(10),line(11))

//preloaded couple of weather station data in to hdfs .
    import sqlcon.sqlContext.implicits._
    val weather_data: RDD[String]= spark.textFile("/user/srm2449/recon_result/012370-99999-2016.gz")
    val schema = weather_schema(Seq("year","month","day","time","Airtemp","Dewpoint","sealevel","winddirection","windspeedrate","skycondition","precip_1","precp_6"))
    val data = weather_data.map(x => createList(x)).map(row)
    val Weatherdf = sqlcon.createDataFrame(data, schema)

    Weatherdf.createOrReplaceTempView("2016_Weather_temp_view");
    val maxtemp = sqlcon.sql("SELECT year,month,day, time, Airtemp FROM 2016_Weather_temp_view  WHERE Airtemp IN ( SELECT MAX( CAST(Airtemp AS FLOAT)) from 2016_Weather_temp_view )")
//Printing Max temperature for a given city
    maxtemp.show()

    val Mintemp = sqlcon.sql(" SELECT year,month,day, time, Airtemp FROM 2016_Weather_temp_view  WHERE Airtemp IN (SELECT MIN(CAST(Airtemp AS FLOAT)) from 2016_Weather_temp_view) ")
   //printing Minimum temperature for a given city
    Mintemp.show()
// Getting max and Min temparatures for all Weather station reported.
    val data1 = spark.wholeTextFiles("/user/srm2449/recon_result/weather/*")
    val dataDF = data1.toDF()
    dataDF.select("_1").show(false)

    //Getting max and Min of a particular station for all Years of reported Data
    val data2 = spark.wholeTextFiles("/user/srm2449/recon_result/weather/*/012370-99999-2016.gz")
    //Applying the same logic of applying teh same schema and filtering stations which did not report data for temparatures




  }
}

