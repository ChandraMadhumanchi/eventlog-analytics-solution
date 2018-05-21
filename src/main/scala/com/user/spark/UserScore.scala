package com.user.spark

import java.sql.Date

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.when
import org.apache.commons.logging.LogFactory

/** Provides a service as described.
  /** 
   *
   *  @ it accepts the input dataFrame
   *  @ It returns the output dataFrame
   */
  def markMostRecent(dataframe: DataFrame): DataFrame = {}
 
  * /** 
   *
   *  @it accepts the 2 input dataFrame and primary key columns
   *  @It returns the output dataFrame
   */
  def findDelta(dataset1: DataFrame, dataset2: DataFrame,primaryKey: Seq[String]): DataFrame = {}
  * 
  */

object UserScore {

  private val LOGGER = LogFactory.getLog(UserScore.getClass);

  def main(args: Array[String]) = {

    case class transaction(user_id: Long, date: Date, price: Int)
    var sparkSession: SparkSession = null
    try {
      // Create Spark Session
      sparkSession = SparkSession.builder.master("local").appName("eventlog-analysis").getOrCreate()

      
     //  Here I have done some sanity test.
      
      val dataset1 = sparkSession.createDataFrame(Seq(
        (1, "2018/05/14", 10),
        (2, "2018/05/14", 10),
        (3, "2018/05/17", 10),
        (5, "2018/05/17", 10),
        (1, "2018/05/15", 20),
        (2, "2018/05/15", 20),
        (3, "2018/05/16", 20),
        (5, "2018/05/16", 20),
        (3, "2018/05/17", 100),
        (1, "2018/05/16", 100),
        (2, "2018/05/17", 100),
        (5, "2018/05/17", 150),
        (5, "2018/05/16", 200),
        (3, "2018/05/16", 300),
        (1, "2018/05/17", 300),
        (2, "2018/05/17", 250),
        (3, "2018/05/17", 5))).toDF("user_id", "date", "score")

      val dataset2 = sparkSession.createDataFrame(Seq(
        (2, "2018/05/14", 10),
        (3, "2018/05/17", 10),
        (4, "2018/05/17", 10),
        (2, "2018/05/15", 20),
        (3, "2018/05/16", 20),
        (4, "2018/05/17", 20),
        (5, "2018/05/16", 20),
        (3, "2018/05/17", 100),
        (4, "2018/05/17", 100),
        (2, "2018/05/17", 100),
        (5, "2018/05/17", 150),
        (5, "2018/05/16", 200),
        (3, "2018/05/16", 300),
        (2, "2018/05/17", 250),
        (3, "2018/05/17", 5))).toDF("user_id", "date", "score")

          
        // Task 1 most recent log
       val markMostRecentDF = markMostRecent(dataset1)
       markMostRecentDF.show()

       // Task 2 produce a “diff” for the datasets
       val primaryKey = Seq("user_id", "date")
       val findDeltaDF = findDelta(dataset1, dataset2,primaryKey)
        findDeltaDF.show()

        //testsqlWay(sparkSession)

      sparkSession.stop()

    } catch {
      case e: Throwable =>
        LOGGER.error(e, e)
        throw e
    } finally {
      if (sparkSession != null) {
        LOGGER.info("Stopping spark session...")
        sparkSession.stop()
      }
    }

  }

 /** Provides a service as described.
	given a dataset of “User Score” event log. 
	implement function which marks the most recent entries for each user as “most_recent”. 
  */
  
  def markMostRecent(dataframe: DataFrame): DataFrame = {

    // validations
    require(dataframe != null, "No dataset supplied")
    require(dataframe.columns.length != 0, "Dataset with no columns cannot be converted")
    val errorList = verifyCols(dataframe, Array("user_id","date","score"))
    if (errorList.nonEmpty) {
      throw new NoSuchElementException
    }
    // date conversion
    val ts = to_date(unix_timestamp(dataframe("date"), "yyyy/MM/dd").cast("timestamp"))
    val df = dataframe.withColumn("date", ts)
 
    // provide rank for most recent event
    val window = Window.partitionBy(col("user_id")).orderBy(col("date").desc, col("score").desc)
    df.withColumn("row", rank().over(window)).withColumn("most_recent",
      when(col("row") === 1, "most_recent").otherwise("")).drop(col("row"))

  }

  private def verifyCols(df: DataFrame, req: Array[String]): List[String] = {
    req.foldLeft(List[String]()) { (l, r) =>
      if (df.columns.contains(r)) l
      else {
        System.err.println(s"DataFrame does not contain specified column: $r")
        r :: l
      }
    }
  }
  
   def hasColumn(df: org.apache.spark.sql.DataFrame, colName: String) = df.columns.contains(colName)
   
  
  /** Provides a service as described.
 *
 given two datasets with arbitrary but equal schema and predefined primary key. 
 produce a “diff” for the datasets. Output must 
 contain all the rows that have different value and the rows that
 are missing in either of two datasets.

	Each output entry must be marked with appropriate “delta_type” column marker:
●	“update” when row is present in both datasets but values are different
●	"delete" when row is present in first dataset but absent in the second
●	"insert" when row is present in second dataset but absent in the first

 Data columns must contain data from:
-	dataset1 for deleted entries;
-	dataset2 for updated and inserted entries;

 */
  
  def findDelta(dataset1: DataFrame, dataset2: DataFrame,primaryKey: Seq[String]): DataFrame = {

    // validation for data frame 1
    require(dataset1 != null, "No dataset supplied")
    require(dataset1.columns.length != 0, "Dataset with no columns cannot be converted")
    val errorList = verifyCols(dataset1, Array("user_id","date","score"))
    if (errorList.nonEmpty) {
      throw new NoSuchElementException
    }
    
    // validation for data frame 2
    require(dataset2 != null, "No dataset supplied")
    require(dataset2.columns.length != 0, "Dataset with no columns cannot be converted")
    val errorList1 = verifyCols(dataset2, Array("user_id","date","score"))
    if (errorList1.nonEmpty) {
      throw new NoSuchElementException
    }
    
    val df1 = dataset1.withColumnRenamed("score", "score_x")  
    val df2 = dataset2.withColumnRenamed("score", "score_y")
    val df3 = df1.as("l").join(df2.as("r"), primaryKey, "fullouter")

    val joinDf = df3.withColumn("delta_type", when(col("score_y").isNull or col("score_y") === "", "delete")
      .when(col("score_x").isNull or col("score_x") === "", "insert")
      .when(col("score_x") =!= col("score_y"), "update")
      .otherwise("same"))

    val resultDf = joinDf.withColumn("score", when(col("delta_type") === "delete", col("score_x"))
      .when(col("delta_type") === "insert", col("score_y"))
      .when(col("delta_type") === "update", col("score_x") + col("score_y"))
      .otherwise(col("score_x"))).drop(col("score_y")).drop(col("score_x"))
      
    resultDf
    
  }
  

  /** Does simple SqlWay */
  
  private def testsqlWay(spark: SparkSession): Unit = {
    val df1 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/d1.csv")
    //df1.take(10)

    val df2 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/d2.csv")
    //df2.take(10)

    df1.createOrReplaceTempView("table1")
    val sqlDF1 = spark.sql("SELECT * FROM table1")
    //sqlDF1.show()

    df2.createOrReplaceTempView("table2")
    val sqlDF2 = spark.sql("SELECT * FROM table2")
    //sqlDF2.show()

    val sqlDF3 = spark.sql(" select (case when a.user_id is not  null and a.create_date is not null  then a.user_id else b.user_id  end) uuser_id ,(case when a.user_id is not  null and a.create_date is not null  then a.create_date else b.create_date  end) ccreate_date ,(case when a.user_id = b.user_id and a.create_date=b.create_date and a.score <> b.score then a.score + b.score when a.user_id is not  null and a.create_date is not null then a.score else b.score  end) sscore,(case when a.user_id = b.user_id and a.create_date=b.create_date and a.score <> b.score then 'Update' when a.user_id = b.user_id and a.create_date=b.create_date and a.score = b.score then 'Same' when a.user_id is null and a.create_date is null then 'Insert' when b.user_id is null and b.create_date is null then 'Delete'  end) rec_status from table1 a Full outer join table2 b on (a.user_id = b.user_id and a.create_date = b.create_date)")

    sqlDF3.show()
  
  }

}