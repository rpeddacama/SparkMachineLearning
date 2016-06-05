package com.rupendra.nyc

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.sql.SQLContext

object MotorVehicleCollisions {

  //Define the case class with the features that we want to load in the data
  case class Collision(ID: String, DATE: String, TIME: String, BOROUGH: String, LATITUDE: String, LONGITUDE: String, 
		ON_STREET_NAME: String, NUMBER_OF_PERSONS_INJURED: Int, NUMBER_OF_PERSONS_KILLED: Int,
      NUMBER_OF_PEDESTRIANS_INJURED: Int, NUMBER_OF_PEDESTRIANS_KILLED: Int,
      NUMBER_OF_CYCLIST_INJURED: Int, NUMBER_OF_CYCLIST_KILLED: Int,
      NUMBER_OF_MOTORIST_INJURED: Int, NUMBER_OF_MOTORIST_KILLED: Int, 
      CONTRIBUTING_FACTOR_VEHICLE_1: String, 
      CONTRIBUTING_FACTOR_VEHICLE_2: String, 
      CONTRIBUTING_FACTOR_VEHICLE_3: String, 
      VEHICLE_TYPE_CODE_1: String,
      VEHICLE_TYPE_CODE_2: String,
      VEHICLE_TYPE_CODE_3: String
		)
	

		def main(args: Array[String]) {
    
      System.setProperty("hadoop.home.dir", "c:\\winutils\\");

      val conf = new SparkConf()
	          .setAppName("NYPD_Motor_Vehicle_Collisions")
	          .setMaster("local")
		  val sc = new SparkContext(conf)
	    sc.setLogLevel("WARN")
	    val sqlContext = new SQLContext(sc)

		  import sqlContext.implicits._
	    import sqlContext._

			
      import org.apache.spark.sql.SQLContext
      import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

      //define custom schema with the data types for loading the data from csv file
      val customSchema = StructType(Array(
        StructField("ID", StringType, true),
        StructField("DATE", StringType, true),
        StructField("TIME", StringType, true),
        StructField("BOROUGH", StringType, true),
        StructField("LATITUDE", StringType, true),
        StructField("LONGITUDE", StringType, true),
        StructField("ON_STREET_NAME", StringType, true),
        StructField("NUMBER_OF_PERSONS_INJURED", IntegerType, true),
        StructField("NUMBER_OF_PERSONS_KILLED", IntegerType, true),
        StructField("NUMBER_OF_PEDESTRIANS_INJURED", IntegerType, true),
        StructField("NUMBER_OF_PEDESTRIANS_KILLED", IntegerType, true),
        StructField("NUMBER_OF_CYCLIST_INJURED", IntegerType, true),
        StructField("NUMBER_OF_CYCLIST_KILLED", IntegerType, true),
        StructField("NUMBER_OF_MOTORIST_INJURED", IntegerType, true),
        StructField("NUMBER_OF_MOTORIST_KILLED", IntegerType, true),
        StructField("CONTRIBUTING_FACTOR_VEHICLE_1", StringType, true),
        StructField("CONTRIBUTING_FACTOR_VEHICLE_2", StringType, true),
        StructField("CONTRIBUTING_FACTOR_VEHICLE_3", StringType, true),
        StructField("VEHICLE_TYPE_CODE_1", StringType, true),
        StructField("VEHICLE_TYPE_CODE_2", StringType, true),
        StructField("VEHICLE_TYPE_CODE_3", StringType, true)))
    
      val df = sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true") // Use first line of all files as header
          .schema(customSchema)
          .load("C:\\temp\\fulldata.csv")
    
      //Register a temp table to analyze data
      df.registerTempTable("collisions")


  		import org.apache.spark.sql.functions._
  		//quick check to verify data is loaded 
			val fatalCollCountsql = sqlContext.sql("select BOROUGH, count(*) as total  FROM collisions "+
			    "WHERE NUMBER_OF_PERSONS_KILLED > 0 "+
			    "GROUP BY BOROUGH "+
					"ORDER BY total DESC LIMIT 50")

			fatalCollCountsql.show()
					
			//dataframe is converted to RDD and cached to extract feature vectors needed
			//for building the decision tree model
			val collisionsRDD: RDD[org.apache.spark.sql.Row] = df.rdd
			collisionsRDD.cache()

			var boroughMap: Map[String, Int] = Map()
      var index: Int = 0
      collisionsRDD.map(row => Collision(row.getString(0),row.getString(1),row.getString(2),row.getString(3),row.getString(4),
              row.getString(5),row.getString(6),
              row.getInt(7),row.getInt(8),
              row.getInt(9),row.getInt(10),
              row.getInt(11),row.getInt(12),
              row.getInt(13),row.getInt(14),
              row.getString(15),row.getString(16),
              row.getString(17),row.getString(18),
              row.getString(19),row.getString(20)).BOROUGH)
              .distinct.collect.foreach(count => { boroughMap += (count -> index); index += 1 })
              

      var timeMap: Map[String, Int] = Map()
      var index1: Int = 0
      collisionsRDD.map(row => Collision(row.getString(0),row.getString(1),row.getString(2),row.getString(3),row.getString(4),
              row.getString(5),row.getString(6),
              row.getInt(7),row.getInt(8),
              row.getInt(9),row.getInt(10),
              row.getInt(11),row.getInt(12),
              row.getInt(13),row.getInt(14),
              row.getString(15),row.getString(16),
              row.getString(17),row.getString(18),
              row.getString(19),row.getString(20)).TIME)
              .distinct.collect.foreach(count => { timeMap += (count -> index1); index1 += 1 })
              
        
      var streetMap: Map[String, Int] = Map()
      var index2: Int = 0
      collisionsRDD.map(row => Collision(row.getString(0),row.getString(1),row.getString(2),row.getString(3),row.getString(4),
              row.getString(5),row.getString(6),
              row.getInt(7),row.getInt(8),
              row.getInt(9),row.getInt(10),
              row.getInt(11),row.getInt(12),
              row.getInt(13),row.getInt(14),
              row.getString(15),row.getString(16),
              row.getString(17),row.getString(18),
              row.getString(19),row.getString(20)).ON_STREET_NAME)
              .distinct.collect.foreach(count => { streetMap += (count -> index2); index2 += 1 })
              
					
      //Define the features array using the feature vectors created above
      val mlprep = collisionsRDD.map(row => {
      val time = timeMap(row.getString(2))
      val street = streetMap(row.getString(6))
      val borough = boroughMap(row.getString(3))

      val fatal = if (row.getInt(8) > 0) 1.0 else 0.0
        Array(fatal.toDouble, time.toDouble, street.toDouble, borough.toDouble)
      })
    
    
      //Build LabeledPoint with features used for training the model
      val mldata = mlprep.map(x => LabeledPoint(x(0), Vectors.dense(x(1), x(2), x(3))))

      //Splitting data 15% - 85% for collisions with and without fatalities
      val mldata0 = mldata.filter(x => x.label == 0).randomSplit(Array(0.85, 0.15))(1)
      //Data with fatalities
      val mldata1 = mldata.filter(x => x.label != 0)
      //mixed data
      val mldata2 = mldata0 ++ mldata1
    
      //split the mixed data into training data (70%) and test data (30%)
      val splits = mldata2.randomSplit(Array(0.7, 0.3))
      val (trainingData, testData) = (splits(0), splits(1))
    
  
      var categoricalFeaturesInfo = Map[Int, Int]()
      categoricalFeaturesInfo += (0 -> timeMap.size)
      categoricalFeaturesInfo += (1 -> streetMap.size)
      categoricalFeaturesInfo += (2 -> boroughMap.size)

      //Setting the values for Spark Decision tree model
      val numClasses = 2
      val impurity = "gini"
      val maxDepth = 9
      //Make sure maxbins is atleast as big as the biggest feature vector
      val maxBins = 8500

      //Build the model with training data
      val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
        impurity, maxDepth, maxBins)
      
      // Evaluate model on test data and compute test error
      val labelAndPreds = testData.map { point =>
        val prediction = model.predict(point.features)
          (point.label, prediction)
      }

      val wrongPrediction =(labelAndPreds.filter{
        case (label, prediction) => ( label !=prediction) 
      })

      println("Number of wrong predictions : "+wrongPrediction.count())

      val ratioWrong=wrongPrediction.count().toDouble/testData.count()
      println("Wrong prediction ratio : "+ratioWrong)

      val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
      println("Test Error = " + testErr)
	  }

}
