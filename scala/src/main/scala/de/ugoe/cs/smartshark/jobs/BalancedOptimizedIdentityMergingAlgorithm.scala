package de.ugoe.cs.smartshark.jobs

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorIndexer }
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.commons.lang3.StringUtils
import de.ugoe.cs.smartshark.util.DBUtilFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.SparkContext._
import scala.reflect.runtime.universe

object algorithm3p {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Spark Scala Identity Merge Algorithm")
      .getOrCreate()

    import sparkSession.implicits._

    val extractLastName = udf((sentence: String) => sentence.split(" ").reverse.head)
    val extractFirstName = udf((sentence: String) => sentence.split(" ").head)
    val getConcatenated = udf((first: String, second: String, third: String) => { first + " " + second + " " + third })
    val intersectlen = udf((x: String, z: String) => (x intersect z).length())
    val jaroWinkler = udf((x: String, z: String) => StringUtils.getJaroWinklerDistance(x, z))
    val format = udf((x: Double) => "%.2f".format(x).toDouble)
    val check = udf((x: String, z: String) => x.contains(z))

    val r = scala.util.Random
    val mySeed = r.nextInt(10000)

    // load data from mongoDB using  DBUtils
    val dbUtils = DBUtilFactory.getDBUtils(sparkSession)
    val peo = dbUtils.loadData("people")
    peo.show(5)

    /*
     * from Algo3.csv, take a sample equally from true, false and uncertain label
     * and is saved as Balanced.csv
     */
    val labeleddata = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .load("data/Balanced.csv")
    val df = labeleddata.filter(labeleddata("Prefix").isNotNull && labeleddata("Firstname").isNotNull && labeleddata("Lastname").isNotNull &&
      labeleddata("Prefix_1").isNotNull && labeleddata("Firstname_1").isNotNull && labeleddata("Lastname_1").isNotNull)
      .where(!('Prefix like "%log%") &&
        !('Prefix like "%list%") &&
        !('Prefix like "%user%"))
      .withColumn("s_prefix", length($"Prefix"))
      .withColumn("s_firstname", length($"Firstname"))
      .withColumn("s_lastname", length($"Lastname"))
      .withColumn("s_prefix_1", length($"Prefix_1"))
      .withColumn("s_firstname_1", length($"Firstname_1"))
      .withColumn("s_lastname_1", length($"Lastname_1"))
      .withColumn("pmax", when($"s_prefix" >= $"s_prefix_1", col("s_prefix")).otherwise(col("s_prefix_1")))
      .withColumn("fmax", when($"s_firstname" >= $"s_firstname_1", col("s_firstname")).otherwise(col("s_firstname_1")))
      .withColumn("lmax", when($"s_lastname" >= $"s_lastname_1", col("s_lastname")).otherwise(col("s_lastname_1")))
      .withColumn("plen", col("s_prefix") + col("s_prefix_1"))
      .withColumn("flen", col("s_firstname") + col("s_firstname_1"))
      .withColumn("llen", col("s_lastname") + col("s_lastname_1"))
      
    /*
     * The same feature as algorithm3 
     * All to All comparisons using only JaroWinkler
     * and also a set of rules are created and checked
     * as features for the machine learning
     */ 

    val mydf = df.withColumn("f1", jaroWinkler(labeleddata("Prefix"), labeleddata("Prefix_1")))
      .withColumn("f2", jaroWinkler(labeleddata("Prefix"), labeleddata("Firstname_1")))
      .withColumn("f3", jaroWinkler(labeleddata("Prefix"), labeleddata("Lastname_1")))
      .withColumn("f4", jaroWinkler(labeleddata("Firstname"), labeleddata("Prefix_1")))
      .withColumn("f5", jaroWinkler(labeleddata("Firstname"), labeleddata("Firstname_1")))
      .withColumn("f6", jaroWinkler(labeleddata("Firstname"), labeleddata("Lastname_1")))
      .withColumn("f7", jaroWinkler(labeleddata("Lastname"), labeleddata("Prefix_1")))
      .withColumn("f8", jaroWinkler(labeleddata("Lastname"), labeleddata("Firstname_1")))
      .withColumn("f9", jaroWinkler(labeleddata("Lastname"), labeleddata("Lastname_1")))
      .withColumn("c1", labeleddata("Prefix_1") === (concat(labeleddata("Firstname"), labeleddata("Lastname"))))
      .withColumn("c3", labeleddata("Prefix_1") === (concat(labeleddata("Lastname"), labeleddata("Firstname"))))
      .withColumn("c2", labeleddata("Prefix") === (concat(labeleddata("Firstname_1"), labeleddata("Lastname_1"))))
      .withColumn("c4", labeleddata("Prefix") === (concat(labeleddata("Lastname_1"), labeleddata("Firstname_1"))))
      .withColumn("c5", concat(labeleddata("Lastname_1"), labeleddata("Firstname_1")) === concat(labeleddata("Lastname"), labeleddata("Firstname")))
      .withColumn("c6", concat(labeleddata("Lastname_1"), labeleddata("Firstname_1")) === concat(labeleddata("Firstname"), labeleddata("Lastname")))
      .withColumn("c7", concat(labeleddata("Lastname"), labeleddata("Firstname")) === concat(labeleddata("Firstname_1"), labeleddata("Lastname_1")))
      .drop("s_lastname_1", "s_lastname", "s_firstname_1", "s_firstname", "s_prefix_1", "s_prefix", "plen", "flen", "llen", "pmax", "fmax", "lmax")

    val assembler = new VectorAssembler()
      .setInputCols(Array("f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9",
        "c1", "c2", "c3", "c4", "c5", "c6", "c7"))
      .setOutputCol("features")

    val my_df = assembler.transform(mydf)

    val labelIndexer = new StringIndexer()
      .setInputCol("Label")
      .setOutputCol("indexedLabel")
      .fit(my_df)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(10)
      .fit(my_df)

    /*
     * data is divided into 2 parts
     * 67% for training and 33% for testing
     */  
    
    val splits = my_df.randomSplit(Array(0.67, 0.33), seed = mySeed)
    val (trainingData, testData) = (splits(0), splits(1))

    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    val model = pipeline.fit(trainingData)
    val predictions = model.transform(testData)
    //predictions.select("predictedLabel", "label", "features").show(500)

    predictions.createOrReplaceTempView("people")
    val predict = sparkSession.sql("SELECT Prefix, Prefix_1, Firstname, Firstname_1, Lastname, Lastname_1,predictedLabel, label FROM people WHERE predictedLabel !=  label")

    /* 
     * Confusion matrix is used to evaluate the algorithm performance
     * Println will print the misclassified records
     */
    
    println(predictions.filter($"predictedLabel" === "F" && $"label" === "F").count() + " FF\n" +
      predictions.filter($"predictedLabel" === "T" && $"label" === "F").count() + " TF\n" +
      predictions.filter($"predictedLabel" === "U" && $"label" === "F").count() + " UF\n" +
      predictions.filter($"predictedLabel" === "T" && $"label" === "T").count() + " TT\n" +
      predictions.filter($"predictedLabel" === "F" && $"label" === "T").count() + " FT\n" +
      predictions.filter($"predictedLabel" === "U" && $"label" === "T").count() + " UT\n" +
      predictions.filter($"predictedLabel" === "F" && $"label" === "U").count() + " FU\n" +
      predictions.filter($"predictedLabel" === "T" && $"label" === "U").count() + " TU\n" +
      predictions.filter($"predictedLabel" === "U" && $"label" === "U").count() + " UU\n")

    predict.show()
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

  }
}