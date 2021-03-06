package de.ugoe.cs.smartshark.jobs

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorIndexer }
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.commons.lang3.StringUtils
import de.ugoe.cs.smartshark.util.DBUtilFactory
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import scala.reflect.runtime.universe

/**
 * <p>
 * Identity Merge Algorithm, 
 * Broad Approach which finds duplicate developers from the people collection
 * Gradient Boosted Tree is used as a Machine Learning Algorithm
 * </p>
 * 
 * @author Atefeh Khajeh
 */

object algorithm2 {

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
     * Data Preprocessing
     * Create New Columns which contain the firstname, lastname, and prefix of email address of each developer 
     */
   val people = peo.withColumn("pre", split($"email", "@").getItem(0))
      .withColumn("Prefix", lower(regexp_replace($"pre", "[^a-zA-Z]", "")))
      .withColumn("Firstname", lower(regexp_replace(extractFirstName($"name"), "[^a-zA-Z]", "")))
      .withColumn("Lastname", lower(regexp_replace(extractLastName($"name"), "[^a-zA-Z]", "")))
      .drop("pre")
      .drop("username")
      .drop("name")
      .drop("email")
      .drop("_id")
      .filter(($"Prefix").isNotNull && ($"Firstname").isNotNull && ($"Lastname").isNotNull)

    val peop = people.withColumnRenamed("Prefix", "Prefix_1")
      .withColumnRenamed("Firstname", "Firstname_1")
      .withColumnRenamed("Lastname", "Lastname_1")

    /*
     * Find the potential duplicate by
     * performing self-join using some equi-join condition
     */
    val join1 = people.join(peop,
      not(people("Prefix") === peop("Prefix_1"))
        && people("Firstname") === peop("Firstname_1")
        && (people("Lastname") === peop("Lastname_1")))

    val join2 = people.join(peop,
      (people("Prefix") === peop("Prefix_1"))
        && not(people("Firstname") === peop("Firstname_1"))
        && (people("Lastname") === peop("Lastname_1")))

    val join3 = people.join(peop,
      people("Prefix") === peop("Prefix_1")
        && (people("Firstname") === peop("Firstname_1"))
        && not(people("Lastname") === peop("Lastname_1")))

    val join4 = people.join(peop,
      not(people("Prefix") === peop("Prefix_1"))
        && not(people("Firstname") === peop("Firstname_1"))
        && people("Lastname") === peop("Lastname_1"))

    val join5 = people.join(peop,
      (people("Prefix") === peop("Prefix_1"))
        && not(people("Firstname") === peop("Firstname_1"))
        && not(people("Lastname") === peop("Lastname_1")))

    val join6 = people.join(peop,
      not(people("Prefix") === peop("Prefix_1"))
        && (people("Firstname") === peop("Firstname_1"))
        && not(people("Lastname") === peop("Lastname_1")))

    val joined = (join1)
      .union(join2)
      .union(join3)
      .union(join4)
      .union(join5)
      .union(join6)
      .distinct()

    /*
     * Part of data is saved as a csv file and labelled manually as true or false
     * true if two developers information seems to belong to one identity and
     * false if not.
     */
    val splitt = joined.randomSplit(Array(0.1, 0.9), seed = 500)
    val (sample_1, sample_2) = (splitt(0), splitt(1))
    sample_1.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("unَAlgo2s.csv") 

    /* end of the data pre-processing */
      
      
    /* 
     * reading the labelled data and 
     * filter the rows that any of the developer information is empty 
     * and the prefix of the email contains any word like log, list, user
     * some new columns are created in order to calculate the string distance
     * which need the size of two strings or max size of two strings , etc 
     */
    val labeleddata = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .load("data/Algo2.csv")

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
     * All to All comparisons
     * JaroWinkler, NormalizedLevenschteinRatio, Jaccard Coefficient 
     * are used as string comparison metrics which are used as
     * features for the machine learning
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
      .withColumn("PP-Jaccard", format((intersectlen(labeleddata("Prefix"), labeleddata("Prefix_1")) / (length(labeleddata("Prefix")) + length(labeleddata("Prefix_1")) - intersectlen(labeleddata("Prefix"), labeleddata("Prefix_1"))))))
      .withColumn("PF-Jaccard", format((intersectlen(labeleddata("Prefix"), labeleddata("Firstname_1")) / (length(labeleddata("Prefix")) + length(labeleddata("Firstname_1")) - intersectlen(labeleddata("Prefix"), labeleddata("Firstname_1"))))))
      .withColumn("PL-Jaccard", format((intersectlen(labeleddata("Prefix"), labeleddata("Lastname_1")) / (length(labeleddata("Prefix")) + length(labeleddata("Lastname_1")) - intersectlen(labeleddata("Prefix"), labeleddata("Lastname_1"))))))
      .withColumn("FP-Jaccard", format((intersectlen(labeleddata("Firstname"), labeleddata("Prefix_1")) / (length(labeleddata("Firstname")) + length(labeleddata("Prefix_1")) - intersectlen(labeleddata("Firstname"), labeleddata("Prefix_1"))))))
      .withColumn("FF-Jaccard", format((intersectlen(labeleddata("Firstname"), labeleddata("Firstname_1")) / (length(labeleddata("Firstname")) + length(labeleddata("Firstname_1")) - intersectlen(labeleddata("Firstname"), labeleddata("Firstname_1"))))))
      .withColumn("FL-Jaccard", format((intersectlen(labeleddata("Firstname"), labeleddata("Lastname_1")) / (length(labeleddata("Firstname")) + length(labeleddata("Lastname_1")) - intersectlen(labeleddata("Firstname"), labeleddata("Lastname_1"))))))
      .withColumn("LP-Jaccard", format((intersectlen(labeleddata("Lastname"), labeleddata("Prefix_1")) / (length(labeleddata("Lastname")) + length(labeleddata("Prefix_1")) - intersectlen(labeleddata("Lastname"), labeleddata("Prefix_1"))))))
      .withColumn("LF-Jaccard", format((intersectlen(labeleddata("Lastname"), labeleddata("Firstname_1")) / (length(labeleddata("Lastname")) + length(labeleddata("Firstname_1")) - intersectlen(labeleddata("Lastname"), labeleddata("Firstname_1"))))))
      .withColumn("LL-Jaccard", format((intersectlen(labeleddata("Lastname"), labeleddata("Lastname_1")) / (length(labeleddata("Lastname")) + length(labeleddata("Lastname_1")) - intersectlen(labeleddata("Lastname"), labeleddata("Lastname_1"))))))
      .withColumn("f7", format(df("pmax") - levenshtein(df("Prefix"), df("Prefix_1"))) / df("pmax"))
      .withColumn("f8", format(df("fmax") - levenshtein(df("Firstname"), df("Firstname_1"))) / df("fmax"))
      .withColumn("f9", format(df("lmax") - levenshtein(df("Lastname"), df("Lastname_1"))) / df("lmax"))
      .drop("s_lastname_1", "s_lastname", "s_firstname_1", "s_firstname", "s_prefix_1", "s_prefix", "plen", "flen", "llen", "pmax", "fmax", "lmax")

    // create a vector of features
    val assembler = new VectorAssembler()
      .setInputCols(Array("f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9",
        "PP-Jaccard", "PF-Jaccard", "PL-Jaccard",
        "FF-Jaccard", "FP-Jaccard", "FL-Jaccard",
        "LP-Jaccard", "LF-Jaccard", "LL-Jaccard"))
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

    //Gradient Boosted Tree as the machine learning algorithm
    val gbt = new GBTClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(20)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

    val model = pipeline.fit(trainingData)
    val predictions = model.transform(testData)
    //predictions.select("predictedLabel", "label", "features").show(500)

    predictions.createOrReplaceTempView("people")
    val predict = sparkSession.sql("SELECT Prefix, Prefix_1, Firstname, Firstname_1, Lastname, Lastname_1,predictedLabel, label FROM people WHERE predictedLabel !=  label")

    /* 
     * Confusion matrix is used to evaluate the algorithm performance
     * TT = True Positive   FT = false negative    TF = false positive     FF = True Negative
     */
    println(predictions.filter($"predictedLabel" === "T" && $"label" === "T").count() + " TT\n" +
      predictions.filter($"predictedLabel" === "F" && $"label" === "T").count() + " FT\n" +
      predictions.filter($"predictedLabel" === "T" && $"label" === "F").count() + " TF\n" +
      predictions.filter($"predictedLabel" === "F" && $"label" === "F").count() + " FF\n")

    predict.show()
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    }
}