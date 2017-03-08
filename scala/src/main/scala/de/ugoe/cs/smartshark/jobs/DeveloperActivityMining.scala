package de.ugoe.cs.smartshark.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import de.ugoe.cs.smartshark.util.DBUtilFactory

object developerActivities {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Spark Developer Activities")
      .getOrCreate()

    import sparkSession.implicits._

    val dbUtils = DBUtilFactory.getDBUtils(sparkSession)
    
    // Load message information from mongoDB collection
    val message = dbUtils.loadData("message")

    // Number of Messages each developer posts in mailing list monthly
    val messages = message.withColumn("Date", date_format(message("date"), "yyyy-MM"))
      .groupBy("Date", "from_id")
      .count()
      .withColumnRenamed("count", "messages")
      .withColumnRenamed("from_id", "ID")
    messages.show(10)
    
    // Load data from mongoDB collection, issue_comment 
    val issue_comment = dbUtils.loadData("issue_comment")
    
    // Number of comments each developer comments monthly on the issues
    val issue_comments = issue_comment.withColumn("Date", date_format(issue_comment("created_at"), "yyyy-MM"))
      .groupBy("Date", "author_id")
      .count()
      .withColumnRenamed("count", "comments")
      .withColumnRenamed("author_id", "ID")
    issue_comments.show(10)
    
    // Load data from mongoDB collection, commit 
    val commit = dbUtils.loadData("commit")
    
    // Number of commits for each developer monthly 
    val commits = commit.withColumn("Date", date_format(commit("committerDate"), "yyyy-MM"))
      .groupBy("Date", "committerId")
      .count()
      .withColumnRenamed("count", "commits")
      .withColumnRenamed("committerId", "ID")
    commits.show(10)
    
    // Number of bugs that each developer fixes monthly
    val bugfix = commit.withColumn("Date", date_format(commit("committerDate"), "yyyy-MM"))
      .withColumn("bugfix", col("message").rlike("(?i)fix(e[ds])?|bugs?|defects?|patch"))
      .groupBy("Date", "committerId", "bugfix")
      .count()
      .withColumnRenamed("count", "bug_fix")
      .withColumnRenamed("committerId", "ID")

    bugfix.filter(bugfix("bugfix") === true).drop("bugfix").show(10)
    
    // Join operation to get a complete picture of developers activities
    val join1 = issue_comments.join(messages, Seq("Date", "ID"), "outer")
    val join2 = commits.join(bugfix, Seq("Date", "ID"), "outer")
    val df = join1.join(join2, Seq("Date", "ID"), "outer")
    df.drop("bugfix").show(100) 
  }
}