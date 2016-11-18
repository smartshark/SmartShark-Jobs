
package de.ugoe.cs.smartshark.jobs;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.ugoe.cs.smartshark.util.AnalysisUtils;
import de.ugoe.cs.smartshark.util.DBUtilFactory;
import de.ugoe.cs.smartshark.util.IDBUtils;

/**
 * <p>
 * A simple Spark Job for SmartSHARK that labels commits as bugfixing commits.
 * </p>
 * 
 * @author Steffen Herbold
 */
public class LabelBugfixingCommits {

    public static void main(String[] args) {
        // create spark session with appropriate App name
        // all other information (e.g., master, DB credentials, etc. should be passed as parameters
        SparkSession sparkSession = SparkSession.builder().appName("Bugfix-Labeller").getOrCreate();
        IDBUtils dbUtils = DBUtilFactory.getDBUtils(sparkSession);

        // fetch data and add bugfix label to commits
        Dataset<Row> commits = dbUtils.loadData("commit").select(col("_id"), col("message"));
        if( args.length>0 ) {
            AnalysisUtils analysisUtils = new AnalysisUtils(sparkSession);
            String projectId = analysisUtils.resolveProjectUrl(args[0]);
            commits.filter(col("projectId").like(projectId));
        }
        commits = commits.withColumn("bugfix",
                                     col("message").rlike("(?i)fix(e[ds])?|bugs?|defects?|patch"));

        // TODO writes to a new collection, as existing documents may overwritten leading to data loss
        dbUtils.writeData(commits, "commits2");
    }
    
}
