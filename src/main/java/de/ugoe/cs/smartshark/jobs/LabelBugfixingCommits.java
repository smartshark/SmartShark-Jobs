
package de.ugoe.cs.smartshark.jobs;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
        Dataset<Row> commits =
            dbUtils.loadData("commit").select(col("_id"), col("vcs_system_id"), col("message"));

        // if a project name is defined, apply only to commits of the same project
        if (args.length > 0) {
            // fetch project id
            Dataset<Row> projects = dbUtils.loadData("project").filter(col("name").like(args[0]))
                .select(col("_id").alias("project_id"));

            // fetch vcs system ids
            Dataset<Row> vcsSystems = dbUtils.loadData("vcs_system").join(projects, "project_id")
                .select(col("_id").alias("vcs_id"));

            // filter commits
            commits = commits
                .join(vcsSystems, commits.col("vcs_system_id").equalTo(vcsSystems.col("vcs_id")))
                .select(col("_id"), col("vcs_system_id"), col("message"));
        }
        commits = commits.withColumn("bugfix",
                                     col("message").rlike("(?i)fix(e[ds])?|bugs?|defects?|patch"));

        // TODO writes to a new collection, as existing documents may overwritten leading to data
        // loss
        // dbUtils.writeData(commits, "commits2");
    }

}
