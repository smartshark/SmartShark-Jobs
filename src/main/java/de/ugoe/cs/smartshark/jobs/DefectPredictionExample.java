
package de.ugoe.cs.smartshark.jobs;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import de.ugoe.cs.smartshark.util.DBUtilFactory;
import de.ugoe.cs.smartshark.util.IDBUtils;

/**
 * <p>
 * A small work-in-progress example to show how defect prediction can be implemented with SmartSHARK
 * using the new dataframe API.
 * </p>
 * 
 * @author Steffen Herbold
 */
public class DefectPredictionExample {

    public static void main(String[] args) {
        // create spark session with appropriate App name
        // all other information (e.g., master, DB credentials, etc. should be passed as parameters
        SparkSession sparkSession = SparkSession.builder().appName("Bugfix-Labeller").getOrCreate();
        IDBUtils dbUtils = DBUtilFactory.getDBUtils(sparkSession);

        // fetch data and add bugfix label to commits
        Dataset<Row> commits = dbUtils.loadData("commit");
        // TODO should be part of other job
        commits = commits.withColumn("bugfix",
                                     col("message").rlike("(?i)fix(e[ds])?|bugs?|defects?|patch"));

        commits = commits.selectExpr("_id as commit_id", "bugfix");
        Dataset<Row> fileState = dbUtils.loadData("file_state")
            .select(col("commit_id"), col("metrics"), col("file_type"));
        fileState = fileState.join(commits, "commit_id").drop("commit_id");
        fileState = fileState.filter(col("file_type").like("method"));
        fileState = fileState.select(col("bugfix"), col("metrics.McCC"), col("metrics.NL"));
        fileState = fileState.filter(col("McCC").isNotNull());
        fileState = fileState.filter(col("NL").isNotNull());
        fileState = fileState.withColumn("bugfix_double", col("bugfix").cast(DataTypes.DoubleType));

        VectorAssembler va = new VectorAssembler().setInputCols(new String[]
            { "McCC", "NL" }).setOutputCol("features");
        fileState = va.transform(fileState);

        LogisticRegression lr = new LogisticRegression();
        lr.setLabelCol("bugfix_double");
        lr.setFeaturesCol("features");
        LogisticRegressionModel model = lr.fit(fileState);

        fileState = model.transform(fileState);

        fileState.show();

        fileState.filter(col("bugfix").like("true")).show();

        // TODO write to correct place
    }
}
