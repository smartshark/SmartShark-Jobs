
package de.ugoe.cs.smartshark.jobs;

import static org.apache.spark.sql.functions.*;

import java.sql.Date;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.ugoe.cs.smartshark.util.DBUtilFactory;
import de.ugoe.cs.smartshark.util.IDBUtils;
import scala.Tuple2;

public class LabelBuginducingCommits {

    @SuppressWarnings("serial")
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        // create spark session with appropriate App name
        // all other information (e.g., master, DB credentials, etc. should be passed as parameters
        SparkSession sparkSession =
            SparkSession.builder().appName("Buginducing-Labeller").getOrCreate();
        IDBUtils dbUtils = DBUtilFactory.getDBUtils(sparkSession);

        Dataset<Row> commits = dbUtils.loadData("commit");

        // TODO this should actually be part of another job
        commits = commits.withColumn("bugfix",
                                     col("message").rlike("(?i)fix(e[ds])?|bugs?|defects?|patch"));
        // TODO other job end

        commits = commits.select(col("_id").alias("commitId"), col("projectId"),
                                 col("fileActionIds"), col("committerDate"), col("bugfix"));
        commits = commits.withColumn("fileActionId", explode(col("fileActionIds")));;
        commits = commits.drop("fileActionIds");

        // join commits and associated file actions
        Dataset<Row> fileActions =
            dbUtils.loadData("file_action").select(col("_id").alias("fileActionId"),
                                                   col("projectId"), col("fileId"), col("hunkIds"));
        commits = commits.join(fileActions, "fileActionId");
        commits = commits.withColumn("hunkId", explode(col("hunkIds")));
        commits = commits.drop("hunkIds");

        // join hunks to commits via file actions
        Dataset<Row> hunks =
            dbUtils.loadData("hunk").select(col("_id").alias("hunkId"), col("new_start"),
                                            col("old_start"), col("old_lines"));
        commits = commits.join(hunks, "hunkId");
        commits = commits.drop("allHunkIds");

        // self join commits to have all previous commits on files where bugfixes happened
        Dataset<Row> commitsCopy = commits.select(col("commitId").alias("other_commitId"),
                                                  col("fileId").alias("other_fileId"),
                                                  col("committerDate").alias("other_committerDate"),
                                                  col("new_start").alias("other_new_start"));
        commits = commits.filter(col("bugfix").like("true"));
        commits.select(col("commitId"), col("fileId"), col("committerDate"), col("old_start"),
                       col("old_lines"));
        commits = commits.join(commitsCopy, col("fileId").equalTo(col("other_fileId"))
            .and(col("committerDate").gt(col("other_committerDate"))));
        commits = commits.select(col("commitId"), col("other_commitId"), col("other_committerDate"),
                                 col("old_start"), col("old_lines"), col("other_new_start"));

        // created grouped commitId-grouped RDD
        JavaPairRDD<String, Iterable<Row>> commitPairsRDD =
            commits.javaRDD().mapToPair(new PairFunction<Row, String, Row>()
        {
                @Override
                public Tuple2<String, Row> call(Row row) throws Exception {
                    String commitId = row.getString(row.fieldIndex("commitId"));
                    return new Tuple2<String, Row>(commitId, row);
                }
            }).groupByKey();

        // determine which commits where buginducing
        JavaRDD<String> inducingCommitsRDD =
            commitPairsRDD.map(new Function<Tuple2<String, Iterable<Row>>, String>()
        {
                @Override
                public String call(Tuple2<String, Iterable<Row>> commitPair) throws Exception {
                    // find latest commit that touched modified hunk
                    int lineStart = -1;
                    int length = -1;
                    Date latestCommitDate = null;
                    String latestCommitId = null;

                    for (Row row : commitPair._2) {
                        if (lineStart == -1) {
                            lineStart = row.getInt(row.fieldIndex("old_start"));
                            length = row.getInt(row.fieldIndex("old_lines"));
                        }

                        int newLineStart = row.getInt(row.fieldIndex("other_new_start"));
                        if (newLineStart <= lineStart + length) {
                            // change within correct area
                            Date currentDate = row.getDate(row.fieldIndex("other_committerDate"));
                            if (latestCommitDate == null) {
                                latestCommitDate = currentDate;
                                latestCommitId = row.getString(row.fieldIndex("other_commitId"));
                            }
                            else {
                                if (latestCommitDate.before(currentDate)) {
                                    latestCommitDate = currentDate;
                                    latestCommitId =
                                        row.getString(row.fieldIndex("other_commitId"));
                                }
                            }
                        }
                    }
                    return latestCommitId;
                }
            });
        // remove duplicates (one commit may have induced multiple bugfixes
        inducingCommitsRDD = inducingCommitsRDD.distinct();
        
        System.out.println((System.currentTimeMillis() - startTime) / 1000);

        // TODO write to correct place
    }
}
