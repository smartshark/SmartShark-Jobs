
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

/**
 * <p>
 * A Spark job that determines the latest commit on the changed parts of a file and identifies this
 * as buginducing commit.
 * </p>
 * 
 * @author Steffen Herbold
 */
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

        commits =
            commits.select(col("_id").alias("commit_id"), col("committer_date"), col("bugfix"));

        // join commits and associated file actions
        Dataset<Row> fileActions = dbUtils.loadData("file_action")
            .select(col("_id").alias("file_action_id"), col("file_id"), col("commit_id"),
                    col("file_id"));
        commits = commits.join(fileActions, "commit_id");

        // join hunks to commits via file actions
        Dataset<Row> hunks = dbUtils.loadData("hunk")
            .select(col("file_action_id"), col("new_start"), col("old_start"), col("old_lines"));
        commits = commits.join(hunks, "file_action_id");

        // self join commits to have all previous commits on files where bugfixes happened
        Dataset<Row> commitsCopy = commits.select(col("commit_id").alias("other_commit_id"),
                                                  col("file_id").alias("other_file_id"),
                                                  col("committer_date")
                                                      .alias("other_committer_date"),
                                                  col("new_start").alias("other_new_start"));
        commits = commits.filter(col("bugfix").like("true"));
        commits.select(col("commit_id"), col("file_id"), col("committer_date"), col("old_start"),
                       col("old_lines"));
        commits = commits.join(commitsCopy, col("file_id").equalTo(col("other_file_id"))
            .and(col("committer_date").gt(col("other_committer_date"))));
        commits =
            commits.select(col("commit_id"), col("other_commit_id"), col("other_committer_date"),
                           col("old_start"), col("old_lines"), col("other_new_start"));

        // created grouped commitId-grouped RDD
        JavaPairRDD<String, Iterable<Row>> commitPairsRDD =
            commits.javaRDD().mapToPair(new PairFunction<Row, String, Row>()
        {
                @Override
                public Tuple2<String, Row> call(Row row) throws Exception {
                    Row commitIdStruct = row.getStruct(row.fieldIndex("commit_id"));
                    String commitId = commitIdStruct.getString(commitIdStruct.fieldIndex("oid"));
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
                            Date currentDate = row.getDate(row.fieldIndex("other_committer_date"));
                            if (latestCommitDate == null) {
                                latestCommitDate = currentDate;
                                Row latestCommitIdStruct =
                                    row.getStruct(row.fieldIndex("other_commit_id"));
                                latestCommitId = latestCommitIdStruct
                                    .getString(latestCommitIdStruct.fieldIndex("oid"));
                            }
                            else {
                                if (latestCommitDate.before(currentDate)) {
                                    latestCommitDate = currentDate;
                                    Row latestCommitIdStruct =
                                        row.getStruct(row.fieldIndex("other_commit_id"));
                                    latestCommitId = latestCommitIdStruct
                                        .getString(latestCommitIdStruct.fieldIndex("oid"));
                                }
                            }
                        }
                    }
                    return latestCommitId;
                }
            });

        // remove duplicates (one commit may have induced multiple bugfixes
        inducingCommitsRDD = inducingCommitsRDD.distinct();
        inducingCommitsRDD.count();
        System.out.println((System.currentTimeMillis() - startTime) / 1000);

        // TODO write to correct place
    }
}
