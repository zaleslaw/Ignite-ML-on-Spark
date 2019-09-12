package cluster_experiments.performance.read.hypothesis;

import cluster_experiments.performance.LargeGeneratorExample;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.util.List;

public class DuplicationOfIgniteQueries {
    /**
     * Ignite config file.
     */
    private static final String CONFIG = "/home/zaleslaw/IdeaProjects/Ignite-ML-on-Spark/config/example-ignite.xml";

    /** Run example. */
    public static void main(String[] args) {
        // Spark reading and writing to table
        SparkSession spark = SparkSession
            .builder()
            .appName("DuplicationQueries")
            .config("ignite.disableSparkSQLOptimization", "true")
            .master("spark://172.25.4.118:7077")
            .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> ds = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .option("charset", "windows-1251")
            .option("delimiter", ";")
            .csv("/home/zaleslaw/data/ds_" + LargeGeneratorExample.AMOUNT_OF_ROWS + ".txt");

        Dataset<Row> newds = ds.repartition(200)
            .filter("BUSINESS_UNIT != 'BUSINESS_UNIT2'")
            .select("id", "BUSINESS_UNIT", "DEPTID", "PRODUCT", "ACCOUNT")
            .sort("BUSINESS_UNIT", "JOURNAL_DATE");
        newds.persist(StorageLevel.MEMORY_ONLY());

        newds.write().format(IgniteDataFrameSettings.FORMAT_IGNITE())
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
            .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "id")
            .option(IgniteDataFrameSettings.OPTION_TABLE(), "RIGHT")
            .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(), "template=replicated")
            .mode(SaveMode.Append)
            .save();

        Dataset<Row> rightDF = spark.read()
            .format(IgniteDataFrameSettings.FORMAT_IGNITE()) //Data source type.
            .option(IgniteDataFrameSettings.OPTION_TABLE(), "RIGHT") //Table to read.
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG) //Ignite config.
            .load();

        Dataset<Row> filteredRightDF = rightDF
                .select("id", "BUSINESS_UNIT", "DEPTID")
                .where("DEPTID != 'DEPTID2'")
                .sort("DEPTID", "BUSINESS_UNIT");

        filteredRightDF.persist(StorageLevel.MEMORY_ONLY());
        filteredRightDF.show();
        System.out.println(filteredRightDF.count());

        //Registering DataFrame as Spark view.
        rightDF.createOrReplaceTempView("GREAT_TABLE");

        //Selecting data from Ignite through Spark SQL Engine.
        Dataset<Row> result = spark.sql("SELECT ID, PRODUCT, ACCOUNT FROM GREAT_TABLE ORDER BY ACCOUNT");

        System.out.println("Result SQL count " + result.count());
        result.show(100);
        result.explain(true);
    }
}
