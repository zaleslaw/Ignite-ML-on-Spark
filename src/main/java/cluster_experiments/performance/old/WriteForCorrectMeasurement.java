package cluster_experiments.performance.old;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

public class WriteForCorrectMeasurement {
    private static final String CACHE_NAME = "testCache";

    /**
     * Ignite config file.
     */
    private static final String CONFIG = "config/example-ignite.xml";

    /** Run example. */
    public static void main(String[] args) throws InterruptedException {

        Ignite ignite = Ignition.start(CONFIG);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(CACHE_NAME).setSqlSchema("PUBLIC");

        // Spark reading and writing to table
        SparkSession spark = SparkSession
            .builder()
            .appName("SparkForIgnite")
            .master("local[4]")
            .getOrCreate();

        Dataset<Row> ds = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .option("charset", "windows-1251")
            .option("delimiter", ";")
            .csv("D:\\ds_large.txt");

        Dataset<Row> filteredDS = ds
            .filter("id != 2")
            .select("id", "BUSINESS_UNIT", "JOURNAL_DATE")
            .sort("BUSINESS_UNIT", "JOURNAL_DATE");
        Dataset<Row> newDS = filteredDS.repartition(200);
        newDS.persist(StorageLevel.MEMORY_ONLY());
        newDS.count();

        newDS.write().format(IgniteDataFrameSettings.FORMAT_IGNITE())
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
            .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "id")
            .option(IgniteDataFrameSettings.OPTION_TABLE(), "LARGE_TABLE")
            .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(), "template=replicated")
            .mode(SaveMode.Append)
            .save();

        newDS.write().format(IgniteDataFrameSettings.FORMAT_IGNITE())
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
            .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "id")
            .option(IgniteDataFrameSettings.OPTION_TABLE(), "LARGE_TABLE")
            .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(), "template=replicated")
            .mode(SaveMode.Append)
            .save();
    }
}
