package Chapter_2_IgniteDF;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkToIgnite {
    private static final String CACHE_NAME = "testCache";

    /**
     * Ignite config file.
     */
    private static final String CONFIG = "config/example-ignite.xml";

    /** Run example. */
    public static void main(String[] args) throws InterruptedException {

        Ignite ignite = Ignition.start(CONFIG);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(CACHE_NAME).setSqlSchema("PUBLIC");

        IgniteCache<?, ?> cache = ignite.getOrCreateCache(ccfg);

        // Spark reading and writing to table
        SparkSession spark = SparkSession
            .builder()
            .appName("SparkForIgnite")
            .master("local[2]")
            .getOrCreate();

        Dataset<Row> ds = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .option("charset", "windows-1251")
            .option("delimiter", ";")
            .csv("D:\\ds_large.txt");

        Dataset<Row> cached_ds = ds.cache();
        Dataset<Row> filtered_ds = cached_ds.filter("id != 2");
        filtered_ds.count();
        filtered_ds.show();
        //ds.repartition(200);
        filtered_ds.write().format("csv").save("D:\\ds_large_2.txt");

        filtered_ds.write().format(IgniteDataFrameSettings.FORMAT_IGNITE())
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
            .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "id")
            .option(IgniteDataFrameSettings.OPTION_TABLE(), "LARGE_TABLE")
            .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(), "template=replicated")
            .save();

        Dataset<Row> df2 = spark.read()
            .format(IgniteDataFrameSettings.FORMAT_IGNITE()) //Data source type.
            .option(IgniteDataFrameSettings.OPTION_TABLE(), "LARGE_TABLE") //Table to read.
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG) //Ignite config.
            .load();
        System.out.println(df2.count());

        Thread.sleep(100000);

        //Ignition.stop(false);
    }
}
