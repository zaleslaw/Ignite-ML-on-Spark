package Chapter_2_IgniteDF_2.performance.read;

import Chapter_2_IgniteDF_2.performance.LargeGeneratorExample;
import java.util.List;
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

public class SimpleReadWrite {
    private static final String CACHE_NAME = "testCache";

    /**
     * Ignite config file.
     */
    private static final String CONFIG = "config/example-ignite.xml";

    /** Run example. */
    public static void main(String[] args) {

        Ignite ignite = Ignition.start(CONFIG);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(CACHE_NAME).setSqlSchema("PUBLIC");

        IgniteCache<?, ?> cache = ignite.getOrCreateCache(ccfg);

        // Spark reading and writing to table
        SparkSession spark = SparkSession
            .builder()
            .appName("SparkForIgnite")
            .config("ignite.disableSparkSQLOptimization", "true")
            .master("spark://192.168.1.66:7077")
            .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> ds = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .option("charset", "windows-1251")
            .option("delimiter", ";")
            .csv("D:\\ds_" + LargeGeneratorExample.AMOUNT_OF_ROWS + ".txt");

        Dataset<Row> newds = ds.repartition(200)
            .filter("BUSINESS_UNIT != 'BUSINESS_UNIT2'")
            .select("id", "BUSINESS_UNIT", "DEPTID", "PRODUCT", "ACCOUNT")
            .sort("BUSINESS_UNIT", "JOURNAL_DATE");
        newds.persist(StorageLevel.MEMORY_ONLY());

        newds.show(100);

        newds.write().format(IgniteDataFrameSettings.FORMAT_IGNITE())
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
            .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "id")
            .option(IgniteDataFrameSettings.OPTION_TABLE(), "RIGHT")
            .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(), "template=replicated")
            .mode(SaveMode.Append)
            .save();

        newds.write().format(IgniteDataFrameSettings.FORMAT_IGNITE())
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
            .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "id")
            .option(IgniteDataFrameSettings.OPTION_TABLE(), "LEFT")
            .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(), "template=replicated")
            .mode(SaveMode.Append)
            .save();

        newds.explain(true);

        //Reading saved data from Ignite.
        List<List<?>> data = cache.query(new SqlFieldsQuery("SELECT COUNT(*) FROM RIGHT")).getAll();
        System.out.println("RIGHT from IGNITE: " + data);

        Dataset<Row> rightDF = spark.read()
            .format(IgniteDataFrameSettings.FORMAT_IGNITE()) //Data source type.
            .option(IgniteDataFrameSettings.OPTION_TABLE(), "RIGHT") //Table to read.
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG) //Ignite config.
            .load();

        Dataset<Row> filteredRightDF = rightDF
            .select("id", "BUSINESS_UNIT", "DEPTID", "PRODUCT", "ACCOUNT")
            .where("DEPTID != 'DEPTID2'")
            .sort("DEPTID", "BUSINESS_UNIT");

        filteredRightDF.show();
        filteredRightDF.explain(true);

        Dataset<Row> result = filteredRightDF.select("DEPTID", "ACCOUNT")
            .groupBy("DEPTID")
            .avg("ACCOUNT");

        result.show(100);
        result.explain(true);
    }
}
