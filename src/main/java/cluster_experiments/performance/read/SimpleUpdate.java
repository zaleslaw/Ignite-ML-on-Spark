package cluster_experiments.performance.read;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SimpleUpdate {
    private static final String CACHE_NAME = "testCache";

    /**
     * Ignite config file.
     */
    private static final String CONFIG = "/home/zaleslaw/Projects/Ignite-ML-on-Spark/config/example-ignite.xml";

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

        //Reading saved data from Ignite.
        List<List<?>> data = cache.query(new SqlFieldsQuery("SELECT COUNT(*) FROM RIGHT")).getAll();
        System.out.println("RIGHT from IGNITE: " + data);

        cache.query(new SqlFieldsQuery("UPDATE RIGHT SET BUSINESS_UNIT = 'Mary Major V2' WHERE BUSINESS_UNIT = 'BUSINESS_UNIT3'"));

        Dataset<Row> rightDF = spark.read()
            .format(IgniteDataFrameSettings.FORMAT_IGNITE()) //Data source type.
            .option(IgniteDataFrameSettings.OPTION_TABLE(), "RIGHT") //Table to read.
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG) //Ignite config.
            .load();

        //Registering DataFrame as Spark view.
        rightDF.createOrReplaceTempView("RIGHT");

        //Selecting data from Ignite through Spark SQL Engine.
        //Dataset<Row> result = spark.sql("UPDATE BUSINESS_UNIT SET BUSINESS_UNIT = 'Mary Major V2' WHERE BUSINESS_UNIT = 'BUSINESS_UNIT3'");

        Dataset<Row> result = rightDF;
        result.filter("BUSINESS_UNIT == 'Mary Major V2'").show();
        result.explain(true);
    }
}
