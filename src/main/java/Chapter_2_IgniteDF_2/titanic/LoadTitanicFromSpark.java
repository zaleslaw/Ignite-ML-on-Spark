package Chapter_2_IgniteDF_2.titanic;

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

public class LoadTitanicFromSpark {
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
            .option("delimiter", ";")
            .option("header", "true")
            .csv("src/main/resources/titanic.csv");
        ds.show();
        Dataset<Row> newds = ds.repartition(200);

        newds.write().format(IgniteDataFrameSettings.FORMAT_IGNITE())
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
            .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "id")
            .option(IgniteDataFrameSettings.OPTION_TABLE(), "titanic")
            .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(), "backups=1, template=replicated")
            .mode("append")
            .save();

        //Reading saved data from Ignite.
        List<List<?>> data = cache.query(new SqlFieldsQuery("SELECT id, sex FROM titanic")).getAll();
        System.out.println(data);

        Dataset<Row> df = spark.read()
            .format(IgniteDataFrameSettings.FORMAT_IGNITE()) //Data source type.
            .option(IgniteDataFrameSettings.OPTION_TABLE(), "titanic") //Table to read.
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG) //Ignite config.
            .load();

        //Registering DataFrame as Spark view.
        df.createOrReplaceTempView("titanic");

        //Selecting data from Ignite through Spark SQL Engine.
        Dataset<Row> igniteDF = spark.sql("SELECT COUNT(*) FROM titanic WHERE id >= 2 AND sex = 'male'");
        igniteDF.show();
        igniteDF.explain(true);

        Thread.sleep(100000);

        //Ignition.stop(false);
    }
}
