package tutorial.Chapter_2_IgniteDF.performance.read;

import tutorial.Chapter_2_IgniteDF.performance.LargeGeneratorExample;
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

public class SimpleReadWriteWithJOIN {
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
            .master("local[4]")
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

        // newds.show(100);

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

        //Reading saved data from Ignite.
        List<List<?>> data2 = cache.query(new SqlFieldsQuery("SELECT COUNT(*) FROM LEFT")).getAll();
        System.out.println("LEFT from IGNITE: " + data2);

        List<List<?>> data3 = cache.query(new SqlFieldsQuery("SELECT table1.ID, table1.PRODUCT, table1.ACCOUNT, table2.BUSINESS_UNIT, table2.DEPTID, table2.PRODUCT, table2.ACCOUNT FROM (SELECT ID, PRODUCT, ACCOUNT FROM LEFT ORDER BY ACCOUNT) table1 JOIN (SELECT ID, BUSINESS_UNIT, DEPTID, PRODUCT, ACCOUNT FROM RIGHT WHERE DEPTID IS NOT NULL AND NOT DEPTID = 'DEPTID2' ORDER BY DEPTID, BUSINESS_UNIT) table2 ON table1.id = table2.id")).getAll();
        System.out.println("Joined from IGNITE: " + data3.size());

        Dataset<Row> rightDF = spark.read()
            .format(IgniteDataFrameSettings.FORMAT_IGNITE()) //Data source type.
            .option(IgniteDataFrameSettings.OPTION_TABLE(), "RIGHT") //Table to read.
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG) //Ignite config.
            .load();

        Dataset<Row> filteredRightDF = rightDF
            .select("id", "BUSINESS_UNIT", "DEPTID")
            .where("DEPTID != 'DEPTID2'")
            .sort("DEPTID", "BUSINESS_UNIT");

        //filteredRightDF.show();

        Dataset<Row> leftDF = spark.read()
            .format(IgniteDataFrameSettings.FORMAT_IGNITE()) //Data source type.
            .option(IgniteDataFrameSettings.OPTION_TABLE(), "LEFT") //Table to read.
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG) //Ignite config.
            .load();

        Dataset<Row> filteredLeftDF = leftDF
            .select("id", "PRODUCT", "ACCOUNT")
            .sort("ACCOUNT");

        //filteredLeftDF.show();

        Dataset<Row> joinedDF = filteredLeftDF.join(filteredRightDF, "id");

        //joinedDF.explain(true);
        //joinedDF.show();

        Dataset<Row> result = joinedDF
            .groupBy("DEPTID")
            .avg("ACCOUNT");

        System.out.println("Result count " + result.count());
        //result.show(100);
        result.explain(true);
    }
}
