package tutorial.Chapter_3_Ignite_Load_cache;

import java.io.Serializable;
import java.time.LocalTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.jetbrains.annotations.Nullable;

public class SparkStore implements CacheStore<Integer, Object[]>, Serializable {
    private SparkSession spark;
    private Dataset<Row> ds;
    private static IgniteBiInClosure<Integer, Object[]> staticClo;

    {
        spark = SparkSession
            .builder()
            .appName("SparkForIgnite")
            .master("local")
            .config("spark.executor.instances", "2")
            .getOrCreate();

        ds = spark.read()
            .option("delimiter", ";")
            .option("header", "true")
            .csv("src/main/resources/titanic.csv");

        ds = ds.withColumn("index", functions.monotonically_increasing_id());

        ds.show();
    }

    @Override public void loadCache(IgniteBiInClosure<Integer, Object[]> clo,
        @Nullable Object... objects) throws CacheLoaderException {
        LocalTime startTime = LocalTime.now();
        System.out.println("[loadCache] Loading cache from the Spark");
        staticClo = clo;

        ForeachFunction<Row> index = row -> {
            Object[] parsedRow = new Object[14];

            for (int i = 0; i < 14; i++)
                parsedRow[i] = row.get(i);

            int key = (int)row.getLong(14);

            staticClo.apply(key, parsedRow);
        };
        ds.foreach(index); // stupid solution

        LocalTime endTime = LocalTime.now();
        System.out.println("[loadCache] Time pre: " + startTime);
        System.out.println("[loadCache] Time pos: " + endTime);
        System.out.println("[loadCache] Time diff: " + (endTime.getNano() - startTime.getNano()));

    }

    @Override public void sessionEnd(boolean b) throws CacheWriterException {

    }

    @Override public Object[] load(Integer id) throws CacheLoaderException {
        System.out.println("[load] Loading single row from Spark");
        List<Row> res = ds.filter("index = " + id).collectAsList();
        if (res.size() > 0) {
            Object[] parsedRow = new Object[14];
            for (int i = 0; i < 14; i++)
                parsedRow[i] = res.get(0).get(i);
            return parsedRow;
        }
        else {
            System.out.println("No rows in Spark with id " + id);
            return null;
        }

    }

    @Override public Map<Integer, Object[]> loadAll(Iterable<? extends Integer> iterable) throws CacheLoaderException {
        return null;
    }

    @Override public void write(Cache.Entry<? extends Integer, ? extends Object[]> entry) throws CacheWriterException {

    }

    @Override public void writeAll(
        Collection<Cache.Entry<? extends Integer, ? extends Object[]>> collection) throws CacheWriterException {

    }

    @Override public void delete(Object o) throws CacheWriterException {

    }

    @Override public void deleteAll(Collection<?> collection) throws CacheWriterException {

    }
}
