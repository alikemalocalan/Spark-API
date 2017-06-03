package com.SparkEngine;

/**
 * Created by alikemal on 19.03.2017.
 */

import com.Conf;
import com.model.Song;
import com.model.Tag;
import com.model.TasteStat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.api.java.StorageLevels.MEMORY_ONLY;

public class InitSpark {
    public static SparkSession spark = null;
    private final Logger LOGGER = LoggerFactory.getLogger(InitSpark.class);

    private final Dataset<Row> userJson;
    private final Dataset<Row> songJson;
    private final Dataset<Row> tagJson;
/*
    private final Dataset<Row> tagParq;
    private final Dataset<Row> songParq;
    private final Dataset<Row> tasteParq;*/
    private final Dataset<Row> ratingJson;

    public InitSpark() {

        SparkConf conf = new SparkConf()
                .setAppName(InitSpark.class.getName())
                .setMaster("local");
        spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();


        //******************************************

        userJson = spark.read().json(Conf.usersJson_txt);
        songJson = spark.read().json(Conf.songJson_txt);
        tagJson = spark.read().json(Conf.tagJson_txt);
        ratingJson = spark.read().json(Conf.ratingJson_txt);

        /*tagParq = spark.read().parquet(Conf.tag_parqPATH);
        tasteParq = spark.read().parquet(Conf.taste_parqPATH);
        songParq = spark.read().parquet(Conf.song_parqPATH);


        /*//******************************************
        songParq.createOrReplaceTempView("song");

        tasteParq.createOrReplaceTempView("tastestats");

        tagParq.createOrReplaceTempView("tag");*/

        userJson.createOrReplaceTempView("userjson");

        songJson.createOrReplaceTempView("songjson");

        tagJson.createOrReplaceTempView("tagjson");

        ratingJson.createOrReplaceTempView("ratingjson");

        //*****************************************


    }

    public static String generateParquet() {

        final JavaRDD<String> songRead = spark.read().textFile(Conf.tracks_txt).toJavaRDD();
        final JavaRDD<String> tasteStatRead = spark.read().textFile(Conf.tasteStat_Txt).toJavaRDD();
        final JavaRDD<String> tagRead = spark.read().textFile(Conf.tag_txt).javaRDD();

        JavaRDD<Song> songRDD = songRead
                .map(lineRAW -> {
                    String[] parts = lineRAW.split(",");
                    return new Song(parts[0], parts[1], parts[2], parts[3]);
                }).persist(MEMORY_ONLY).cache();

        Dataset<Row> songDF = spark.createDataFrame(songRDD, Song.class);
        songDF.write().parquet(Conf.song_parqPATH);

        //******************************************

        JavaRDD<TasteStat> tasteRDD = tasteStatRead.map(lineRAW -> {
            String[] parts = lineRAW.split(",");
            return new TasteStat(parts[1], parts[0], Integer.parseInt(parts[2]));
        });

        Dataset<Row> tasteDF = spark.createDataFrame(tasteRDD, TasteStat.class);
        tasteDF.write().parquet(Conf.taste_parqPATH);

        //******************************************

        JavaRDD<Tag> tagRDD = tagRead
                .map(lineRAW -> {
                    String[] parts = lineRAW.split(",");
                    return new Tag(parts[0], parts[1]);
                }).persist(MEMORY_ONLY).cache();

        Dataset<Row> tagDF = spark.createDataFrame(tagRDD, Tag.class);
        tagDF.write().parquet(Conf.tag_parqPATH);
        return "ok";
    }


}