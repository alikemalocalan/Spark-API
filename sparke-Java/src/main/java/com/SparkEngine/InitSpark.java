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

    private final JavaRDD<String> songRead;
    private final JavaRDD<String> tasteStatRead;
    private final JavaRDD<String> tagRead;
    private final Dataset<Row> userJson;
    private final Dataset<Row> songJson;
    private final Dataset<Row> tagJson;

    private JavaRDD<Song> songRDD = null;
    private JavaRDD<TasteStat> tasteRDD = null;
    private JavaRDD<Tag> tagRDD = null;

    public InitSpark() {

        SparkConf conf = new SparkConf()
                .setAppName(InitSpark.class.getName())
                .setMaster("local");
        spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();


        //******************************************
        songRead = spark.read().textFile(Conf.tracks_txt).toJavaRDD();
        tasteStatRead = spark.read().textFile(Conf.tasteStat_Txt).toJavaRDD();
        tagRead = spark.read().textFile(Conf.tag_txt).javaRDD();

        userJson = spark.read().json(Conf.usersJson_txt);
        songJson = spark.read().json(Conf.songJson_txt);
        tagJson = spark.read().json(Conf.tagJson_txt);

        //****************************************
        userJson.createOrReplaceTempView("userjson");

        songJson.createOrReplaceTempView("songjson");

        tagJson.createOrReplaceTempView("tagjson");

        //*****************************************
        songRDD = songRead
                .map(lineRAW -> {
                    String[] parts = lineRAW.split(",");
                    return new Song(parts[0], parts[1], parts[2], parts[3]);
                }).persist(MEMORY_ONLY).cache();

        Dataset<Row> songDF = spark.createDataFrame(songRDD, Song.class);
        songDF.createOrReplaceTempView("song");

        //******************************************


        tasteRDD = tasteStatRead.map(lineRAW -> {
            String[] parts = lineRAW.split(",");
            return new TasteStat(parts[1], parts[0], Integer.parseInt(parts[2]));
        }).persist(MEMORY_ONLY).cache();
        ;

        Dataset<Row> songPriceDF = spark.createDataFrame(tasteRDD, TasteStat.class);
        songPriceDF.createOrReplaceTempView("tastestats");


        //******************************************


        tagRDD = tagRead
                .map(lineRAW -> {
                    String[] parts = lineRAW.split(",");
                    return new Tag(parts[0], parts[1]);
                }).persist(MEMORY_ONLY).cache();
        ;

        Dataset<Row> tagDF = spark.createDataFrame(tagRDD, Tag.class);
        tagDF.createOrReplaceTempView("tag");


        //******************************************


    }


}