package com.SparkEngine;

/**
 * Created by alikemal on 19.03.2017.
 */

import com.model.Song;
import com.model.Tag;
import com.model.UserSales;
import com.model.UserStat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Recommendation {
    private static final Logger LOGGER = LoggerFactory.getLogger(Recommendation.class);
    private static final String base_txt = "/home/alikemal/IdeaProjects/Spark-API/dataset/";

    private static final String tag_txt = base_txt + "msd-MAGD-genreAssignment-new.cls";
    private static final String price_txt = base_txt + "train_triplets.txtaa";
    private static final String userStat_txt = base_txt + "jam_to_msd-new.tsv";
    private static final String tracks_txt = base_txt + "unique_tracks.txt";
    private static final String master = "local[*]";
    private SparkSession spark = null;

    public Recommendation() {

        SparkConf conf = new SparkConf()
                .setAppName(Recommendation.class.getName())
                .setMaster(master);
        spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config(conf)
                .getOrCreate();

        //******************************************


        JavaRDD<Song> songRDD = spark.read()
                .textFile(tracks_txt)
                .javaRDD()
                .map(lineRAW -> {
                    String[] parts = lineRAW.split(",");
                    Song line = new Song();
                    line.setTrackID(parts[0]);
                    line.setSongID(parts[1]);
                    line.setSongTitle(parts[2]);
                    line.setArtistTitle(parts[3]);
                    return line;
                });

        Dataset<Row> songDF = spark.createDataFrame(songRDD, Song.class);
        songDF.createOrReplaceTempView("song");

        //******************************************


        JavaRDD<UserSales> songPriceRDD = (JavaRDD<UserSales>) spark.read()
                .textFile(price_txt)
                .javaRDD()
                .map(lineRAW -> {
                    String[] parts = lineRAW.split(",");
                    UserSales price = new UserSales();
                    price.setUserID(parts[0]);
                    price.setTrackID(parts[1]);
                    price.setPrice(parts[2]);
                    return price;
                });

        Dataset<Row> songPriceDF = spark.createDataFrame(songPriceRDD, UserSales.class);
        songPriceDF.createOrReplaceTempView("usersongPrice");


        //******************************************


        JavaRDD<Tag> tagRDD = spark.read()
                .textFile(tag_txt)
                .javaRDD()
                .map(lineRAW -> {
                    String[] parts = lineRAW.split(",");
                    Tag tag = new Tag();
                    tag.setTrackID(parts[0]);
                    tag.setTagID(parts[1]);
                    return tag;
                });

        Dataset<Row> tagDF = spark.createDataFrame(tagRDD, Tag.class);
        tagDF.createOrReplaceTempView("tag");


        //******************************************


        JavaRDD<UserStat> userRDD = spark.read()
                .textFile(userStat_txt)
                .javaRDD()
                .map(lineRAW -> {
                    String[] parts = lineRAW.split(",");
                    UserStat stat = new UserStat();
                    stat.setUserID(parts[0]);
                    stat.setTrackID(parts[1]);
                    return stat;
                });

        Dataset<Row> userDf = spark.createDataFrame(userRDD, UserStat.class);
        userDf.createOrReplaceTempView("userStat");
    }

    public List<String> getSongs() {
        Dataset<Row> result = spark.sql("SELECT * FROM song limit 1");
        return result.toJSON().collectAsList();
    }

    public String getSongbyTrackID(String trackID) {
        Dataset<Row> result = spark.sql("SELECT * FROM song limit 1").filter(new Column("trackID").equalTo(trackID));
        return result.toJSON().collectAsList().get(0);
    }

    public List<String> getStats() {
        Dataset<Row> result = spark.sql("SELECT * " +
                "FROM userStat st" +
                " INNER JOIN tag t ON" +
                " st.trackID= t.trackID " +
                " INNER JOIN song p ON" +
                " t.trackID= p.trackID " +
                "limit 10");
        return result.toJSON().collectAsList();
    }
}