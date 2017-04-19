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
    private static final String master = "local";
    private SparkSession spark = null;

    public Recommendation() {

        SparkConf conf = new SparkConf()
                .setAppName(Recommendation.class.getName())
                .setMaster(master);
        spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();


        final JavaRDD<String> songRead = spark.read().textFile(tracks_txt).toJavaRDD();
        final JavaRDD<String> userSalesRead = spark.read().textFile(price_txt).toJavaRDD();
        final JavaRDD<String> tagRead = spark.read().textFile(tag_txt).javaRDD();
        final JavaRDD<String> userRead = spark.read().textFile(userStat_txt).javaRDD();

        //******************************************


        JavaRDD<Song> songRDD = songRead
                .map(lineRAW -> {
                    String[] parts = lineRAW.split(",");
                    return new Song(parts[0], parts[1], parts[2], parts[3]);
                });

        Dataset<Row> songDF = spark.createDataFrame(songRDD, Song.class);
        songDF.createOrReplaceTempView("song");

        //******************************************


        JavaRDD<UserSales> salesRDD = userSalesRead.map(lineRAW -> {
            String[] parts = lineRAW.split(",");
            return new UserSales(parts[1], parts[0], parts[2]);
        });

        Dataset<Row> songPriceDF = spark.createDataFrame(salesRDD, UserSales.class);
        songPriceDF.createOrReplaceTempView("usersongPrice");


        //******************************************


        JavaRDD<Tag> tagRDD = tagRead
                .map(lineRAW -> {
                    String[] parts = lineRAW.split(",");
                    return new Tag(parts[0], parts[1]);
                });

        Dataset<Row> tagDF = spark.createDataFrame(tagRDD, Tag.class);
        tagDF.createOrReplaceTempView("tag");


        //******************************************


        JavaRDD<UserStat> userRDD = userRead
                .map(lineRAW -> {
                    String[] parts = lineRAW.split(",");
                    return new UserStat(parts[1], parts[0]);
                });

        Dataset<Row> userDf = spark.createDataFrame(userRDD, UserStat.class);
        userDf.createOrReplaceTempView("userStat");
    }

    public List<String> getSongs() {
        Dataset<Row> result = spark.sql("SELECT * FROM song limit 100");
        return result.toJSON().collectAsList();
    }

    public String getSongbyTrackID(String trackID) {
        Dataset<Row> result = spark.sql("SELECT * FROM song limit 100").filter(new Column("trackID").equalTo(trackID));
        return result.toJSON().collectAsList().get(0);
    }

    public List<String> getStats() {
        Dataset<Row> result = spark.sql("SELECT *   " +
                "FROM userStat st" +
                "   INNER JOIN tag t ON" +
                "   st.trackID= t.trackID " +
                "   INNER JOIN song s ON" +
                "   st.trackID= s.trackID" +
                "   INNER JOIN usersongPrice p ON" +
                "   s.songID= p.songID" +
                "   limit 100");
        return result.toJSON().collectAsList();
    }
}