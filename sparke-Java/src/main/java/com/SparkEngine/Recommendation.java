package com.SparkEngine;

/**
 * Created by alikemal on 19.03.2017.
 */

import com.model.Song;
import com.model.Tag;
import com.model.TasteStat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Recommendation {
    private final Logger LOGGER = LoggerFactory.getLogger(Recommendation.class);
    private final String base_txt = "/home/alikemal/IdeaProjects/Spark-API/dataset/";

    private final String tag_txt = base_txt + "msd-MAGD-genreAssignment-new.cls";
    private final String tasteStat_Txt = base_txt + "train_triplets.aa.tsv";
    private final String jamStat_txt = base_txt + "jam_to_msd-new.tsv";
    private final String tracks_txt = base_txt + "unique_tracks.tsv";

    private static final String master = "local";
    private SparkSession spark = null;

    private final JavaRDD<String> songRead;
    private final JavaRDD<String> tasteStatRead;
    private final JavaRDD<String> tagRead;
    private final JavaRDD<String> jamStatRead;

    private JavaRDD<Song> songRDD = null;
    private JavaRDD<TasteStat> tasteRDD = null;
    private JavaRDD<Tag> tagRDD = null;

    Dataset<Row> result;

    public Recommendation() {

        SparkConf conf = new SparkConf()
                .setAppName(Recommendation.class.getName())
                .setMaster(master);
        spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();


        //******************************************
        songRead = spark.read().textFile(tracks_txt).toJavaRDD();
        tasteStatRead = spark.read().textFile(tasteStat_Txt).toJavaRDD();
        tagRead = spark.read().textFile(tag_txt).javaRDD();
        jamStatRead = spark.read().textFile(jamStat_txt).javaRDD();

        songRDD = songRead
                .map(lineRAW -> {
                    String[] parts = lineRAW.split(",");
                    return new Song(parts[0], parts[1], parts[2], parts[3]);
                });

        Dataset<Row> songDF = spark.createDataFrame(songRDD, Song.class);
        songDF.createOrReplaceTempView("song");

        //******************************************


        tasteRDD = tasteStatRead.map(lineRAW -> {
            String[] parts = lineRAW.split(",");
            return new TasteStat(parts[1], parts[0], Integer.parseInt(parts[2]));
        });

        Dataset<Row> songPriceDF = spark.createDataFrame(tasteRDD, TasteStat.class);
        songPriceDF.createOrReplaceTempView("tastestats");


        //******************************************


        tagRDD = tagRead
                .map(lineRAW -> {
                    String[] parts = lineRAW.split(",");
                    return new Tag(parts[0], parts[1]);
                });

        Dataset<Row> tagDF = spark.createDataFrame(tagRDD, Tag.class);
        tagDF.createOrReplaceTempView("tag");


        //******************************************

        /*
        JavaRDD<UserStat> userRDD = jamStatRead
                .map(lineRAW -> {
                    String[] parts = lineRAW.split(",");
                    return new UserStat(parts[1], parts[0]);
                });

        Dataset<Row> userDf = spark.createDataFrame(userRDD, UserStat.class);
        userDf.createOrReplaceTempView("userStat");
        */

        result = spark.sql("SELECT *   " +
                "FROM tastestats st" +
                "   INNER JOIN song s ON" +
                "   st.songID= s.songID" +
                "   INNER JOIN tag t ON" +
                "   s.trackID= t.trackID " +
                "   limit 100");
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
        return result.toJSON().collectAsList();
    }

    public List<String> recommenbyGenre() {
        // Build the recommendation model using ALS
        JavaRDD<Rating> ratings = tasteRDD.map(
                (Function<TasteStat, Rating>) s -> new Rating(
                        Integer.parseInt(s.getUserID()),
                        Integer.parseInt(s.getSongID()),
                        s.getRating())
        );

        int rank = 10;
        int numIterations = 10;
        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);

        return null;
    }
}