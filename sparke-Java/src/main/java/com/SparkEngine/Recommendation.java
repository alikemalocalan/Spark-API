package com.SparkEngine;

/**
 * Created by alikemal on 19.03.2017.
 */

import com.model.Line;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Recommendation {
    private static final Logger LOGGER = LoggerFactory.getLogger(Recommendation.class);
    private static final String tracks_txt = "/home/alikemal/IdeaProjects/Spark-API/dataset/unique_tracks.txt";
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

        JavaRDD<Line> lineRDD = spark.read()
                .textFile(tracks_txt)
                .javaRDD()
                .map(lineRAW -> {
                    String[] parts = lineRAW.split(",");
                    Line line = new Line();
                    line.setArtistTitle(parts[2]);
                    line.setSongTitle(parts[3]);
                    return line;
                });

        Dataset<Row> lineDF = spark.createDataFrame(lineRDD, Line.class);
        lineDF.createOrReplaceTempView("song");
    }

    public List<String> getSongs() {
        Dataset<Row> result = spark.sql("SELECT * FROM song limit 1");
        return result.toJSON().collectAsList();
    }
}