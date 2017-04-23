package com.SparkEngine;


import com.google.gson.Gson;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by alikemal on 21.04.2017.
 */
public class Recommendation {
    JavaRDD<Rating> ratings;

    public Recommendation() {
        ratings = rating().toJavaRDD().map(row ->
                new Rating((int) row.getLong(4), (int) row.getLong(3), (int) row.getLong(1)));
    }

    public List<String> getSongs() {
        Dataset<Row> result = InitSpark.spark.sql("SELECT * FROM song limit 100");
        return result.toJSON().collectAsList();
    }

    public List<String> getSongbyTrackID(String trackID) {
        Dataset<Row> result = InitSpark.spark.sql("SELECT * FROM song limit 100").filter(new Column("trackID").equalTo(trackID));

        return result.toJSON().collectAsList();
    }

    public List<String> liststatnumber() {
        Dataset<Row> rs = InitSpark.spark.sql(
                "SELECT songID, row_number() OVER ( ORDER BY songID) as id" +
                        " FROM song group by songID ");

        return rs.toJSON().collectAsList();
    }

    public Dataset<Row> rating() {
        return InitSpark.spark.sql("SELECT * FROM ratingjson order by userID limit 200");
    }

    public String getRecommend() {
        final int rank = 5, iterations = 1, blocks = -1;
        ArrayList<String> result = new ArrayList<>();

        MatrixFactorizationModel model = ALS.train(ratings.rdd(), rank, iterations, 0.01, blocks);

        return new Gson().toJson(
                model.productFeatures().toJavaRDD().map(element ->
                        (element._1() + "," + Arrays.toString(element._2())))
                        .collect());
    }

}


/*
"SELECT u.id,s.id,ts.rating, tj.id " +
                "FROM tastestats ts" +
                "   INNER JOIN userjson u ON" +
                "   ts.userID= u.userID" +
                "   INNER JOIN songjson s ON" +
                "   ts.songID= s.songID "  +
                "   INNER JOIN song sg ON" +
                "   sg.songID= s.songID "  +
                "   INNER JOIN tag t ON" +
                "   sg.trackID= t.trackID "+
                "   INNER JOIN tagjson tj ON" +
                "   t.tagID= tj.tagID ");*/

// "SELECT tagID, row_number() OVER ( ORDER BY tagID) as id FROM tag group by tagID ");

/*
"SELECT *   " +
            "FROM tastestats st" +
            "   INNER JOIN song s ON" +
            "   st.songID= s.songID" +
            "   INNER JOIN tag t ON" +
            "   s.trackID= t.trackID " +
            "   limit 100"
 */