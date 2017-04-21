package com.SparkEngine;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * Created by alikemal on 21.04.2017.
 */
public class Recommendation {

    private Dataset<Row> result = InitSpark.spark.sql("SELECT *   " +
            "FROM tastestats st" +
            "   INNER JOIN song s ON" +
            "   st.songID= s.songID" +
            "   INNER JOIN tag t ON" +
            "   s.trackID= t.trackID " +
            "   limit 100");

    public List<String> getSongs() {
        Dataset<Row> result = InitSpark.spark.sql("SELECT * FROM song limit 100");
        return result.toJSON().collectAsList();
    }

    public List<String> getSongbyTrackID(String trackID) {
        Dataset<Row> result = InitSpark.spark.sql("SELECT * FROM song limit 100").filter(new Column("trackID").equalTo(trackID));
        return result.toJSON().collectAsList();
    }

    public List<String> getStats() {
        return result.toJSON().collectAsList();
    }

    public List<String> liststatnumber() {
        Dataset<Row> rs = InitSpark.spark.sql(
                "SELECT songID, row_number() OVER ( ORDER BY songID) as id" +
                        " FROM song group by songID ");

        return rs.toJSON().collectAsList();
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