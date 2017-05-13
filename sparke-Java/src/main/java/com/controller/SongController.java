package com.controller;

import com.SparkEngine.InitSpark;
import com.SparkEngine.Recommendation;

import static spark.Spark.after;
import static spark.Spark.get;

/**
 * Created by alikemal on 19.03.2017.
 */
public class SongController {

    public SongController(final Recommendation sparkService) {
        after("/*", (request, response) -> {
            response.type("application/json");
        });
        get("/songs", (request, response) -> sparkService.getSongs());
        get("/test", (request, response) -> sparkService.getRecommend());
        get("/init", (request, response) -> InitSpark.generateParquet() );
        get("/songbyID", (request, response) -> {
            //loging.insert(request.queryMap().get("userID").value(), request.queryMap().get("trackID").value());
            return sparkService.getSongbyTrackID("TRMMMHY12903CB53F1");
        });
    }
}

