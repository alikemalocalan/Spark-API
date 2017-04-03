package com.controller;

import com.SparkEngine.Recommendation;
import com.logging.LogMong;
import spark.Request;
import spark.Response;

import static spark.Spark.before;
import static spark.Spark.get;

/**
 * Created by alikemal on 19.03.2017.
 */
public class SongController {
    private final LogMong loging= new LogMong();

    public SongController(final Recommendation sparkService) {
        before("/*", (Request request, Response response) -> {
            response.type("application/json");

        });
        get("/songs", (request, response) -> {
                        return sparkService.getSongs();
        });

        get("/userstat", (request, response) -> {
            return sparkService.getStats();
        });
        get("/getsong", (request, response) -> {
            loging.insertUserStat( request.queryMap().get("userID").value(), request.queryMap().get("trackID").value());
            return sparkService.getSongbyTrackID("TRMMMYQ128F932D901");
        });
    }
}

