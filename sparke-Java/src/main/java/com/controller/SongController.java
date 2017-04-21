package com.controller;

import com.SparkEngine.Recommendation;
import com.logging.UserLog;
import spark.Request;
import spark.Response;

import static spark.Spark.before;
import static spark.Spark.get;

/**
 * Created by alikemal on 19.03.2017.
 */
public class SongController {
    private final UserLog loging = new UserLog();

    public SongController(final Recommendation sparkService) {
        before("/*", (Request request, Response response) -> {
            response.type("application/json");
        });
        get("/songs", (request, response) -> sparkService.getSongs());
        get("/liststat", (Request request, Response response) -> sparkService.liststatnumber());
        get("/userstat", (request, response) -> sparkService.getStats());
        get("/getsong", (request, response) -> {
            loging.insert(request.queryMap().get("userID").value(), request.queryMap().get("trackID").value());
            return sparkService.getSongbyTrackID("TRMMMHY12903CB53F1");
        });
    }
}

