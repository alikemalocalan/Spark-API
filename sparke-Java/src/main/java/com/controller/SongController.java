package com.controller;

import com.SparkEngine.Recommendation;
import spark.Request;
import spark.Response;
import spark.Route;

import static spark.Spark.get;

/**
 * Created by alikemal on 19.03.2017.
 */
public class SongController {

    public SongController(final Recommendation sparkService) {
        get("/songs", (request, response) -> {
            response.type("application/json");
            return new Recommendation().getSongs();
        });
    }
}

