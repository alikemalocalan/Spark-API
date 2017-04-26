package com;

import com.SparkEngine.InitSpark;
import com.SparkEngine.Recommendation;
import com.controller.SongController;

import static spark.Spark.get;

/**
 * Created by alikemal on 19.03.2017.
 */

public class App {
    public static void main(String[] args) {
        get("/", (req, res) -> "Hello World");
        InitSpark ınitSpark = new InitSpark();
        Recommendation recommendation = new Recommendation();
        new SongController(recommendation);
    }
}