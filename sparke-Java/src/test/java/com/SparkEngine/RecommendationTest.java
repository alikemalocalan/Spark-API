package com.SparkEngine;


import org.junit.Test;

import static java.lang.System.out;


/**
 * Created by alikemal on 19.03.2017.
 */
public class RecommendationTest {
    @Test
    public void song() throws Exception {
        //new Recommendation().getSongs().collectAsList().toString();
        out.println(new Recommendation().getSongs());
    }
}