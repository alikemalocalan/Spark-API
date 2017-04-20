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

    @Test
    public void generic() throws Exception {
        //new Recommendation().getSongs().collectAsList().toString();
        out.println(Recommendation.class);

    }
}

class Main {
    public static void main(String[] args) {
        String isim="e34ea6717d38441ee9a8d0ad32fa042b2446f1e3";
        String soyisim= "ed7110f943d63aff393d7130604c410f3f50d8fb";
        System.out.println(isim.hashCode());
        System.out.println(soyisim.hashCode());
    }
}