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
        Tuple2<Integer, String> t = Tuple2.create(1, "one");
        System.out.println(t.toString());
    }

    public static class Tuple2<T1, T2> {
        public final T1 _1;
        public final T2 _2;

        public Tuple2(T1 _1, T2 _2) {
            this._1 = _1;
            this._2 = _2;
        }

        public static <T1, T2> Tuple2<T1, T2> create(T1 a, T2 b) {
            return new Tuple2<T1, T2>(a, b);
        }

        @Override
        public String toString() {
            return "(" + _1.toString() + ", " + _2.toString() + ")";
        }
    }
}