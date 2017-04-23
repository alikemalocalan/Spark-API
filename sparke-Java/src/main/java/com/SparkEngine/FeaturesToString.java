package com.SparkEngine;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;

class FeaturesToString implements Function<Tuple2<Object, double[]>, String> {
    @Override
    public String call(Tuple2<Object, double[]> element) {
        return element._1() + "," + Arrays.toString(element._2());
    }
}
