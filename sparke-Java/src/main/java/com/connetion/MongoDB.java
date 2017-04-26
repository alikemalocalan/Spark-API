package com.connetion;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

/**
 * Created by alikemal on 12.04.2017.
 */
public class MongoDB {
    private static MongoDB intance = new MongoDB();
    private DB db;
    private Jongo jongo;

    public MongoDB() {
        MongoClient mongoClient = new MongoClient("139.162.158.192", 27017);
        db = mongoClient.getDB("ali_bitirme");
        jongo = new Jongo(db);
    }

    public static MongoDB getIntance() {
        return intance;
    }

    public static void setIntance(MongoDB intance) {
        MongoDB.intance = intance;
    }

    public MongoCollection getColletion(String collectionName) {
        return jongo.getCollection(collectionName);
    }
}
