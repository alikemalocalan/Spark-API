package com.services;

import com.connetion.MongoDB;
import org.bson.types.ObjectId;
import org.jongo.MongoCursor;

/**
 * Created by alikemal on 12.04.2017.
 */
public abstract class BaseService<T> {
    private MongoDB db = MongoDB.getIntance();
    private String collectionName;
    private Class<T> clazz;

    public BaseService(String collectionName) {
        this.collectionName = collectionName;
    }

    private String getCollectionName() {
        return collectionName;
    }

    private void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public MongoCursor<T> listByUserID(String userid) {
        return db.getColletion(getCollectionName()).find("{userID: '" + userid + "'").as(clazz);
    }

    protected MongoCursor<T> findByUserID(String userid) {
        return db.getColletion(getCollectionName()).find("{userID: '" + userid + "'").as(clazz);
    }

    protected void insert(T t) {
        db.getColletion(getCollectionName()).insert(t);
    }

    protected void deleteByObjectID(String objectID) {
        db.getColletion(getCollectionName()).remove(new ObjectId(objectID));
    }
}
