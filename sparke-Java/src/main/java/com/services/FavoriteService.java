package com.services;

import com.model.Favorite;

import java.util.Date;

/**
 * Created by alikemal on 12.04.2017.
 */

public class FavoriteService extends BaseService<Favorite> {
    private final static String collectionName = "favorite";

    public FavoriteService() {
        super(collectionName);
    }

    public void insert(String userid, String trackID) {
        insert(new Favorite(trackID, userid, new Date()));
    }

    public void delete(String userid, String trackID) {
        for (Favorite fav : findByUserID(userid))
            if (fav.getTrackID().equals(trackID))
                deleteByObjectID(fav.get_id());
    }
}
