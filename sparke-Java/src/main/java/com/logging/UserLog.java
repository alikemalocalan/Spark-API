package com.logging;


import com.model.UserStat;
import com.services.BaseService;

import java.util.Date;

/**
 * Created by alikemal on 04.04.2017.
 */

public class UserLog extends BaseService<UserStat> {
    private final static String collectionName = "userstat";

    public UserLog() {
        super(collectionName);
    }

    public void insert(String userid, String trackID) {
        insert(new UserStat(trackID, userid, new Date()));
    }

    public void delete(String userid, String trackID) {
        for (UserStat stat : findByUserID(userid))
            if (stat.getTrackID().equals(trackID))
                deleteByObjectID(stat.get_id());
    }
}
