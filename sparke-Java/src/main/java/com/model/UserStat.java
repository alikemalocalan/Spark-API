package com.model;

import java.util.Date;

/**
 * Created by alikemal on 29.03.2017.
 */
public class UserStat {
    private final int LoggingID = 0;
    private String _id;
    private String trackID;
    private String userID;
    private Date addingDate;

    public UserStat() {
    }

    public UserStat(String trackID, String userID) {
        this.trackID = trackID;
        this.userID = userID;
    }

    public UserStat(String trackID, String userID, Date addingDate) {
        this.trackID = trackID;
        this.userID = userID;
        this.addingDate = addingDate;
    }

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public int getLoggingID() {
        return LoggingID;
    }

    public Date getAddingDate() {
        return addingDate;
    }

    public void setAddingDate(Date addingDate) {
        this.addingDate = addingDate;
    }

    public String getTrackID() {
        return trackID;
    }

    public void setTrackID(String trackID) {
        this.trackID = trackID;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }
}
