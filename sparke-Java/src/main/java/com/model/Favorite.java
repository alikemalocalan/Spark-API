package com.model;

import java.util.Date;

/**
 * Created by alikemal on 12.04.2017.
 */
public class Favorite {
    private String _id;
    private String trackID;
    private String userID;
    private Date addingDate;

    public Favorite(String trackID, String userID, Date addingDate) {
        this.trackID = trackID;
        this.userID = userID;
        this.addingDate = addingDate;
    }

    public Favorite(String trackID, String userID) {

        this.trackID = trackID;
        this.userID = userID;
    }

    public Favorite() {

    }

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
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

    public Date getAddingDate() {
        return addingDate;
    }

    public void setAddingDate(Date addingDate) {
        this.addingDate = addingDate;
    }
}
