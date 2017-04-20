package com.model;

/**
 * Created by alikemal on 29.03.2017.
 */
public class TasteStat {
    private String songID;
    private String userID;
    private int rating;

    public TasteStat() {
    }

    public TasteStat(String trackID, String userID, int rating) {
        this.songID = trackID;
        this.userID = userID;
        this.rating = rating;
    }

    public String getSongID() {
        return songID;
    }

    public void setSongID(String songID) {
        this.songID = songID;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }
}
