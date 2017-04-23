package com.model;

import java.io.Serializable;

/**
 * Created by alikemal on 23.04.2017.
 */
public class Rating implements Serializable {
    private int userID;
    private int songID;
    private int rating;
    private int tagID;

    public Rating(int userID, int songID, int rating, int tagID) {
        this.userID = userID;
        this.songID = songID;
        this.rating = rating;
        this.tagID = tagID;
    }

    public Rating() {
    }

    public int getUserID() {
        return userID;
    }

    public void setUserID(int userID) {
        this.userID = userID;
    }

    public int getSongID() {
        return songID;
    }

    public void setSongID(int songID) {
        this.songID = songID;
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

    public int getTagID() {
        return tagID;
    }

    public void setTagID(int tagID) {
        this.tagID = tagID;
    }
}
