package com.model;

/**
 * Created by alikemal on 29.03.2017.
 */
public class Tag {
    private String trackID;
    private String tagID;

    public Tag() {
    }

    public Tag(String trackID, String tagID) {
        this.trackID = trackID;
        this.tagID = tagID;
    }

    public String getTrackID() {
        return trackID;
    }

    public void setTrackID(String trackID) {
        this.trackID = trackID;
    }

    public String getTagID() {
        return tagID;
    }

    public void setTagID(String tagID) {
        this.tagID = tagID;
    }
}
