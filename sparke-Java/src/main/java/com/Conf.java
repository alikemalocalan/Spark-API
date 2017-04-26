package com;

/**
 * Created by alikemal on 21.04.2017.
 */
public class Conf {
    public static final String base_txt = "/home/alikemal/IdeaProjects/Spark-API/dataset/";
    public static final String tag_txt = base_txt + "msd-MAGD-genreAssignment-new.cls";
    public static final String tasteStat_Txt = base_txt + "train_triplets.aa.tsv";
    public static final String jamStat_txt = base_txt + "jam_to_msd-new.tsv";
    public static final String tracks_txt = base_txt + "unique_tracks.tsv";
    public static final String usersJson_txt = base_txt + "users.json";
    public static final String songJson_txt = base_txt + "songs.json";
    public static final String tagJson_txt = base_txt + "tags.json";
    public static final String ratingJson_txt = base_txt + "rating.json";

    public static final String tag_parqPATH = base_txt + "tagDF.parquet";
    public static final String taste_parqPATH = base_txt + "tasteDF.parquet";
    public static final String song_parqPATH = base_txt + "songDF.parquet";

}
