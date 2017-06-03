package com.model;

/**
 * Created by alikemal on 01.05.2017.
 */
public class User {
    private String userID,signup,sex;
    private String country;
    private int age;

    public User(String userID, String country, String signup, String sex, int age) {
        this.userID = userID;
        this.country = country;
        this.signup = signup;
        this.sex = sex;
        this.age = age;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getSignup() {
        return signup;
    }

    public void setSignup(String signup) {
        this.signup = signup;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
