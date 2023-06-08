package com.wolffy.es.bean;

public class Movie {
    private String id;
    private String MovieName;

    public Movie() {
    }

    public Movie(String id, String movieName) {
        this.id = id;
        MovieName = movieName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getMovieName() {
        return MovieName;
    }

    public void setMovieName(String movieName) {
        MovieName = movieName;
    }
}
