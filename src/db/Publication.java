package db;

import java.util.List;

public class Publication {
    String key;
    String type;
    String venue;
    String venue_year;
    String article;
    String author;
    String title;
    String pages;
    String year;
    String volume;
    String journal;
    String booktitle;
    String number;
    String ee;
    String crossref;
    String url;


    public Publication(String key, String venue,  String article) {
        this.key = key;
        this.venue = venue;
        this.article = article;
    }

}