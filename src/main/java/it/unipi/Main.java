package it.unipi;

import java.io.IOException;

public class Main {

    private static final String collectionPath = "./collection/collection.tar.gz";
    private static final String stopwordsPath = "./resources/stopwords.txt";

    public static void main(String[] args) throws IOException {
        Indexer indexer = new Indexer(stopwordsPath);
        indexer.indexCollection(collectionPath);
    }
}