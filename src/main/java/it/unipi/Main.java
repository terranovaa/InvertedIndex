package it.unipi;

import it.unipi.utils.Utils;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {

    private static final String collectionPath = "./collection/collection.tar.gz";
    private static final String stopwordsPath = "./resources/stopwords.txt";

    public static void main(String[] args) throws IOException {
        Indexer indexer = new Indexer(stopwordsPath);
        indexer.indexCollection(collectionPath);
    }
}