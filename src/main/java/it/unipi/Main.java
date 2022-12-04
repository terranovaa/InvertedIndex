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
        Indexer indexer;
        // TODO: is this what it refers to, talking about compile flags?
        if(args.length == 2) {
            System.out.println("The user has provided flags for stopwords and stemming");
            indexer = new Indexer(Boolean.parseBoolean(args[0]), stopwordsPath, Boolean.parseBoolean(args[1]));
        } else //default case
            indexer = new Indexer( true, stopwordsPath,true);
        indexer.indexCollection(collectionPath);
        // TODO: Ideally, your program should have a compile flag that allows you to use ASCII format during debugging and binary format for performance.
    }
}