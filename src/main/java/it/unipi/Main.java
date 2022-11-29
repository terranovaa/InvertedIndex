package it.unipi;

import java.io.IOException;

public class Main {

    private static final String collectionPath = "./collection/collection.tar";

    public static void main(String[] args) throws IOException {
        Indexer indexer = new Indexer();
        indexer.indexCollection(collectionPath);
    }
}