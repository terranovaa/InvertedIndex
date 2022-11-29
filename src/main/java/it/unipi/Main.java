package it.unipi;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;

public class Main {

    private static final String collectionPath = "./collection/collection.tar";

    public static void main(String[] args) throws IOException {
        Indexer indexer = new Indexer();
        indexer.indexCollection(collectionPath);
    }
}