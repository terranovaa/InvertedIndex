package it.unipi;

import it.unipi.utils.Constants;

import java.io.IOException;

public class Main {

    private static final String collectionPath = "./collection/collection.tar.gz";
    private static final String stopwordsPath = "./resources/stopwords.txt";

    public static void main(String[] args) throws IOException {
        Indexer indexer;
        if(args.length == 1) {
            if (!args[0].equals(Constants.DAT_FORMAT) && !args[0].equals(Constants.TXT_FORMAT)) {
                throw new RuntimeException("File format not supported");
            }
            indexer = new Indexer(args[0]);
        } else //default case // TODO default case should be BINARY
            indexer = new Indexer(Constants.DAT_FORMAT);
        long start = System.currentTimeMillis();
        indexer.indexCollection();
        long end = System.currentTimeMillis();
        System.out.println("Indexed in " + (end - start) + " ms");
        indexer.mergeBlocks();
    }
}