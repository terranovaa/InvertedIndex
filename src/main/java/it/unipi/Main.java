package it.unipi;

import it.unipi.utils.Constants;
import java.io.IOException;

import static it.unipi.utils.Utils.setupEnvironment;

public class Main {
    public static void main(String[] args) throws IOException {
        setupEnvironment();
        Indexer indexer;
        if(args.length == 1) {
            if (!args[0].equals(Constants.DAT_FORMAT) && !args[0].equals(Constants.TXT_FORMAT)) {
                throw new RuntimeException("File format not supported");
            }
            indexer = new Indexer(args[0]);
        } else // default case binary
            indexer = new Indexer(Constants.DAT_FORMAT);
        long start = System.currentTimeMillis();
        indexer.indexCollection();
        long end = System.currentTimeMillis();
        System.out.println("Indexed in " + (end - start) + " ms");
        indexer.mergeBlocks();
    }
}