package it.unipi;

import it.unipi.indexer.Indexer;
import it.unipi.indexer.IndexerBinary;
import it.unipi.indexer.IndexerTextual;
import it.unipi.utils.Constants;
import it.unipi.utils.Utils;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {

        if (args.length >= 1) {
            if (args[0].equals("index")){
                // TODO: add support to different collection?
                System.out.println("Starting indexer for the default collection..");
                if (!args[1].equalsIgnoreCase(Constants.DAT_FORMAT) && !args[1].equalsIgnoreCase(Constants.TXT_FORMAT)) {
                    throw new RuntimeException("File format not supported..");
                }
                index(args[1]);
            }
            else if (args[0].equals("query")){
                System.out.println("Starting the query processor..");
                QueryProcessor qp = new QueryProcessor();
                qp.commandLine();
            }
            else throw new RuntimeException("Operation not supported..");
        } else { // default case, no args => indexing
            index(Constants.DAT_FORMAT);
        }
    }

    public static void index(String fileFormat) throws IOException{
        Utils.setupEnvironment();
        Indexer indexer = (fileFormat.equals(Constants.TXT_FORMAT)) ? new IndexerTextual() : new IndexerBinary();
        long startIndexing = System.currentTimeMillis();
        indexer.indexCollection();
        long endIndexing = System.currentTimeMillis();
        System.out.println("Indexed in " + (endIndexing - startIndexing) + " ms");
        long startMerge = System.currentTimeMillis();
        indexer.merge();
        long endMerge = System.currentTimeMillis();
        System.out.println("Merged in " + (endMerge - startMerge) + " ms");
        Utils.deleteTemporaryFolders();
    }
}