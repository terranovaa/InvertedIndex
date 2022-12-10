package it.unipi;

import it.unipi.utils.Constants;
import java.io.IOException;
import java.util.Arrays;
import static it.unipi.utils.Utils.setupEnvironment;

public class Main {

    public static void main(String[] args) throws IOException {
        // DEBUG
        args = new String[]{"index", ".TXT"};

        if (args.length >= 1) {
            if (args[0].equals("index")){
                // TODO: add support to different collection?
                System.out.println("Starting indexer for the default collection..");
                index(Arrays.copyOfRange(args, 1, args.length));
            }
            else if (args[0].equals("query")){
                System.out.println("Starting the query processor..");
                QueryProcessor qp = new QueryProcessor();
                qp.commandLine();
            }
            else throw new RuntimeException("Operation not supported..");
        } else { // default case, no args => indexing
            index(args);
        }
    }

    public static void index(String[] args) throws IOException{
        setupEnvironment();
        Indexer indexer;
        if(args.length == 1) {
            if (!args[0].equalsIgnoreCase(Constants.DAT_FORMAT) && !args[0].equalsIgnoreCase(Constants.TXT_FORMAT)) {
                throw new RuntimeException("File format not supported..");
            }
            indexer = new Indexer(args[0]);
        } else // default case binary
            indexer = new Indexer(Constants.DAT_FORMAT);
        long startIndexing = System.currentTimeMillis();
        indexer.indexCollection();
        long endIndexing = System.currentTimeMillis();
        System.out.println("Indexed in " + (endIndexing - startIndexing) + " ms");
        long startMerge = System.currentTimeMillis();
        indexer.mergeBlocks();
        long endMerge = System.currentTimeMillis();
        System.out.println("Merged in " + (endMerge - startMerge) + " ms");
    }
}