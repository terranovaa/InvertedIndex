package it.unipi;

import it.unipi.indexer.Indexer;
import it.unipi.indexer.BinaryIndexer;
import it.unipi.indexer.TextualIndexer;
import it.unipi.query.processor.QueryProcessor;
import it.unipi.utils.Constants;
import it.unipi.utils.FileSystemUtils;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        if (args.length == 0){
            // default case: no args => indexing, DAT format
            index(Constants.DAT_FORMAT);
        } else {
            if (args[0].equals("index")){
                if (args.length == 1) {
                    // default case: index with no specifications => DAT format
                    System.out.println("Using default extension...");
                    index(Constants.DAT_FORMAT);
                } else {
                    // default case: index with file specification
                    if (!args[1].equalsIgnoreCase(Constants.DAT_FORMAT) && !args[1].equalsIgnoreCase(Constants.TXT_FORMAT))
                        throw new RuntimeException("File format not supported..");
                    index(args[1]);
                }
            } else if (args[0].equals("query")){
                System.out.println("Starting the query processor..");
                QueryProcessor qp = new QueryProcessor();
                qp.commandLine();
            }  else throw new RuntimeException("Operation not supported..");
        }
    }

    public static void index(String fileFormat) throws IOException{

        // creating the folders
        FileSystemUtils.setupEnvironment();

        Indexer indexer = (fileFormat.equalsIgnoreCase(Constants.TXT_FORMAT)) ? new TextualIndexer() : new BinaryIndexer();

        // indexing
        long startIndexing = System.currentTimeMillis();
        indexer.indexCollection();
        long endIndexing = System.currentTimeMillis();
        System.out.println("Indexed in " + (endIndexing - startIndexing) + " ms");

        // merging
        long startMerge = System.currentTimeMillis();
        indexer.merge();
        long endMerge = System.currentTimeMillis();
        System.out.println("Merged in " + (endMerge - startMerge) + " ms");

        // refining
        long startRefineIndex = System.currentTimeMillis();
        indexer.refineIndex();
        long endRefineIndex = System.currentTimeMillis();
        System.out.println("Refined in " + (endRefineIndex - startRefineIndex) + " ms");

        // deleting the folders
        FileSystemUtils.deleteTemporaryFolders();
    }
}