package it.unipi;

import it.unipi.indexer.Indexer;
import it.unipi.indexer.IndexerBinary;
import it.unipi.indexer.IndexerTextual;
import it.unipi.query.processor.QueryProcessor;
import it.unipi.utils.Constants;
import it.unipi.utils.FileSystemUtils;

import java.io.IOException;

public class Main {
    // default options
    private static boolean stemming = true;
    private static boolean stopwords_removal = true;

    public static void main(String[] args) throws IOException {
        args = new String[]{"index"};
        if (args.length == 0){
            // default case: no args => indexing, DAT format, with stemming and stopword removal
            index(Constants.DAT_FORMAT);
        } else {
            if (args[0].equals("index")){
                if (args.length == 1) {
                    // default case: index with no specifications => DAT format
                    System.out.println("Using default extension...");
                    index(Constants.DAT_FORMAT);
                } else if(args.length >= 2) {
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
        FileSystemUtils.setupEnvironment();
        Indexer indexer = (fileFormat.equalsIgnoreCase(Constants.TXT_FORMAT)) ? new IndexerTextual() : new IndexerBinary();
        long startIndexing = System.currentTimeMillis();
        indexer.indexCollection(stemming, stopwords_removal);
        long endIndexing = System.currentTimeMillis();
        System.out.println("Indexed in " + (endIndexing - startIndexing) + " ms");
        long startMerge = System.currentTimeMillis();
        indexer.merge();
        long endMerge = System.currentTimeMillis();
        System.out.println("Merged in " + (endMerge - startMerge) + " ms");
        long startRefineIndex = System.currentTimeMillis();
        indexer.refineIndex();
        long endRefineIndex = System.currentTimeMillis();
        System.out.println("Refined in " + (endRefineIndex - startRefineIndex) + " ms");
        FileSystemUtils.deleteTemporaryFolders();
    }
}