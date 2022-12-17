package it.unipi;

import it.unipi.indexer.Indexer;
import it.unipi.indexer.IndexerBinary;
import it.unipi.indexer.IndexerTextual;
import it.unipi.queryProcessor.QueryProcessor;
import it.unipi.utils.Constants;
import it.unipi.utils.FileSystemUtils;

import java.io.IOException;

public class Main {
    // default options
    private static boolean stemming = true;
    private static boolean stopwords_removal = true;

    public static void main(String[] args) throws IOException {
        args = new String[]{"query"};
        if (args.length == 0){
            // default case: no args => indexing, DAT format, with stemming and stopword removal
            index(Constants.DAT_FORMAT);
        } else {
            if (args[0].equals("index")){
                if (args.length == 1) {
                    // default case: index with no specifications => DAT format with stemming and stopword removal
                    System.out.println("Using default extension...");
                    index(Constants.DAT_FORMAT);
                } else if(args.length == 2) {
                    // default case: index with file specification => with stemming and stopword removal
                    if (!args[1].equalsIgnoreCase(Constants.DAT_FORMAT) && !args[1].equalsIgnoreCase(Constants.TXT_FORMAT))
                        throw new RuntimeException("File format not supported..");
                    index(args[1]);
                } else if(args.length > 2) { // stemming flag
                    if (args[2].equals("true")){}
                    else if (args[2].equals("false")) {
                        System.out.println("Disabling stemming flag");
                        stemming = false;
                    }
                    else throw new RuntimeException("Stemming flag requires boolean value..");
                    if (args[3] != null) {
                        if (args[3].equals("true")) {}
                        else if (args[3].equals("false")) {
                            System.out.println("Disabling stopwords removal flag");
                            stopwords_removal = false;
                        }
                        else throw new RuntimeException("Stopwords removal flag requires boolean value..");
                    }
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
        FileSystemUtils.deleteTemporaryFolders();
    }
}