package it.unipi;
import it.unipi.exceptions.IllegalQueryType;
import it.unipi.models.Document;
import it.unipi.models.LexiconTermBinaryIndexing;
import it.unipi.models.LexiconTermIndexing;
import it.unipi.utils.Constants;
import it.unipi.utils.Utils;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeMap;
import static it.unipi.utils.Utils.*;

public class QueryProcessor {
    private final String[] QUIT_CODES = new String[]{"Q", "q", "QUIT", "quit", "EXIT", "exit"};
    private final TreeMap<String, LexiconTermIndexing> lexicon = new TreeMap<>();
    private final HashMap<Integer, Document> documentTable = new HashMap<>();

    public QueryProcessor(){
        //loadDocumentTable();
        loadLexicon();
    }

    public void loadLexicon(){
        System.out.println("Loading the lexicon in memory....");
        String lexiconInputFile = Constants.LEXICON_FILE_PATH + Constants.DAT_FORMAT;
        FileInputStream lexiconStream;
        try {
            lexiconStream = new FileInputStream(lexiconInputFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        byte[] nextLexiconEntry = new byte[Constants.LEXICON_ENTRY_SIZE];
        LexiconTermBinaryIndexing nextLexicon;
        int bytesRead = 0;
        while(true) {
            try {
                bytesRead = lexiconStream.readNBytes(nextLexiconEntry, 0, Constants.LEXICON_ENTRY_SIZE);
                if (bytesRead < Constants.LEXICON_ENTRY_SIZE)
                    break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            nextLexicon = new LexiconTermBinaryIndexing();
            nextLexicon.deserialize(nextLexiconEntry);
            lexicon.put(nextLexicon.getTerm(), nextLexicon);
        }
        System.out.println("Lexicon read in memory..");
    }

    public void loadDocumentTable(){
        System.out.println("Loading the document table in memory....");
        String documentTableInputFile = Constants.DOCUMENT_TABLE_FILE_PATH + Constants.DAT_FORMAT;
        FileInputStream documentTableStream;
        try {
            documentTableStream = new FileInputStream(documentTableInputFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        byte[] nextDocumentEntry = new byte[Constants.DOCUMENT_ENTRY_SIZE];
        Document nextDocument;
        int bytesRead;
        while(true) {
            try {
                bytesRead = documentTableStream.readNBytes(nextDocumentEntry, 0, Constants.DOCUMENT_ENTRY_SIZE);
                if (bytesRead < Constants.LEXICON_ENTRY_SIZE)
                    break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            nextDocument = new Document();
            nextDocument.deserializeBinary(nextDocumentEntry);
            documentTable.put(nextDocument.getDocId(), nextDocument);
        }
        System.out.println("Document table read in memory..");
    }

    public void commandLine(){
        System.out.println("Starting the command line..");
        System.out.println("Input Format: [AND|OR] term1 ... termN");
        try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            System.out.print("> ");
            while ((line = in.readLine()) != null) {
                if(Arrays.asList(QUIT_CODES).contains(line)){
                    System.out.println("Shutting down...");
                    break;
                }
                String pid = "NaN";
                try {
                    pid = processQuery(line);
                } catch(IllegalQueryType e){
                    e.printStackTrace();
                    System.out.println("Input Format: [AND|OR] term1 ... termN");
                }
                if(!pid.equals("NaN"))
                    System.out.println("Resulting PID: " + pid);
                System.out.print("> ");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String processQuery(String query) throws IllegalQueryType {
        String[] tokens = tokenize(query);

        //TODO: remove duplicates?
        for (int i = 1; i < tokens.length; ++i){
            // skip first token specifying the type of the query
            if(Utils.invalidToken(tokens[i])) // also remove stopwords
                continue;
            tokens[i] = Utils.stemming(tokens[i]);
        }
        String result;
        if(tokens[0].equals("and"))
            result = conjunctiveQuery(Arrays.copyOfRange(tokens, 1, tokens.length));
        else if (tokens[0].equals("or"))
            result = disjunctiveQuery(Arrays.copyOfRange(tokens, 1, tokens.length));
        else
            throw new IllegalQueryType(tokens[0]);
        return result;
    }

    // tokens are normalized and stemmed
    //TODO: your program must perform seeks on disk in order to read only those inverted lists from disk that correspond to query words, and then compute the result

    public String conjunctiveQuery(String[] tokens){
        System.out.println("You have requested a conjunctive query with the following preprocessed tokens:");
        for(int i = 0; i < tokens.length; ++i)
            if(i < tokens.length-1)
                System.out.print(tokens[i] + ", ");
            else
                System.out.print(tokens[i]);
        System.out.println();

        PostingListQueryInterface[] postingLists = loadPostingLists(tokens);
        return "NaN";
    }

    public String disjunctiveQuery(String[] tokens){
        System.out.println("You have requested a disjunctive query with the following preprocessed tokens:");
        for(int i = 0; i < tokens.length; ++i)
            if(i < tokens.length-1)
                System.out.print(tokens[i] + ", ");
            else
                System.out.print(tokens[i]);
        System.out.println();

        // TODO: Continue defining interface and reading from the disk
        PostingListQueryInterface[] postingLists = loadPostingLists(tokens);
        return "NaN";
    }

    public PostingListQueryInterface[] loadPostingLists(String[] tokens){
        // TODO: Continue defining interface and reading from the disk
        PostingListQueryInterface[] pls = new PostingListQueryInterface[tokens.length];
        for(String token: tokens){
            LexiconTermIndexing lexiconTermIndexing = lexicon.get("token");
            PostingListQueryInterface pl = new PostingListQueryInterface(token, lexiconTermIndexing);
        }
        return pls;
    }
}
