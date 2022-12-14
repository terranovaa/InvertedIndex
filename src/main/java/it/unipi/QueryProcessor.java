package it.unipi;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import it.unipi.exceptions.IllegalQueryTypeException;
import it.unipi.exceptions.TerminatedListException;
import it.unipi.models.*;
import it.unipi.utils.Constants;
import it.unipi.utils.Utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeMap;
import static it.unipi.utils.Utils.*;

public class QueryProcessor {
    private final String[] QUIT_CODES = new String[]{"Q", "q", "QUIT", "quit", "EXIT", "exit"};
    private final HashMap<Integer, Document> documentTable = new HashMap<>();

    private final Cache<Integer, Document> documentTableCache = Caffeine.newBuilder()
            .maximumSize(1_000)
            .build();

    private final Cache<String, LexiconTerm> lexiconCache = Caffeine.newBuilder()
            .maximumSize(1_000)
            .build();

    private final CollectionStatistics collectionStatistics;

    public QueryProcessor(){
        // TODO load collection statistics
        collectionStatistics = new CollectionStatistics();
        //loadDocumentTable();
        //loadLexicon();
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
                } catch(IllegalQueryTypeException e){
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

    public String processQuery(String query) throws IllegalQueryTypeException {

        String[] tokens = Utils.tokenize(query);

        //TODO: remove duplicates?
        for (int i = 1; i < tokens.length; ++i){
            // skip first token specifying the type of the query
            if(Utils.invalidToken(tokens[i])) // also remove stopwords
                continue;
            tokens[i] = Utils.stemming(tokens[i]);
        }
        String result;
        PostingListQueryInterface[] postingLists = loadPostingLists(Arrays.copyOfRange(tokens, 1, tokens.length));
        if(tokens[0].equals("and")) {
            System.out.println("You have requested a conjunctive query with the following preprocessed tokens:");
            printTokens(Arrays.copyOfRange(tokens, 1, tokens.length));
            result = conjunctiveQuery(postingLists);
        } else if (tokens[0].equals("or")) {
            System.out.println("You have requested a disjunctive query with the following preprocessed tokens:");
            printTokens(Arrays.copyOfRange(tokens, 1, tokens.length));
            result = disjunctiveQuery(postingLists);
        }
        else throw new IllegalQueryTypeException(tokens[0]);
        return result;
    }


    private void printTokens(String[] tokens){
        for(int i = 0; i < tokens.length; ++i)
            if(i < tokens.length-1)
                System.out.print(tokens[i] + ", ");
            else
                System.out.print(tokens[i]);
        System.out.println();
    }

    public String conjunctiveQuery(PostingListQueryInterface[] postingLists){
        for(PostingListQueryInterface postingList : postingLists) {
            postingList.openList();

            // DEBUG
            while(true) {
                System.out.println("--Processing posting list regarding the term " + postingList.getTerm());
                try {
                    postingList.next();
                } catch (TerminatedListException e) {
                    System.out.println(e.getMessage());
                    break;
                }
                System.out.println(postingList.getDocId());
                System.out.println(postingList.getFreq());
            }
            postingList.closeList();
        }
        return "NaN";
    }

    public String disjunctiveQuery(PostingListQueryInterface[] postingLists){
        return "NaN";
    }

    public PostingListQueryInterface[] loadPostingLists(String[] tokens){
        ArrayList<PostingListQueryInterface> pls = new ArrayList<>();
        for(int i = 0; i < tokens.length; ++i){
            LexiconTerm lexiconTerm = lexiconDiskSearch(tokens[i]);
            if(lexiconTerm != null) {
                PostingListQueryInterface pl = new PostingListQueryInterface(lexiconTerm);
                pls.add(pl);
            }else System.out.println("Term " + tokens[i] + " not in the lexicon, skipping it...");
        }
        return pls.toArray(new PostingListQueryInterface[pls.size()]);
    }

    private static LexiconTerm lexiconDiskSearch(String term) {
        try {
            long fileSeekPointer;
            FileChannel lexiconChannel = FileChannel.open(Paths.get(Constants.LEXICON_FILE_PATH + Constants.DAT_FORMAT));
            int numberOfTerms = (int)lexiconChannel.size() / Constants.LEXICON_ENTRY_SIZE;

            LexiconTermBinaryIndexing currentEntry = new LexiconTermBinaryIndexing();
            ByteBuffer buffer = ByteBuffer.allocate(Constants.LEXICON_ENTRY_SIZE);
            int leftExtreme = 0;
            int rightExtreme = numberOfTerms;

            while(rightExtreme > leftExtreme){
                fileSeekPointer = (leftExtreme + ((rightExtreme - leftExtreme) / 2)) * Constants.LEXICON_ENTRY_SIZE;
                lexiconChannel.position(fileSeekPointer);
                buffer.clear();
                lexiconChannel.read(buffer);
                //TODO to change
                currentEntry.deserialize(buffer.array());
                if(currentEntry.getTerm().compareTo(term) > 0){
                    //we go left on the array
                    rightExtreme = rightExtreme - (int)Math.ceil(((double)(rightExtreme - leftExtreme) / 2));
                } else if (currentEntry.getTerm().compareTo(term) < 0) {
                    //we go right on the array
                    leftExtreme = leftExtreme + (int)Math.ceil(((double)(rightExtreme - leftExtreme) / 2));
                } else {
                    return currentEntry;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
