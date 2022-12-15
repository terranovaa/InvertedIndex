package it.unipi;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import it.unipi.exceptions.IllegalQueryTypeException;
import it.unipi.models.CollectionStatistics;
import it.unipi.models.Document;
import it.unipi.models.LexiconTerm;
import it.unipi.utils.Constants;
import it.unipi.utils.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class QueryProcessor {
    private final String[] QUIT_CODES = new String[]{"Q", "q", "QUIT", "quit", "EXIT", "exit"};

    private final Cache<Integer, Document> documentTableCache = CacheBuilder.newBuilder()
            .maximumSize(1_000)
            .build();

    private final LoadingCache<String, LexiconTerm> lexiconCache = CacheBuilder.newBuilder()
            .maximumSize(1_000)
            .build(new CacheLoader<>() {
                public LexiconTerm load(String term) {
                    return lexiconDiskSearch(term);
                }
            });
    private final CollectionStatistics collectionStatistics;

    public QueryProcessor(){
        // TODO load collection statistics
        collectionStatistics = new CollectionStatistics();
        //loadDocumentTable();
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
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
                if(!pid.equals("NaN"))
                    System.out.println("Resulting PID: " + pid);
                System.out.print("> ");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String processQuery(String query) throws IllegalQueryTypeException, IOException, ExecutionException {

        String[] tokens = Utils.tokenize(query);

        //TODO: remove duplicates?
        for (int i = 1; i < tokens.length; ++i){
            // skip first token specifying the type of the query
            if(Utils.invalidToken(tokens[i])) // also remove stopwords
                continue;
            tokens[i] = Utils.stemming(tokens[i]);
        }
        PostingListInterface[] postingLists = loadPostingLists(Arrays.copyOfRange(tokens, 1, tokens.length));
        if(tokens[0].equals("and")) {
            System.out.println("You have requested a conjunctive query with the following preprocessed tokens:");
            printTokens(Arrays.copyOfRange(tokens, 1, tokens.length));
            //result = conjunctiveQuery(postingLists);
        } else if (tokens[0].equals("or")) {
            System.out.println("You have requested a disjunctive query with the following preprocessed tokens:");
            printTokens(Arrays.copyOfRange(tokens, 1, tokens.length));
            //result = disjunctiveQuery(postingLists);
        }
        else throw new IllegalQueryTypeException(tokens[0]);
        return "NaN";
    }


    private void printTokens(String[] tokens){
        for(int i = 0; i < tokens.length; ++i)
            if(i < tokens.length-1)
                System.out.print(tokens[i] + ", ");
            else
                System.out.print(tokens[i]);
        System.out.println();
    }

    public void conjunctiveQuery(PostingListInterface[] postingLists){

    }

    public void disjunctiveQuery(PostingListInterface[] postingLists){
    }

    public PostingListInterface[] loadPostingLists(String[] tokens) throws IOException, ExecutionException {
        ArrayList<PostingListInterface> pls = new ArrayList<>();
        for (String token : tokens) {
            LexiconTerm lexiconTerm = lexiconCache.get(token);
            PostingListInterface pl = new PostingListInterface(lexiconTerm);
            pls.add(pl);
        }
        return pls.toArray(new PostingListInterface[0]);
    }

    private static LexiconTerm lexiconDiskSearch(String term) {
        try {
            long fileSeekPointer;
            FileChannel lexiconChannel = FileChannel.open(Paths.get(Constants.LEXICON_FILE_PATH + Constants.DAT_FORMAT));
            int numberOfTerms = (int)lexiconChannel.size() / Constants.LEXICON_ENTRY_SIZE;

            LexiconTerm currentEntry = new LexiconTerm();
            ByteBuffer buffer = ByteBuffer.allocate(Constants.LEXICON_ENTRY_SIZE);
            int leftExtreme = 0;
            int rightExtreme = numberOfTerms;

            while(rightExtreme > leftExtreme){
                fileSeekPointer = (long) (leftExtreme + ((rightExtreme - leftExtreme) / 2)) * Constants.LEXICON_ENTRY_SIZE;
                lexiconChannel.position(fileSeekPointer);
                buffer.clear();
                lexiconChannel.read(buffer);
                //TODO to change
                currentEntry.deserializeBinary(buffer.array());
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
