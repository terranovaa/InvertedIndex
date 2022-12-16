package it.unipi;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import it.unipi.exceptions.IllegalQueryTypeException;
import it.unipi.exceptions.TerminatedListException;
import it.unipi.models.CollectionStatistics;
import it.unipi.models.Document;
import it.unipi.models.DocumentScore;
import it.unipi.models.LexiconTerm;
import it.unipi.utils.Constants;
import it.unipi.utils.Utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.*;
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

    private PriorityQueue<DocumentScore> docsPriorityQueue = new PriorityQueue<>(10); // depends on k

    public QueryProcessor(){
        // loading collection statistics
        collectionStatistics = new CollectionStatistics();
        try (FileInputStream fisCollectionStatistics = new FileInputStream(Constants.COLLECTION_STATISTICS_FILE_PATH + Constants.DAT_FORMAT)){
            byte[] csBytes = fisCollectionStatistics.readNBytes(12);
            collectionStatistics.deserializeBinary(csBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
                } catch (ExecutionException | TerminatedListException e) {
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

    public String processQuery(String query) throws IllegalQueryTypeException, IOException, ExecutionException, TerminatedListException {

        String[] tokens = Utils.tokenize(query);

        String queryType = tokens[0];

        for (int i = 1; i < tokens.length; ++i){
            // skip first token specifying the type of the query
            if(Utils.isAStopWord(tokens[i])) continue; // also remove stopwords
            tokens[i] = Utils.truncateToken(tokens[i]);
            tokens[i] = Utils.stemToken(tokens[i]);
        }
        // TODO manage duplicate terms in query
        Set<PostingListInterface> postingLists = loadPostingLists(Arrays.copyOfRange(tokens, 1, tokens.length));

        postingLists.removeIf(postingList -> !postingList.next());

        int currentDocId;

        OptionalInt currentDocIdOpt;

        if ((currentDocIdOpt = postingLists.stream()
                .mapToInt(PostingListInterface::getDocId)
                .min()).isEmpty()) {
            return "NaN";
        } else {
            currentDocId = currentDocIdOpt.getAsInt();
        }

        while (!postingLists.isEmpty()) {

            int currentScore = 0;

            for (PostingListInterface postingList: postingLists) {
                if (postingList.getDocId() != currentDocId) continue;
                // compute partial score
                if (!postingList.next()) postingLists.remove(postingList);
            }

            // again k is not fixed I think
            if (docsPriorityQueue.size() == 10) {
                docsPriorityQueue.add(new DocumentScore(currentDocId, 0));
                // removes the head of the queue, need to check if it's the right way
                docsPriorityQueue.poll();
            }

            if ((currentDocIdOpt = postingLists.stream()
                    .mapToInt(PostingListInterface::getDocId)
                    .min()).isEmpty()) {
                return "NaN"; // if it's empty the list is empty I guess?
            } else {
                currentDocId = currentDocIdOpt.getAsInt();
            }
        }

        /*
        if(queryType.equals("and")) {
            System.out.println("You have requested a conjunctive query with the following preprocessed tokens:");
            printTokens(Arrays.copyOfRange(tokens, 1, tokens.length));
            //result = conjunctiveQuery(postingLists);
        } else if (queryType.equals("or")) {
            System.out.println("You have requested a disjunctive query with the following preprocessed tokens:");
            printTokens(Arrays.copyOfRange(tokens, 1, tokens.length));
            //result = disjunctiveQuery(postingLists);
        }
        else throw new IllegalQueryTypeException(tokens[0]);

         */
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

    public Set<PostingListInterface> loadPostingLists(String[] tokens) throws IOException, ExecutionException {
        HashSet<PostingListInterface> postingLists = new HashSet<>();
        for (String token : tokens) {
            LexiconTerm lexiconTerm = lexiconCache.get(token);
            PostingListInterface pl = new PostingListInterface(lexiconTerm);
            postingLists.add(pl);
        }
        return postingLists;
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
