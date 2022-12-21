package it.unipi.query.processor;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import it.unipi.exceptions.IllegalQueryTypeException;
import it.unipi.exceptions.NoResultsFoundException;
import it.unipi.exceptions.TerminatedListException;
import it.unipi.models.*;
import it.unipi.utils.Constants;
import it.unipi.utils.DiskDataStructuresSearch;
import it.unipi.utils.ScoringFunctions;
import it.unipi.utils.TextProcessingUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class QueryProcessor {
    private final String[] QUIT_CODES = new String[]{"Q", "q", "QUIT", "quit", "EXIT", "exit"};
    private final CollectionStatistics collectionStatistics;

    private final SortedSet<DocumentScore> docsPriorityQueue;
    private HashSet<String> tokenSet = new HashSet<>();
    private QueryType queryType;

    public final MappedByteBuffer lexiconBuffer;
    public final MappedByteBuffer docTableBuffer;

    public final int numberOfTerms;

    private final Cache<HashSet<String>, SortedSet<DocumentScore>> queryCache = CacheBuilder.newBuilder().maximumSize(100).initialCapacity(100).build();

    private int k;

    public QueryProcessor() throws IOException{
        collectionStatistics = DiskDataStructuresSearch.readCollectionStatistics();
        docsPriorityQueue = new TreeSet<>();
        FileChannel lexiconChannel = FileChannel.open(Paths.get(Constants.LEXICON_FILE_PATH + Constants.DAT_FORMAT));
        lexiconBuffer = lexiconChannel.map(FileChannel.MapMode.READ_ONLY, 0, lexiconChannel.size()).load();
        FileChannel docTableChannel = FileChannel.open(Paths.get(Constants.DOCUMENT_TABLE_FILE_PATH + Constants.DAT_FORMAT));
        docTableBuffer = docTableChannel.map(FileChannel.MapMode.READ_ONLY, 0, docTableChannel.size()).load();
        numberOfTerms = (int)lexiconChannel.size() / Constants.LEXICON_ENTRY_SIZE;
        k = Constants.NUMBER_OF_OUTPUT_DOCUMENTS;
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
                runQuery(line, k);
                System.out.print("> ");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SortedSet<DocumentScore> runQuery(String query, int k) {

        this.k = k;

        long startQuery = System.currentTimeMillis();

        boolean success = false;
        try {
            success = processQuery(query);
        } catch(IllegalQueryTypeException e){
            e.printStackTrace();
            System.out.println("Input Format: [AND|OR] term1 ... termN");
        } catch (NoResultsFoundException e){
            e.printStackTrace();
        } catch (ExecutionException | TerminatedListException | IOException e) {
            throw new RuntimeException(e);
        }
        if(success) {
            for(DocumentScore ds : docsPriorityQueue){
                System.out.println(ds.docNo());
            }
        }

        long end = System.currentTimeMillis();
        System.out.println(((double)(end - startQuery)/1000) + " seconds");

        SortedSet<DocumentScore> results = new TreeSet<>();

        if (success) {

            results.addAll(docsPriorityQueue);

            if (queryCache.getIfPresent(tokenSet) == null) {
                if(queryType == QueryType.CONJUNCTIVE){
                    tokenSet.add("and");
                }
                else{
                    tokenSet.add("or");
                }
                queryCache.put(tokenSet, results);
            }

            docsPriorityQueue.clear();
        }

        return results;
    }

    public boolean processQuery(String query) throws IllegalQueryTypeException, IOException, ExecutionException, TerminatedListException, NoResultsFoundException {

        String[] tokens = TextProcessingUtils.tokenize(query);

        if (tokens[0].equals("and")) {
            queryType = QueryType.CONJUNCTIVE;
            System.out.println("You have requested a conjunctive query with the following preprocessed tokens:");
        } else if (tokens[0].equals("or")) {
            queryType = QueryType.DISJUNCTIVE;
            System.out.println("You have requested a disjunctive query with the following preprocessed tokens:");
        } else {
            throw new IllegalQueryTypeException();
        }

        printTokens(tokens);

        int limit = tokens.length;
        if (tokens.length > Constants.MAX_QUERY_LENGTH) {
            System.out.println("Query too long, all the tokens after " + tokens[Constants.MAX_QUERY_LENGTH] + " are ignored");
            limit = Constants.MAX_QUERY_LENGTH + 1;
        }

        tokenSet = new HashSet<>();
        for (int i = 1; i < limit; ++i){
            // skip first token specifying the type of the query
            String token = tokens[i];
            if(TextProcessingUtils.isAStopWord(token)) continue; // also remove stopwords
            token = TextProcessingUtils.truncateToken(token);
            token = TextProcessingUtils.stemToken(token);
            tokenSet.add(token);
        }

        SortedSet<DocumentScore> documentScores;
        tokenSet.add(tokens[0]);
        if ((documentScores = queryCache.getIfPresent(tokenSet)) != null) {
            docsPriorityQueue.addAll(documentScores);
            return true;
        }
        tokenSet.remove(tokens[0]);

        // Using a TreeSet as the Posting Lists need to be sorted in increasing order of max score contribution
        ArrayList<PostingListInterface> postingLists = new ArrayList<>();

        HashMap<String, LexiconTerm> lexiconTerms = new HashMap<>();

        for (String token : tokenSet) {
            LexiconTerm lexiconTerm;
            lexiconTerm = DiskDataStructuresSearch.lexiconDiskSearch(token, numberOfTerms, lexiconBuffer);
            if (lexiconTerm == null) {
                if(queryType == QueryType.CONJUNCTIVE){
                    return false;
                }
                else{
                    continue;
                }
            }
            lexiconTerms.put(token, lexiconTerm);
            PostingListInterface pl = new PostingListInterface(lexiconTerm);
            postingLists.add(pl);
        }

        Collections.sort(postingLists);

        // checking for empty posting lists
        for (Iterator<PostingListInterface> postingListIterator = postingLists.iterator(); postingListIterator.hasNext();) {
            PostingListInterface postingList = postingListIterator.next();
            if (!postingList.next()) {
                postingList.closeList();
                postingListIterator.remove();
            }
        }

        if (postingLists.isEmpty()) return false;

        int n = postingLists.size();

        // initializing document upper bounds for MaxScore
        ArrayList<Double> docUpperBounds = new ArrayList<>(postingLists.size());
        docUpperBounds.add(0, postingLists.get(0).getTermUpperBound());
        for (int i = 1; i < n; i++) {
            docUpperBounds.add(i, docUpperBounds.get(i - 1) + postingLists.get(i).getTermUpperBound());
        }

        switch (queryType) {
            case CONJUNCTIVE -> {
                return processConjunctiveQuery(postingLists, docUpperBounds, lexiconTerms);
            }
            case DISJUNCTIVE -> {
                return processDisjunctiveQuery(postingLists, docUpperBounds, lexiconTerms);
            }
        }

        return false;
    }

    private void printTokens(String[] tokens){
        for(int i = 1; i < tokens.length; ++i)
            if(i < tokens.length-1)
                System.out.print(tokens[i] + ", ");
            else
                System.out.print(tokens[i]);
        System.out.println();
    }

    private boolean processDisjunctiveQuery(List<PostingListInterface> postingLists, List<Double> docUpperBounds, HashMap<String, LexiconTerm> lexiconTerms) {

        double threshold = 0;
        int pivot = 0;
        int currentDocId;
        double score;

        int n = postingLists.size();

        if (!postingLists.isEmpty()) {
            currentDocId = postingLists.stream()
                    .mapToInt(PostingListInterface::getDocId)
                    .min().getAsInt();
        } else {
            return false;
        }

        HashSet<Integer> finishedPostingLists = new HashSet<>();

        while (currentDocId != -1 && pivot < n) {

            if (finishedPostingLists.size() == postingLists.size()) break;

            int next = -1;

            score = 0;
            Document currentDoc = DiskDataStructuresSearch.docTableDiskSearch(currentDocId, docTableBuffer);

            // essential lists
            for (int i = pivot; i < n; i++) {
                if (finishedPostingLists.contains(i)) continue; // TODO check if this is the way to go
                PostingListInterface postingList = postingLists.get(i);
                if (postingList.getDocId() == currentDocId) {
                    score += ScoringFunctions.BM25(currentDoc.getLength(), postingList.getFreq(), lexiconTerms.get(postingList.getTerm()), collectionStatistics);
                    //move the pointer to the next posting (if present)
                    if (!postingList.next()) finishedPostingLists.add(i);
                }
                //compute next document to score
                if (next == -1 || postingList.getDocId() < next) {
                    next = postingList.getDocId();
                }
            }

            // non essential lists
            for (int i = pivot - 1; i >= 0; i--) {
                if (finishedPostingLists.contains(i)) continue; // TODO check if this is the way to go
                if (score + docUpperBounds.get(i) <= threshold) break;
                PostingListInterface postingList = postingLists.get(i);
                if (!postingList.nextGEQ(currentDocId)) {
                    finishedPostingLists.add(i);
                }
                if (postingList.getDocId() == currentDocId) {
                    score += ScoringFunctions.BM25(currentDoc.getLength(), postingList.getFreq(), lexiconTerms.get(postingList.getTerm()), collectionStatistics);
                }
            }

            DocumentScore docScore = new DocumentScore(currentDoc.getDocNo(), score);

            if (docsPriorityQueue.size() < Constants.NUMBER_OF_OUTPUT_DOCUMENTS || score > docsPriorityQueue.last().score()) {
                updatePriorityQueue(docScore);
                // list pivot update
                // TODO why? shouldn't we always update the threshold?
                if(docsPriorityQueue.size() == Constants.NUMBER_OF_OUTPUT_DOCUMENTS){
                    threshold = docsPriorityQueue.last().score();
                }
                while (pivot < n && docUpperBounds.get(pivot) <= threshold) {
                    pivot++;
                }
            }

            // current doc_id update
            currentDocId = next;
        }
        return true;
    }

    private boolean processConjunctiveQuery(List<PostingListInterface> postingLists, List<Double> docUpperBounds, HashMap<String, LexiconTerm> lexiconTerms) {
        double threshold = 0;
        int pivot = 0;
        int currentDocId;
        double score;

        int n = postingLists.size();

        currentDocId = postingLists.get(0).getDocId();
        for (PostingListInterface postingList: postingLists) {
            if (postingList.getDocId() > currentDocId) {
                currentDocId = postingList.getDocId();
            }
        }

        boolean atLeastAPostingListIsFinished = false;

        while (pivot < n && !atLeastAPostingListIsFinished) {

            score = 0;
            Document currentDoc = DiskDataStructuresSearch.docTableDiskSearch(currentDocId, docTableBuffer);

            // essential lists
            for (int i = pivot; i < n; i++) {
                PostingListInterface postingList = postingLists.get(i);
                if (!postingList.nextGEQ(currentDocId)) {
                    atLeastAPostingListIsFinished = true;
                }
                if (postingList.getDocId() == currentDocId) {
                    score += ScoringFunctions.BM25(currentDoc.getLength(), postingList.getFreq(), lexiconTerms.get(postingList.getTerm()), collectionStatistics);
                    if (!postingList.next()) atLeastAPostingListIsFinished = true;
                } else {
                    score = -1;
                    break;
                }
            }

            if (score != -1) {
                // non essential lists
                for (int i = pivot - 1; i >= 0; i--) {
                    if (score + docUpperBounds.get(i) <= threshold) break;
                    PostingListInterface postingList = postingLists.get(i);
                    if (!postingList.nextGEQ(currentDocId)) {
                        atLeastAPostingListIsFinished = true;
                    }
                    if (postingList.getDocId() == currentDocId) {
                        score += ScoringFunctions.BM25(currentDoc.getLength(), postingList.getFreq(), lexiconTerms.get(postingList.getTerm()), collectionStatistics);
                    } else {
                        score = -1;
                        break;
                    }
                }
            }

            if (score != -1) {
                DocumentScore docScore = new DocumentScore(currentDoc.getDocNo(), score);

                if (docsPriorityQueue.size() < k || score > docsPriorityQueue.last().score()) {
                    updatePriorityQueue(docScore);
                    // list pivot update
                    if(docsPriorityQueue.size() == k){
                        threshold = docsPriorityQueue.last().score();
                    }
                    while (pivot < n && docUpperBounds.get(pivot) <= threshold) {
                        pivot++;
                    }
                }
            }

            // current doc_id update
            //TODO is it right?
            currentDocId = -1;
            for (PostingListInterface postingList: postingLists) {
                if (postingList.getDocId() > currentDocId) {
                    currentDocId = postingList.getDocId();
                }
            }
        }
        return true;
    }

    private void updatePriorityQueue(DocumentScore docScore) {
        docsPriorityQueue.add(docScore);
        if (docsPriorityQueue.size() > k) {
            docsPriorityQueue.remove(docsPriorityQueue.last());
        }
    }
}
