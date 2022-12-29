package it.unipi.query.processor;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import it.unipi.exceptions.IllegalQueryTypeException;
import it.unipi.exceptions.NoResultsFoundException;
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
    // priority queue used to store query results (ordered by decreasing score)
    private final SortedSet<DocumentScore> docsPriorityQueue;
    // hash set used to store query tokens (sets do not allow duplicates)
    private HashSet<String> tokenSet = new HashSet<>();
    private QueryType queryType;
    // memory mapping of the lexicon file
    public final MappedByteBuffer lexiconBuffer;
    // memory mapping of the doc table file
    public final MappedByteBuffer docTableBuffer;
    // number of terms in the lexicon (used for binary search)
    public final int numberOfTerms;
    // LRU cache used to store query results
    private final Cache<HashSet<String>, SortedSet<DocumentScore>> queryCache = CacheBuilder.newBuilder().maximumSize(500).initialCapacity(500).build();

    // number of documents to be returned by the query processor
    private int k;

    public QueryProcessor() throws IOException{

        //used to compute scoring functions
        collectionStatistics = DiskDataStructuresSearch.readCollectionStatistics();
        docsPriorityQueue = new TreeSet<>();

        FileChannel lexiconChannel = FileChannel.open(Paths.get(Constants.LEXICON_FILE_PATH + Constants.DAT_FORMAT));
        lexiconBuffer = lexiconChannel.map(FileChannel.MapMode.READ_ONLY, 0, lexiconChannel.size()).load();
        FileChannel docTableChannel = FileChannel.open(Paths.get(Constants.DOCUMENT_TABLE_FILE_PATH + Constants.DAT_FORMAT));
        docTableBuffer = docTableChannel.map(FileChannel.MapMode.READ_ONLY, 0, docTableChannel.size()).load();

        //used for binary search over lexicon data structure
        numberOfTerms = (int)lexiconChannel.size() / Constants.LEXICON_ENTRY_SIZE;

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
                runQuery(line, Constants.NUMBER_OF_OUTPUT_DOCUMENTS);
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
        } catch (ExecutionException | IOException e) {
            throw new RuntimeException(e);
        }
        if(success) {
            for(DocumentScore ds : docsPriorityQueue){
                System.out.println(ds.docNo() + " " + ds.score());
            }
        }

        long end = System.currentTimeMillis();
        System.out.println(((double)(end - startQuery)/1000) + " seconds");

        SortedSet<DocumentScore> results = new TreeSet<>();

        // if the query was successful, save query results in cache
        if (success) {

            results.addAll(docsPriorityQueue);

            // updating the query cache
            if (queryCache.getIfPresent(tokenSet) == null) {
                if(queryType == QueryType.CONJUNCTIVE){
                    tokenSet.add("and");
                }
                else{
                    tokenSet.add("or");
                }
                // save query type, query tokens and output documents
                queryCache.put(tokenSet, results);
            }

            docsPriorityQueue.clear();
        }

        return results;
    }

    public boolean processQuery(String query) throws IllegalQueryTypeException, IOException, ExecutionException, NoResultsFoundException {

        // same preprocessing as the documents
        String[] tokens = TextProcessingUtils.tokenize(query);

        // checking the query type
        if (tokens[0].equals("and")) {
            queryType = QueryType.CONJUNCTIVE;
            System.out.println("You have requested a conjunctive query with the following preprocessed tokens:");
        } else if (tokens[0].equals("or")) {
            queryType = QueryType.DISJUNCTIVE;
            System.out.println("You have requested a disjunctive query with the following preprocessed tokens:");
        } else {
            throw new IllegalQueryTypeException();
        }

        // printing the query tokens
        for(int i = 1; i < tokens.length; ++i)
            if(i < tokens.length-1)
                System.out.print(tokens[i] + ", ");
            else
                System.out.print(tokens[i] + '\n');

        int limit = tokens.length;
        if (tokens.length > Constants.MAX_QUERY_LENGTH) {
            System.out.println("Query too long, all the tokens after " + tokens[Constants.MAX_QUERY_LENGTH] + " are ignored");
            limit = Constants.MAX_QUERY_LENGTH + 1;
        }

        tokenSet = new HashSet<>();
        for (int i = 1; i < limit; ++i){
            // skipping first token specifying the type of the query
            String token = tokens[i];
            if(TextProcessingUtils.isAStopWord(token)) continue; // removing stopwords
            token = TextProcessingUtils.truncateToken(token);
            token = TextProcessingUtils.stemToken(token);
            tokenSet.add(token);
        }

        SortedSet<DocumentScore> documentScores;
        // checking if the query has already been processed
        tokenSet.add(tokens[0]); // adding the query type
        if ((documentScores = queryCache.getIfPresent(tokenSet)) != null) {
            docsPriorityQueue.addAll(documentScores);
            return true;
        }
        tokenSet.remove(tokens[0]); // removing the query type

        ArrayList<PostingListInterface> postingLists = new ArrayList<>();

        HashMap<String, LexiconTerm> lexiconTerms = new HashMap<>();

        // retrieving the terms' info from the lexicon
        for (String token : tokenSet) {
            LexiconTerm lexiconTerm;
            lexiconTerm = DiskDataStructuresSearch.lexiconDiskSearch(token, numberOfTerms, lexiconBuffer);

            if (lexiconTerm == null) {
                //if one of the query terms isn't present in the lexicon and the query type is conjunctive, no documents are returned
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

        // sorting the posting lists in increasing order of max score contribution
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

    private boolean processDisjunctiveQuery(List<PostingListInterface> postingLists, List<Double> docUpperBounds, HashMap<String, LexiconTerm> lexiconTerms) {

        // for MaxScore
        double threshold = 0;
        int pivot = 0;
        int currentDocId;
        double score;

        int n = postingLists.size();

        // getting the min doc id from all the posting lists
        if (!postingLists.isEmpty()) {
            currentDocId = postingLists.stream()
                    .mapToInt(PostingListInterface::getDocId)
                    .min().getAsInt();
        } else {
            return false;
        }

        // used to keep track of terminated posting lists
        HashSet<Integer> finishedPostingLists = new HashSet<>();

        while (currentDocId != -1 && pivot < n) {

            // all the posting lists are finished
            if (finishedPostingLists.size() == postingLists.size()) break;

            int next = -1;
            // BM25 score
            score = 0;
            // loading the doc from disk
            Document currentDoc = DiskDataStructuresSearch.docTableDiskSearch(currentDocId, docTableBuffer);

            // essential lists
            for (int i = pivot; i < n; i++) {
                if (finishedPostingLists.contains(i)) continue; // if the list is finished I move on
                PostingListInterface postingList = postingLists.get(i);
                if (postingList.getDocId() == currentDocId) {
                    score += ScoringFunctions.BM25(currentDoc.getLength(), postingList.getFreq(), lexiconTerms.get(postingList.getTerm()), collectionStatistics);
                    // moving the pointer to the next posting (if present)
                    if (!postingList.next()) finishedPostingLists.add(i);
                }
                // computing the next document to score
                if ((next == -1 || postingList.getDocId() < next) && !finishedPostingLists.contains(i)) {
                    next = postingList.getDocId();
                }
            }

            // non essential lists
            for (int i = pivot - 1; i >= 0; i--) {
                if (finishedPostingLists.contains(i)) continue; // if the list is finished I move on
                if (score + docUpperBounds.get(i) <= threshold) break;
                PostingListInterface postingList = postingLists.get(i);
                // moving the pointer to a docId GEQ than currentDocId (if present)
                if (!postingList.nextGEQ(currentDocId)) {
                    finishedPostingLists.add(i);
                }
                if (postingList.getDocId() == currentDocId) {
                    score += ScoringFunctions.BM25(currentDoc.getLength(), postingList.getFreq(), lexiconTerms.get(postingList.getTerm()), collectionStatistics);
                }
            }

            DocumentScore docScore = new DocumentScore(currentDoc.getDocNo(), score);

            // updating the priority queue
            if (docsPriorityQueue.size() < k || score > docsPriorityQueue.last().score()) {
                updatePriorityQueue(docScore);
                // list pivot update
                threshold = updateThreshold();
                pivot = updatePivot(pivot, n, docUpperBounds, threshold);
            }

            // current doc_id update
            currentDocId = next;
        }
        return true;
    }

    private boolean processConjunctiveQuery(List<PostingListInterface> postingLists, List<Double> docUpperBounds, HashMap<String, LexiconTerm> lexiconTerms) {

        // for MaxScore
        double threshold = 0;
        int pivot = 0;
        int currentDocId;
        double score;

        int n = postingLists.size();

        // getting the max doc id from all the posting lists
        currentDocId = postingLists.get(0).getDocId();
        for (PostingListInterface postingList: postingLists) {
            if (postingList.getDocId() > currentDocId) {
                currentDocId = postingList.getDocId();
            }
        }

        // conjunctive query, if just one posting list is finished we can exit
        boolean atLeastAPostingListIsFinished = false;

        while (pivot < n && !atLeastAPostingListIsFinished) {

            score = 0;
            Document currentDoc = DiskDataStructuresSearch.docTableDiskSearch(currentDocId, docTableBuffer);

            // essential lists
            for (int i = pivot; i < n; i++) {
                PostingListInterface postingList = postingLists.get(i);
                // moving the pointer to a docId GEQ than currentDocId (if present)
                if (!postingList.nextGEQ(currentDocId)) {
                    atLeastAPostingListIsFinished = true;
                }
                if (postingList.getDocId() == currentDocId) {
                    score += ScoringFunctions.BM25(currentDoc.getLength(), postingList.getFreq(), lexiconTerms.get(postingList.getTerm()), collectionStatistics);
                    // moving the pointer to the next posting (if present)
                    if (!postingList.next()) atLeastAPostingListIsFinished = true;
                } else { // not all the posting lists have currentDocId, so we do not score it
                    score = -1;
                    break;
                }
            }

            // we continue only if all the essential lists had currentDocId
            if (score != -1) {
                // non essential lists
                for (int i = pivot - 1; i >= 0; i--) {
                    if (score + docUpperBounds.get(i) <= threshold) break;
                    PostingListInterface postingList = postingLists.get(i);
                    // moving the pointer to a docId GEQ than currentDocId (if present)
                    if (!postingList.nextGEQ(currentDocId)) {
                        atLeastAPostingListIsFinished = true;
                    }
                    if (postingList.getDocId() == currentDocId) {
                        score += ScoringFunctions.BM25(currentDoc.getLength(), postingList.getFreq(), lexiconTerms.get(postingList.getTerm()), collectionStatistics);
                    } else { // not all the posting lists have currentDocId, so we do not score it
                        score = -1;
                        break;
                    }
                }
            }

            // if the document has been scored we update the priority queue
            if (score != -1) {
                DocumentScore docScore = new DocumentScore(currentDoc.getDocNo(), score);

                if (docsPriorityQueue.size() < k || score > docsPriorityQueue.last().score()) {
                    updatePriorityQueue(docScore);
                    // list pivot update
                    threshold = updateThreshold();
                    pivot = updatePivot(pivot, n, docUpperBounds, threshold);
                }
            }

            // current doc_id update (max of all the posting lists' current docId)
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

    private double updateThreshold() {
        if(docsPriorityQueue.size() == k){
            return docsPriorityQueue.last().score();
        } else return 0.0;
    }

    private int updatePivot(int pivot, int n, List<Double> docUpperBounds, double threshold) {
        while (pivot < n && docUpperBounds.get(pivot) <= threshold) {
            pivot++;
        }
        return pivot;
    }
}
