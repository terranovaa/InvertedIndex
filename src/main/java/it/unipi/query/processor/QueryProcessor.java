package it.unipi.query.processor;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import it.unipi.exceptions.IllegalQueryTypeException;
import it.unipi.exceptions.NoResultsFoundException;
import it.unipi.exceptions.TermNotFoundException;
import it.unipi.exceptions.TerminatedListException;
import it.unipi.models.*;
import it.unipi.utils.Constants;
import it.unipi.utils.DiskDataStructuresSearch;
import it.unipi.utils.ScoringFunctions;
import it.unipi.utils.TextProcessingUtils;
import opennlp.tools.parser.Cons;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class QueryProcessor {
    private final String[] QUIT_CODES = new String[]{"Q", "q", "QUIT", "quit", "EXIT", "exit"};

    private final LoadingCache<Integer, Document> documentTableCache = CacheBuilder.newBuilder()
            .maximumSize(1_000_000)
            .build(new CacheLoader<>() {
                @Nonnull
                public Document load(@Nonnull Integer docId) {
                    return DiskDataStructuresSearch.docTableDiskSearch(docId, docTableBuffer);
                }
            });

    private final LoadingCache<String, LexiconTerm> lexiconCache = CacheBuilder.newBuilder()
            .maximumSize(10_000)
            .build(new CacheLoader<>() {
                @Nonnull
                public LexiconTerm load(@Nonnull String term) throws TermNotFoundException {
                    LexiconTerm lexiconTerm = DiskDataStructuresSearch.lexiconDiskSearch(term, numberOfTerms, lexiconBuffer);
                    // query term may not exist in our Lexicon
                    if (lexiconTerm == null) {
                        throw new TermNotFoundException();
                    }
                    return lexiconTerm;
                }
            });
    private final CollectionStatistics collectionStatistics;

    private final SortedSet<DocumentScore> docsPriorityQueue;

    public final MappedByteBuffer lexiconBuffer;
    public final MappedByteBuffer docTableBuffer;

    public final int numberOfTerms;

    public QueryProcessor() throws IOException{
        collectionStatistics = DiskDataStructuresSearch.readCollectionStatistics();
        docsPriorityQueue = new TreeSet<>();
        FileChannel lexiconChannel = FileChannel.open(Paths.get(Constants.LEXICON_FILE_PATH + Constants.DAT_FORMAT));
        lexiconBuffer = lexiconChannel.map(FileChannel.MapMode.READ_ONLY, 0, lexiconChannel.size()).load();
        FileChannel docTableChannel = FileChannel.open(Paths.get(Constants.DOCUMENT_TABLE_FILE_PATH + "_SPLIT1_" + Constants.DAT_FORMAT));
        docTableBuffer = docTableChannel.map(FileChannel.MapMode.READ_ONLY, 0, docTableChannel.size()).load();
        numberOfTerms = (int)lexiconChannel.size() / Constants.LEXICON_ENTRY_SIZE;

        //initialize the lexicon cache with terms with the highest document frequency
        FileChannel warmUpLexiconChannel = FileChannel.open(Paths.get(Constants.WARM_UP_LEXICON_FILE_PATH + Constants.DAT_FORMAT));
        ByteBuffer bb = ByteBuffer.allocate(Constants.LEXICON_ENTRY_SIZE * 5000);
        warmUpLexiconChannel.read(bb);
        byte[] warmUpLexicon = bb.array();
        byte[] entry = new byte[Constants.LEXICON_ENTRY_SIZE];
        for(int i=0; i < Constants.LEXICON_ENTRY_SIZE * 5000; i = i + Constants.LEXICON_ENTRY_SIZE){
            System.arraycopy(warmUpLexicon, i, entry, 0, Constants.LEXICON_ENTRY_SIZE);
            LexiconTermBinaryIndexing lexiconTerm = new LexiconTermBinaryIndexing();
            lexiconTerm.deserializeBinary(entry);
            lexiconCache.put(lexiconTerm.getTerm(), lexiconTerm);
        }

        //initialize the document table cache with longest documents
        FileChannel warmUpDocTableChannel = FileChannel.open(Paths.get(Constants.WARM_UP_DOC_TABLE + Constants.DAT_FORMAT));
        bb = ByteBuffer.allocate(Constants.DOCUMENT_ENTRY_SIZE_SPLIT1 * 5000);
        warmUpDocTableChannel.read(bb);
        byte[] warmUpDocTable = bb.array();
        entry = new byte[Constants.DOCUMENT_ENTRY_SIZE_SPLIT1];
        for(int i=0; i < Constants.DOCUMENT_ENTRY_SIZE_SPLIT1 * 5000; i = i + Constants.DOCUMENT_ENTRY_SIZE_SPLIT1){
            System.arraycopy(warmUpDocTable, i, entry, 0, Constants.DOCUMENT_ENTRY_SIZE_SPLIT1);
            Document d = new Document();
            d.deserializeBinarySplit1(entry);
            documentTableCache.put(d.getDocId(), d);
        }
    }

    public void commandLine(){
        System.out.println("Starting the command line..");
        System.out.println("Input Format: [AND|OR] term1 ... termN");
        try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            System.out.print("> ");
            while ((line = in.readLine()) != null) {
                long startQuery = System.currentTimeMillis();
                if(Arrays.asList(QUIT_CODES).contains(line)){
                    System.out.println("Shutting down...");
                    break;
                }
                boolean success = false;
                try {
                    success = processQuery(line);
                } catch(IllegalQueryTypeException e){
                    e.printStackTrace();
                    System.out.println("Input Format: [AND|OR] term1 ... termN");
                } catch (NoResultsFoundException e){
                    e.printStackTrace();
                } catch (ExecutionException | TerminatedListException e) {
                    throw new RuntimeException(e);
                }
                if(success)
                    returnResults();
                docsPriorityQueue.clear();
                long end = System.currentTimeMillis();
                System.out.println(((double)(end - startQuery)/1000) + " seconds");
                System.out.print("> ");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean processQuery(String query) throws IllegalQueryTypeException, IOException, ExecutionException, TerminatedListException, NoResultsFoundException {

        String[] tokens = TextProcessingUtils.tokenize(query);

        QueryType queryType;

        if (tokens[0].equals("and")) {
            queryType = QueryType.CONJUCTIVE;
            System.out.println("You have requested a conjunctive query with the following preprocessed tokens:");
        } else if (tokens[0].equals("or")) {
            queryType = QueryType.DISJUCTIVE;
            System.out.println("You have requested a disjunctive query with the following preprocessed tokens:");
        } else {
            throw new IllegalQueryTypeException();
        }

        printTokens(Arrays.copyOfRange(tokens, 1, tokens.length));

        int limit = tokens.length;
        if (tokens.length > Constants.MAX_QUERY_LENGTH) {
            System.out.println("Query too long, all the tokens after " + tokens[Constants.MAX_QUERY_LENGTH] + " are ignored");
            limit = Constants.MAX_QUERY_LENGTH + 1;
        }

        HashSet<String> tokenSet = new HashSet<>();
        for (int i = 1; i < limit; ++i){
            // skip first token specifying the type of the query
            String token = tokens[i];
            if(TextProcessingUtils.isAStopWord(token)) continue; // also remove stopwords
            token = TextProcessingUtils.truncateToken(token);
            token = TextProcessingUtils.stemToken(token);
            tokenSet.add(token);
        }

        // Using a TreeSet as the Posting Lists need to be sorted in increasing order of max score contribution
        ArrayList<PostingListInterface> postingLists = new ArrayList<>();

        for (String token : tokenSet) {
            LexiconTerm lexiconTerm;
            try {
                lexiconTerm = lexiconCache.get(token);
            } catch (ExecutionException e) {
                // term is not present in the lexicon
                System.err.println(e.getMessage());
                continue;
            }
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
            case CONJUCTIVE -> {
                return processConjunctiveQuery(postingLists, docUpperBounds);
            }
            case DISJUCTIVE -> {
                return processDisjunctiveQuery(postingLists, docUpperBounds);
            }
        }

            /*    for (Iterator<PostingListInterface> postingListIterator = postingLists.iterator(); postingListIterator.hasNext();) {
                    PostingListInterface postingList = postingListIterator.next();
                    if (this.queryType == QueryType.CONJUCTIVE && postingList.getDocId() != currentDocId) {
                        // if query is conjunctive and posting list does not contain the docid, move to the next docid
                        if (!postingList.next()) {
                            postingList.closeList();
                            postingListIterator.remove();
                        }
                        //move also all the subsequent posting lists that have the currentDocId
                        while(postingListIterator.hasNext()){
                            PostingListInterface nextPostingList = postingListIterator.next();
                            if (nextPostingList.getDocId() == currentDocId && !nextPostingList.next()) {
                                nextPostingList.closeList();
                                postingListIterator.remove();
                            }
                        }
                        score = -1;
                        break;
                    } else if(this.queryType == QueryType.DISJUCTIVE && postingList.getDocId() != currentDocId)
                        continue;
                    // compute partial score
                    score += ScoringFunctions.BM25(currentDoc.getLength(), postingList.getFreq(), lexiconCache.get(postingList.getTerm()), collectionStatistics);
                    //score += ScoringFunctions.TFIDF(postingList.getFreq(), lexiconCache.get(postingList.getTerm()), collectionStatistics);
                    // posting list end
                    if (!postingList.next()) {
                        postingList.closeList();
                        postingListIterator.remove();
                    }
                }
                if(score == -1) {
                    continue;
                }
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }



            DocumentScore docScore = new DocumentScore(currentDocId, score);

            if (docsPriorityQueue.size() < Constants.NUMBER_OF_OUTPUT_DOCUMENTS) {
                docsPriorityQueue.add(docScore);
            } else {
                if (score > docsPriorityQueue.last().score()) {
                    docsPriorityQueue.add(docScore);
                    docsPriorityQueue.remove(docsPriorityQueue.last());
                }
            }

             */
        return false;
    }

    private void returnResults() {
        for(DocumentScore ds : docsPriorityQueue){
            System.out.println(ds);
        }
    }

    private void printTokens(String[] tokens){
        for(int i = 0; i < tokens.length; ++i)
            if(i < tokens.length-1)
                System.out.print(tokens[i] + ", ");
            else
                System.out.print(tokens[i]);
        System.out.println();
    }

    private boolean processDisjunctiveQuery(List<PostingListInterface> postingLists, List<Double> docUpperBounds) {

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

            int next = -1;

            try {
                score = 0;
                Document currentDoc = documentTableCache.get(currentDocId);

                // essential lists
                for (int i = pivot; i < n; i++) {
                    if (finishedPostingLists.contains(i)) continue; // TODO check if this is the way to go
                    PostingListInterface postingList = postingLists.get(i);
                    if (postingList.getDocId() == currentDocId) {
                        score += ScoringFunctions.BM25(currentDoc.getLength(), postingList.getFreq(), lexiconCache.get(postingList.getTerm()), collectionStatistics);
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
                        score += ScoringFunctions.BM25(currentDoc.getLength(), postingList.getFreq(), lexiconCache.get(postingList.getTerm()), collectionStatistics);
                    }
                }
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }

            DocumentScore docScore = new DocumentScore(currentDocId, score);

            if (docsPriorityQueue.size() < Constants.NUMBER_OF_OUTPUT_DOCUMENTS || score > docsPriorityQueue.last().score()) {
                updatePriorityQueue(docScore);
                // list pivot update
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

    private boolean processConjunctiveQuery(List<PostingListInterface> postingLists, List<Double> docUpperBounds) {
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
            try {

                score = 0;
                Document currentDoc = documentTableCache.get(currentDocId);

                // essential lists
                for (int i = pivot; i < n; i++) {
                    PostingListInterface postingList = postingLists.get(i);
                    if (!postingList.nextGEQ(currentDocId)) {
                        atLeastAPostingListIsFinished = true;
                    }
                    if (postingList.getDocId() == currentDocId) {
                        score += ScoringFunctions.BM25(currentDoc.getLength(), postingList.getFreq(), lexiconCache.get(postingList.getTerm()), collectionStatistics);
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
                            score += ScoringFunctions.BM25(currentDoc.getLength(), postingList.getFreq(), lexiconCache.get(postingList.getTerm()), collectionStatistics);
                        } else {
                            score = -1;
                            break;
                        }
                    }
                }
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }

            if (score != -1) {
                DocumentScore docScore = new DocumentScore(currentDocId, score);

                if (docsPriorityQueue.size() < Constants.NUMBER_OF_OUTPUT_DOCUMENTS || score > docsPriorityQueue.last().score()) {
                    updatePriorityQueue(docScore);
                    // list pivot update
                    if(docsPriorityQueue.size() == Constants.NUMBER_OF_OUTPUT_DOCUMENTS){
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
        if (docsPriorityQueue.size() > Constants.NUMBER_OF_OUTPUT_DOCUMENTS) {
            docsPriorityQueue.remove(docsPriorityQueue.last());
        }
    }
}
