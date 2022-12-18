package it.unipi.queryProcessor;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import it.unipi.exceptions.IllegalQueryTypeException;
import it.unipi.exceptions.TermNotFoundException;
import it.unipi.exceptions.TerminatedListException;
import it.unipi.models.*;
import it.unipi.utils.Constants;
import it.unipi.utils.ScoringFunctions;
import it.unipi.utils.TextProcessingUtils;

import javax.annotation.Nonnull;
import java.io.*;
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
                    return docTableDiskSearch(docId);
                }
            });

    private final LoadingCache<String, LexiconTerm> lexiconCache = CacheBuilder.newBuilder()
            .maximumSize(10_000)
            .build(new CacheLoader<>() {
                @Nonnull
                public LexiconTerm load(@Nonnull String term) throws TermNotFoundException {
                    LexiconTerm lexiconTerm = lexiconDiskSearch(term);
                    // query term may not exist in our Lexicon
                    if (lexiconTerm == null) {
                        throw new TermNotFoundException();
                    }
                    return lexiconTerm;
                }
            });
    private CollectionStatistics collectionStatistics;

    private final SortedSet<DocumentScore> docsPriorityQueue;

    private final MappedByteBuffer lexiconBuffer;
    private final MappedByteBuffer docTableBuffer;
    private long startQuery;

    private final int numberOfTerms;

    public QueryProcessor() throws IOException{
        readCollectionStatistics();
        docsPriorityQueue = new TreeSet<>();
        FileChannel lexiconChannel = FileChannel.open(Paths.get(Constants.LEXICON_FILE_PATH + Constants.DAT_FORMAT));
        lexiconBuffer = lexiconChannel.map(FileChannel.MapMode.READ_ONLY, 0, lexiconChannel.size()).load();
        FileChannel docTableChannel = FileChannel.open(Paths.get(Constants.DOCUMENT_TABLE_FILE_PATH + Constants.DAT_FORMAT));
        docTableBuffer = docTableChannel.map(FileChannel.MapMode.READ_ONLY, 0, docTableChannel.size()).load();
        numberOfTerms = (int)lexiconChannel.size() / Constants.LEXICON_ENTRY_SIZE;
    }

    private void readCollectionStatistics(){
        collectionStatistics = new CollectionStatistics();
        try (FileInputStream fisCollectionStatistics = new FileInputStream(Constants.COLLECTION_STATISTICS_FILE_PATH + Constants.DAT_FORMAT)){
            byte[] csBytes = fisCollectionStatistics.readNBytes(16);
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
                startQuery = System.currentTimeMillis();
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

    public boolean processQuery(String query) throws IllegalQueryTypeException, IOException, ExecutionException, TerminatedListException {

        String[] tokens = TextProcessingUtils.tokenize(query);

        String queryType = tokens[0];
        if(queryType.equals("and")) {
            System.out.println("You have requested a conjunctive query with the following preprocessed tokens:");
            printTokens(Arrays.copyOfRange(tokens, 1, tokens.length));
        } else if (queryType.equals("or")) {
            System.out.println("You have requested a disjunctive query with the following preprocessed tokens:");
            printTokens(Arrays.copyOfRange(tokens, 1, tokens.length));
        }
        else throw new IllegalQueryTypeException(tokens[0]);

        int limit = tokens.length;
        if (tokens.length > Constants.MAX_QUERY_LENGTH) {
            System.out.println("Query too long, all the tokens after " + tokens[Constants.MAX_QUERY_LENGTH] + " are ignored");
            limit = Constants.MAX_QUERY_LENGTH + 1;
        }

        HashSet<String> tokensSet = new HashSet<>();
        for (int i = 1; i < limit; ++i){
            // skip first token specifying the type of the query
            if(TextProcessingUtils.isAStopWord(tokens[i])) continue; // also remove stopwords
            tokens[i] = TextProcessingUtils.truncateToken(tokens[i]);
            tokens[i] = TextProcessingUtils.stemToken(tokens[i]);
            tokensSet.add(tokens[i]);
        }

        // TODO Maybe change set to list for postingLists?
        Set<PostingListInterface> postingLists = loadPostingLists(tokensSet);

        for (Iterator<PostingListInterface> postingListIterator = postingLists.iterator(); postingListIterator.hasNext();) {
            PostingListInterface postingList = postingListIterator.next();
            if (!postingList.next()) {
                postingList.closeList();
                postingListIterator.remove();
            }
        }
        int currentDocId;

        OptionalInt currentDocIdOpt;

        double score;

        while (!postingLists.isEmpty()) {
            if ((currentDocIdOpt = postingLists.stream()
                    .mapToInt(PostingListInterface::getDocId)
                    .min()).isEmpty()) {
                return false;
            } else {
                currentDocId = currentDocIdOpt.getAsInt();
            }
            try {
                score = Scorer(currentDocId, postingLists, queryType.equals("and"));
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
        }

        return true;
    }

    private double Scorer(int currentDocId, Set<PostingListInterface> postingLists, boolean conjunctive) throws ExecutionException {
        double score = 0;
        Document currentDoc = documentTableCache.get(currentDocId);
        for (Iterator<PostingListInterface> postingListIterator = postingLists.iterator(); postingListIterator.hasNext();) {
            PostingListInterface postingList = postingListIterator.next();
            if (conjunctive && postingList.getDocId() != currentDocId) {
                // if query is conjunctive and posting list does not contain the docid, move to the next docid
                if (!postingList.next()) {
                    postingList.closeList();
                    postingListIterator.remove();
                }
                return -1;
            } else if(!conjunctive && postingList.getDocId() != currentDocId)
                continue;
            // compute partial score
            score += ScoringFunctions.BM25(currentDoc, postingList, lexiconCache.get(postingList.getTerm()), collectionStatistics);
            //score += ScoringFunctions.TFIDF(postingList, lexiconCache.get(postingList.getTerm()), collectionStatistics);
            // posting list end
            if (!postingList.next()) {
                postingList.closeList();
                postingListIterator.remove();
            }
        }
        return score;
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

    public Set<PostingListInterface> loadPostingLists(Set<String> tokens) throws IOException {
        HashSet<PostingListInterface> postingLists = new HashSet<>();
        for (String token : tokens) {
            if(TextProcessingUtils.isAStopWord(token)){
                continue;
            }
            LexiconTerm lexiconTerm;
            try {
                lexiconTerm = lexiconCache.get(token);
            } catch (ExecutionException e) {
                e.printStackTrace();
                continue;
            }
            PostingListInterface pl = new PostingListInterface(lexiconTerm);
            postingLists.add(pl);
        }
        return postingLists;
    }

    public LexiconTerm lexiconDiskSearch(String term) {
        int pointer;

        LexiconTerm currentEntry = new LexiconTerm();
        int leftExtreme = 0;
        int rightExtreme = numberOfTerms;

        while(rightExtreme > leftExtreme){
            pointer = (leftExtreme + ((rightExtreme - leftExtreme) / 2)) * Constants.LEXICON_ENTRY_SIZE;
            lexiconBuffer.position(pointer);
            byte[] buffer = new byte[Constants.LEXICON_ENTRY_SIZE];
            lexiconBuffer.get(buffer, 0, Constants.LEXICON_ENTRY_SIZE);
            String currentTerm = currentEntry.deserializeTerm(buffer);
            if(currentTerm.compareTo(term) > 0){
                //we go left on the array
                rightExtreme = rightExtreme - (int)Math.ceil(((double)(rightExtreme - leftExtreme) / 2));
            } else if (currentTerm.compareTo(term) < 0) {
                //we go right on the array
                leftExtreme = leftExtreme + (int)Math.ceil(((double)(rightExtreme - leftExtreme) / 2));
            } else {
                currentEntry.deserializeBinary(buffer);
                return currentEntry;
            }
        }
        return null;
    }

    public Document docTableDiskSearch(int docId) {
        Document doc = new Document();
        int fileSeekPointer = docId * Constants.DOCUMENT_ENTRY_SIZE_SPLIT1;
        docTableBuffer.position(fileSeekPointer);
        byte[] result = new byte[Constants.DOCUMENT_ENTRY_SIZE_SPLIT1];
        docTableBuffer.get(result, 0, Constants.DOCUMENT_ENTRY_SIZE_SPLIT1);
        doc.deserializeBinarySplit1(result);
        return doc;
    }
}
