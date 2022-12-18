package it.unipi.utils;

import it.unipi.models.CollectionStatistics;
import it.unipi.models.Document;
import it.unipi.models.LexiconTerm;
import it.unipi.models.PostingListInterface;

public class ScoringFunctions {
    public static double BM25(Document d, PostingListInterface pl, LexiconTerm term, CollectionStatistics cs){
        int termFreq = pl.getFreq();
        int docFreq = term.getDocumentFrequency();
        // TODO I think we should compute it during indexing and save it in CollectionStatistics, also IDF?
        double avgDocLen = cs.getAvgDocLen();
        // compute partial score
        return ((double) termFreq / ((Constants.K_BM25*((1 - Constants.B_BM25) + Constants.B_BM25 * ( (double) d.getLength() / avgDocLen))) + termFreq)) * Math.log((double) cs.getNumDocs() / docFreq);
    }

    public static double TFIDF(PostingListInterface pl, LexiconTerm term, CollectionStatistics cs){
        int termFreq = pl.getFreq();
        int docFreq = term.getDocumentFrequency();
        return (Math.log((double) cs.getNumDocs() / docFreq)) * (1 + Math.log(termFreq));
    }
}
