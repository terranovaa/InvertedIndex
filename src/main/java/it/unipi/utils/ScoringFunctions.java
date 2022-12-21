package it.unipi.utils;

import it.unipi.models.CollectionStatistics;
import it.unipi.models.LexiconTerm;

public class ScoringFunctions {
    public static double BM25(int docLength, int termDocFreq, LexiconTerm term, CollectionStatistics cs){
        int docFreq = term.getDocumentFrequency();
        double avgDocLen = cs.getAvgDocLen();
        // compute partial score
        return ((double) termDocFreq / ((Constants.K_BM25*((1 - Constants.B_BM25) + Constants.B_BM25 * ( (double) docLength / avgDocLen))) + termDocFreq)) * Math.log((double) cs.getNumDocs() / docFreq);
    }

    public static double TFIDF(int termDocFreq, LexiconTerm term, CollectionStatistics cs){
        int docFreq = term.getDocumentFrequency();
        return (Math.log((double) cs.getNumDocs() / docFreq)) * (1 + Math.log(termDocFreq));
    }
}
