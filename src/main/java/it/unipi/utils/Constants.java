package it.unipi.utils;

public final class Constants {

    public static final String[] DIRECTORIES_PATHS = new String[]{"./resources", "./resources/document_table/", "./resources/inverted_index/", "./resources/lexicon/" };
    public static final String[] TEMPORARY_DIRECTORIES_PATHS = new String[]{"./resources/document_table/", "./resources/inverted_index/", "./resources/lexicon/" };
    private static final String RESOURCES_PATH = "./resources/";
    private static final String INVERTED_INDEX_PATH = "inverted_index/";

    public static final String PARTIAL_POSTINGS_DOC_IDS_FILE_PATH = RESOURCES_PATH + INVERTED_INDEX_PATH + "postings_doc_ids";
    public static final String POSTINGS_DOC_IDS_FILE_PATH = RESOURCES_PATH + "postings_doc_ids";
    public static final String PARTIAL_POSTINGS_FREQUENCIES_FILE_PATH = RESOURCES_PATH + INVERTED_INDEX_PATH + "postings_frequencies";
    public static final String POSTINGS_FREQUENCIES_FILE_PATH = RESOURCES_PATH + "postings_frequencies";

    public static final String MERGED_LEXICON_FILE_PATH = RESOURCES_PATH + "merged_lexicon";
    public static final String LEXICON_FILE_PATH = RESOURCES_PATH + "lexicon";
    public static final String PARTIAL_LEXICON_FILE_PATH = RESOURCES_PATH + "lexicon/lexicon";

    public static final String DOCUMENT_TABLE_FILE_PATH = RESOURCES_PATH + "document_table";
    public static final String PARTIAL_DOCUMENT_TABLE_FILE_PATH = RESOURCES_PATH + "document_table/document_table";

    public static final String COLLECTION_STATISTICS_FILE_PATH = RESOURCES_PATH + "collection_statistics";

    public static final String COLLECTION_PATH = "./collection/collection.tar.gz";
    public static final String STOPWORDS_PATH = RESOURCES_PATH + "stopwords.txt";

    public static final String DAT_FORMAT = ".dat";
    public static final String TXT_FORMAT = ".txt";

    public static final double MEMORY_FULL_THRESHOLD_PERCENTAGE = 0.75;
    public static final double MEMORY_ENOUGH_THRESHOLD_PERCENTAGE = 0.25;

    // 20 (term) + 4 (df) + 4 (cf) + 8 (docIdOffset) + 8 (freqOffset) + 4 (docIdSize) + 4 (docIdSize) + 8 (termUpperBound) = 92
    public static final int LEXICON_ENTRY_SIZE = 60;
    // doc_no 30 byte + 2*4
    public static final int DOCUMENT_ENTRY_SIZE = 38;
    // docid + doclen
    public static final int DOCUMENT_ENTRY_SIZE_SPLIT1 = 8;
    // docid + docno
    public static final int DOCUMENT_ENTRY_SIZE_SPLIT2 = 34;

    // todo
    public static final int SKIP_POINTERS_THRESHOLD = 1024;
    public static final int MAX_TERM_LEN = 20;

    public static final double B_BM25 = 0.75;
    public static final double K_BM25 = 1.2;

    public static final int NUMBER_OF_OUTPUT_DOCUMENTS = 10;
    public static final int MAX_QUERY_LENGTH = 32;

}
