package it.unipi.utils;

public final class Constants {

    public static final String[] DIRECTORIES_PATHS = new String[]{"./collection","./resources", "./resources/document_table", "./resources/inverted_index/", "./resources/lexicon/" };
    private static final String RESOURCES_PATH = "./resources/";
    private static final String INVERTED_INDEX_PATH = "inverted_index/";

    public static final String PARTIAL_POSTINGS_DOC_IDS_FILE_PATH = RESOURCES_PATH + INVERTED_INDEX_PATH + "postings_doc_ids";
    public static final String POSTINGS_DOC_IDS_FILE_PATH = RESOURCES_PATH + "postings_doc_ids";
    public static final String PARTIAL_POSTINGS_FREQUENCIES_FILE_PATH = RESOURCES_PATH + INVERTED_INDEX_PATH + "postings_frequencies";
    public static final String POSTINGS_FREQUENCIES_FILE_PATH = RESOURCES_PATH + "postings_frequencies";

    public static final String LEXICON_FILE_PATH = RESOURCES_PATH + "lexicon";
    public static final String PARTIAL_LEXICON_FILE_PATH = RESOURCES_PATH + "lexicon/lexicon";

    public static final String DOCUMENT_TABLE_FILE_PATH = RESOURCES_PATH + "document_table";
    public static final String PARTIAL_DOCUMENT_TABLE_FILE_PATH = RESOURCES_PATH + "document_table/document_table";

    public static final String COLLECTION_PATH = "./collection/collection.tar.gz";
    public static final String STOPWORDS_PATH = RESOURCES_PATH + "stopwords.txt";

    public static final String DAT_FORMAT = ".dat";
    public static final String TXT_FORMAT = ".txt";

    public static final int LEXICON_ENTRY_SIZE = 144;
    // TODO to change too, doc_no max size is 20 chars ok? 20*6 + 2*4
    public static final int DOCUMENT_ENTRY_SIZE = 128;
    //TODO useless now?
    public static final int TERMS_TO_CACHE_DURING_MERGE = 10;
    // todo
    public static final int NUM_POSTINGS_PER_BLOCK = 1024;
    public static final int MAX_TERM_LEN = 30;

}
