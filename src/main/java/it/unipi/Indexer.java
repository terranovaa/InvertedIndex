package it.unipi;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;

public class Indexer {

    // current doc id
    protected int currentId = 0;
    // useful for checking memory usage
    protected static final Runtime runtime = Runtime.getRuntime();
    // useful for giving different names to partial files
    protected int currentBlock = 0;

    public Indexer() {
    }

    public void indexCollection(String collectionPath) throws IOException {

        File file = new File(collectionPath);
        final TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new FileInputStream(file));
        TarArchiveEntry tarArchiveEntry = tarArchiveInputStream.getNextTarEntry();
        if (tarArchiveEntry != null) {
            byte[] bytes = new byte[2048]; // Random number
            // contains the last document of the block which is not whole
            String lastDocument = "";
            while (tarArchiveInputStream.read(bytes) != -1) {
                // UTF8 or ASCII?
                String test = lastDocument + new String(bytes, StandardCharsets.UTF_8);
                String[] lines = test.split("\n");
                Iterator<String> linesIterator = Arrays.stream(lines).iterator();
                while (linesIterator.hasNext()) {
                    String line = linesIterator.next();
                    // Saving the partial document
                    if (!linesIterator.hasNext()) {
                        lastDocument = line;
                        break;
                    }
                    String doc_id = line.substring(0, line.indexOf("\t"));
                    String document = line.substring(line.indexOf("\t") + 1);
                    // TODO split into tokens and implement SPIMI
                }
            }
        }
    }
}
