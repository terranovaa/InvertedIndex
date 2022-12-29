package it.unipi.utils;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class FileSystemUtilsTest {

    @Test
    void loadAppPropertiesTest() throws ConfigurationException, IOException {
        Configuration appProps = FileSystemUtils.loadAppProperties();
        assert appProps.getBoolean("stemming");
        assert appProps.getBoolean("stopwords");
    }
}
