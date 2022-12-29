package it.unipi.utils;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

public final class FileSystemUtils {

    // creates the necessary directories in order to avoid exceptions
    public static void setupEnvironment(){
        try {
            for(String directory: Constants.DIRECTORIES_PATHS)
                Files.createDirectories(Paths.get(directory));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // removes the folders containing the partial files
    public static void deleteTemporaryFolders(){
        for(String directory: Constants.TEMPORARY_DIRECTORIES_PATHS) {
            Path pathToBeDeleted = Paths.get(directory);
            try (Stream<Path> files = Files.walk(pathToBeDeleted)) {
                files.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static Configuration loadAppProperties() throws IOException, ConfigurationException {
        Configurations configs = new Configurations();
        Configuration applicationProperties;
        File appProps = new File("application.properties");
        try {
            boolean fileCreated = appProps.createNewFile();
            try {
                applicationProperties = configs.properties(new File("application.properties"));
                // if the file did not exist we add default options
                if (fileCreated) {
                    applicationProperties.addProperty("stemming", true);
                    applicationProperties.addProperty("stopwords", true);
                }
                System.out.println("App properties loaded. Stemming: " + applicationProperties.getBoolean("stemming") + ", stopwords removal: " + applicationProperties.getBoolean("stopwords"));
            } catch (ConfigurationException cex) {
                System.out.println("There was a problem while parsing the application.properties file");
                throw cex;
            }
        } catch (IOException | ConfigurationException e) {
            e.printStackTrace();
            throw e;
        }

        return applicationProperties;
    }
}
