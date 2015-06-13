package eu.semagrow.stack.modules.querylog;

import eu.semagrow.stack.modules.querylog.api.QueryLogException;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;


/**
 * Created by kzam on 5/14/15.
 */
public class QueryLogManager {

    private String logDir;
    private String filePrefix;
    private File[] listOfFiles;

    public QueryLogManager(String logDir, String filePrefix) {
        this.logDir = logDir;
        this.filePrefix = filePrefix + ".";
    }

    public String getLastFile() throws QueryLogException {

        listOfFiles = getListOfFiles();

        if(listOfFiles == null || listOfFiles.length == 0) {
            String filename = logDir + filePrefix + 0;

            return createFile(filename);
        }

        getNameSorted();

        //return logDir + listOfFiles[listOfFiles.length - 1].getName();
        return logDir + listOfFiles[listOfFiles.length - 1].getName();

    }

    private void getNameSorted() {
        Arrays.sort(listOfFiles, new Comparator<File>() {
            public int compare(File f1, File f2) {
                return Long.valueOf(f1.getName().split(filePrefix)[1]).compareTo(Long.valueOf(f2.getName().split(filePrefix)[1]));
                //return Long.valueOf(f1.lastModified()).compareTo(f2.lastModified());
            }
        });

    }

    private String getLastModified(File[] listOfFiles) {
        String filename = null;
        long max = 0;

        for(int i=0; i<listOfFiles.length; i++) {

            if(listOfFiles[i].lastModified() > max) {
                max = listOfFiles[i].lastModified();
                filename = listOfFiles[i].getName();
            }
        }
        return filename;
    }

    private String createFile(String filename)  throws QueryLogException {
        File file = new File(filename);

        try {
            if(file.createNewFile()) {
                return filename;
            }
        } catch (IOException e) {
            throw new QueryLogException(e);
        }

        throw new QueryLogException("Error in creating new qfr file");
    }

    private File[] getListOfFiles() {
        File folder = new File(logDir);

        File[] foundFiles = folder.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith(filePrefix);
            }
        });

        return foundFiles;
    }

}
