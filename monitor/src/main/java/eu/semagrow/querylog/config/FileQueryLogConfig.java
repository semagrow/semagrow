package eu.semagrow.querylog.config;

import eu.semagrow.querylog.api.QueryLogConfig;

/**
 * Created by kzam on 5/18/15.
 */
public class FileQueryLogConfig implements QueryLogConfig {

    private String filename;
    private int counter;

    @Override
    public String getFilename() {
        return filename;
    }

    @Override
    public void setFilename(String filename) {
        this.filename = filename;
    }

    @Override
    public boolean rotate() { return true; }

    @Override
    public void setCounter(int counter) {
        this.counter = counter;
    }

    @Override
    public int getCounter() {
        return counter;
    }
}
