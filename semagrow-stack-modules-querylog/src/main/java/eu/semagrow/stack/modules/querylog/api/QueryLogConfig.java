package eu.semagrow.stack.modules.querylog.api;

/**
 * Created by kzam on 5/18/15.
 */
public interface QueryLogConfig {

    String getFilename();

    void setFilename(String filename);

    boolean rotate();

    void setCounter(int counter);

    int getCounter();
}
