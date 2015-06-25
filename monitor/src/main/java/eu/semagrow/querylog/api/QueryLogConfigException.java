package eu.semagrow.querylog.api;

/**
 * Created by kzam on 5/18/15.
 */
public class QueryLogConfigException extends Exception {

    public QueryLogConfigException(Exception e) {
        super(e);
    }

    public QueryLogConfigException(String msg) {
        super(msg);
    }
}
