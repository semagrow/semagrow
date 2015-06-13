package eu.semagrow.stack.modules.querylog.api;

/**
 * Created by angel on 10/20/14.
 */
public class QueryLogException extends Exception {

    public QueryLogException(Exception e) {
        super(e);
    }

    public QueryLogException(String msg) { super(msg); }
}
