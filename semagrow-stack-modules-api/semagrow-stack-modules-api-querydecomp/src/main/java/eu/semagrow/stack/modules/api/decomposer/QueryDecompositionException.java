package eu.semagrow.stack.modules.api.decomposer;

/**
 * Created by angel on 6/16/14.
 */
public class QueryDecompositionException extends Exception {

    public QueryDecompositionException(String message) {
        super(message);
    }

    public QueryDecompositionException(String message, Throwable cause) {
        super(message,cause);
    }

    public QueryDecompositionException(Throwable cause) {
        super(cause);
    }
}
