package eu.semagrow.core.plan;

/**
 * Query Decomposition Exception
 * 
 * <p>This exception wraps exceptions thrown during query decomposition.</p>
 * 
 * @author Angelos Charalambidis
 */

public class QueryDecompositionException extends Exception
{

	private static final long serialVersionUID = 6820239196227181310L;

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
