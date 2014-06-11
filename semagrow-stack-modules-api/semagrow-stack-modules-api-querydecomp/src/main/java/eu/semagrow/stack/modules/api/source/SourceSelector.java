package eu.semagrow.stack.modules.api.source;

import org.openrdf.query.algebra.StatementPattern;
import java.util.List;

/**
 * @author Angelos Charalambidis
 */
public interface SourceSelector {

    /**
     * Returns a list of operational endpoints where you can find
     * triples that match the given pattern.
     * @param pattern
     * @return a list of endpoints
     */
    List<SourceMetadata> getSources(StatementPattern pattern);

}
