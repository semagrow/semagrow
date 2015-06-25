package eu.semagrow.query;

import org.openrdf.model.URI;
import org.openrdf.query.Query;
import org.openrdf.query.algebra.TupleExpr;

import java.util.Collection;

/**
 * Created by angel on 6/8/14.
 */
public interface SemagrowQuery extends Query {

    TupleExpr getDecomposedQuery() ;

    void addExcludedSource(URI source);

    void addIncludedSource(URI source);

    Collection<URI> getExcludedSources();

    Collection<URI> getIncludedSources();
}
