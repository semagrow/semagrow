package eu.semagrow.core.impl.planner;

import eu.semagrow.core.impl.util.FilterCollector;
import org.openrdf.query.algebra.*;
import java.util.Collection;


/**
 * Decomposer Context
 * 
 * <p>Holds solution modifiers (such as LIMIT) and FILTER statements
 * that form the context within which query decomposition operates.
 * </p> 
 *  
 * @author Angelos Charalambidis
 * @author Stasinos Konstantopoulos
 */

public class DecomposerContext
{

    private Ordering ordering;
    private Collection<ValueExpr> filters;
    private int limit;

	DecomposerContext( TupleExpr expr )
	{
		this.filters = FilterCollector.process( expr );
	}

    public Ordering getOrdering() { return this.ordering; }
    public Collection<ValueExpr> getFilters() { return this.filters; }
    public int getLimit() { return this.limit; }

}
