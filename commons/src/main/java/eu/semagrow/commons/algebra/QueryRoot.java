package eu.semagrow.commons.algebra;

import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.repository.sail.SailQuery;


/**
 * Title: Query Root
 *
 * <p>
 * QueryRoot instances are created to provide a top over the normal top
 * of the expression. For this reason, instantiating a QueryRoot only makes
 * sense in the context of processing a query. The getQuery() method returns
 * a pointer back to that SailQuery instance.
 * </p>
 *  
 * @author Angelos Charalambidis
 * @author Stasinos Konstantopoulos
 */

public class QueryRoot extends org.openrdf.query.algebra.QueryRoot
{
	private org.slf4j.Logger logger =
			org.slf4j.LoggerFactory.getLogger( QueryRoot.class );

	private final SailQuery query;

	/**
	 * Returns the QueryRoot at the top of the AST of any QueryModelNode
	 * @param subexpr
	 * @return
	 */

	public static QueryRoot of( QueryModelNode subexpr )
	{
		 QueryModelNode up = subexpr.getParentNode(), top = subexpr;
		 while( up != null) {
			 top = up; up = up.getParentNode();
		 }
		 assert top instanceof QueryRoot;
		 return (QueryRoot)top;
	}


	public QueryRoot( TupleExpr tupleExpr, SailQuery query )
	{
		super( tupleExpr );
		this.query = query;

		logger.debug( "Created new query root for expression {}", tupleExpr );
	}

	public SailQuery getQuery() { return this.query; }
}
