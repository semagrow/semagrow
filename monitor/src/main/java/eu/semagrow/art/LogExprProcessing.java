package eu.semagrow.art;

import java.util.UUID;

import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.slf4j.MDC;

import eu.semagrow.commons.algebra.QueryRoot;


/**
 * AST Expression Processing Event
 * 
 * @author Stasinos Konstantopoulos
 */

public class LogExprProcessing extends StructuredLogItemBase
{
	
	static public LogExprProcessing create( TupleExpr expr )
	{
		 QueryModelNode up = expr.getParentNode(), top = expr;
		 while( up != null) {
			 top = up; up = up.getParentNode();
		 }
		 assert top instanceof QueryRoot;
		 
		 return new LogExprProcessing( ((QueryRoot)top).getUUID() );
	}

	private final String nestingLevel;

	public LogExprProcessing( UUID queryUUID )
	{
		super( queryUUID );
		String l = MDC.get( "nestingLevel" );
		if( l == null ) {
			this.nestingLevel = "1";
			MDC.put("nestingLevel", "1");
		}
		else {
			final int nn = Integer.parseInt(l) + 1;
			this.nestingLevel = Integer.toString( nn );
			MDC.put( "nestingLevel", this.nestingLevel );
		}
	}

	@Override
	public void finalize()
	{
		if( this.end_time == -1 ) {
			String l = MDC.get( "nestingLevel" );
			assert l == this.nestingLevel;
			int ll = Integer.parseInt( l ) - 1;
			assert ll >= 0;
			MDC.put( "nestingLevel", Integer.toString(ll) );
			super.finalize();
		}
	}

	@Override
	public String toString()
	{
		return "Execution at level " + this.nestingLevel;
	}

}
