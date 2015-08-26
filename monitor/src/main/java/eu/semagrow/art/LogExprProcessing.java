package eu.semagrow.art;

import java.util.UUID;

import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;

import eu.semagrow.commons.algebra.QueryRoot;


/**
 * AST Expression Processing Event
 * 
 * @author Stasinos Konstantopoulos
 */

public class LogExprProcessing extends StructuredLogItemBase
{
	
	static public LogExprProcessing create( TupleExpr expr, int processing_layer )
	{
		 QueryModelNode up = expr.getParentNode(), top = expr;
		 while( up != null) {
			 top = up; up = up.getParentNode();
		 }
		 assert top instanceof QueryRoot;
		 return new LogExprProcessing( ((QueryRoot)top).getUUID(), processing_layer );
	}


	private final int processing_layer;

	public LogExprProcessing( UUID queryUUID, int processing_layer )
	{
		super( queryUUID );
		this.processing_layer = processing_layer;
	}

	public int getLayer() { return this.processing_layer; }


	@Override
	public String toString()
	{
		return null;
	}

}
