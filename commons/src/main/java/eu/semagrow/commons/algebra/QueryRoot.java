package eu.semagrow.commons.algebra;

import java.util.UUID;

import org.openrdf.query.algebra.TupleExpr;

public class QueryRoot extends org.openrdf.query.algebra.QueryRoot
{
	private final UUID uuid;
	private final String label;

	public QueryRoot( String label )
	{
		this.uuid = UUID.randomUUID();
		this.label = label;
	}

	public QueryRoot( String label, TupleExpr tupleExpr )
	{
		super( tupleExpr );
		this.uuid = UUID.randomUUID();
		this.label = label;
	}

	public String getLabel() { return this.label; }
	public UUID getUUID() { return this.uuid; }
}
