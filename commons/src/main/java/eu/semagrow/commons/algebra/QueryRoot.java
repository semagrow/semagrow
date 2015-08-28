package eu.semagrow.commons.algebra;

import java.util.UUID;

import org.openrdf.query.algebra.TupleExpr;


public class QueryRoot extends org.openrdf.query.algebra.QueryRoot
{
	private org.slf4j.Logger logger =
			org.slf4j.LoggerFactory.getLogger( QueryRoot.class );

	private final UUID uuid;
	private final String label;

	public QueryRoot( String label )
	{
		this.uuid = UUID.randomUUID();
		this.label = label;
		if( label == null ) {
			logger.debug( "Created new query root with UUID {}", this.uuid );
		}
		else {
			logger.debug( "Created new query root with label {}, UUID {}", label, this.uuid );
		}
	}

	public QueryRoot( String label, TupleExpr tupleExpr )
	{
		super( tupleExpr );
		this.uuid = UUID.randomUUID();
		this.label = label;
		if( label == null ) {
			logger.debug( "Created new query root with UUID {} for expression {}",
					this.uuid, tupleExpr );
		}
		else {
			logger.debug( "Created new query root with label {}, UUID {} for expression {}",
					label, this.uuid, tupleExpr );
		}
	}

	public String getLabel() { return this.label; }
	public UUID getUUID() { return this.uuid; }
}
