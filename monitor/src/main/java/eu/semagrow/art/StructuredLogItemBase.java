package eu.semagrow.art;

import java.util.UUID;


/**
 * Structured Log Item Base
 * 
 * <p>Convenience base for implementing {@link StructuredLogItem}</p> 
 *  
 * @author Stasinos Konstantopoulos
 */

public abstract class StructuredLogItemBase implements StructuredLogItem 
{
	private final UUID queryUUID;
	private final long start_time;
	private long end_time;

	protected StructuredLogItemBase( UUID queryUUID )
	{
		this.queryUUID = queryUUID;
		this.start_time = System.currentTimeMillis();
		this.end_time = -1;
	}

	public void endEvent()
	{
		this.end_time = System.currentTimeMillis();
	}

	@Override
	public UUID getQueryUUID() { return this.queryUUID; }

	@Override
	public long getStartTime() { return this.start_time; }

	@Override
	public long getEndTime() { return this.end_time; }

	@Override
	public abstract String toString();
}
