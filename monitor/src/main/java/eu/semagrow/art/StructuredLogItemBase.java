package eu.semagrow.art;

import java.util.UUID;
import org.slf4j.MDC;
import org.slf4j.Marker;


/**
 * Title: Structured Log Item Base
 * 
 * <p>
 * Convenience base for implementing {@link StructuredLogItem} classes.
 * Instances of this class represent an event within a specific query
 * processing.
 * </p>
 * 
 * <p>
 * Each query processing is identified by a UUID. All instances of
 * StructuredLogItemBase know the UUTID of the processing they are part of,
 * and their own start and end timestampts. Start time is assigned upon
 * constructing the StructuredLogItemBase and cannot be updated.
 * End time is assigned by the first invocation of endEvent();
 * Subsequent invocations of endEvent() are ignired.
 * </p> 
 *  
 * @author Stasinos Konstantopoulos
 */

public abstract class StructuredLogItemBase implements StructuredLogItem 
{
	protected final long start_time;
	protected long end_time;

	protected StructuredLogItemBase()
	{
		this.start_time = System.currentTimeMillis();
		this.end_time = -1;
		MDC.put( "startTime", Long.toString(this.start_time) );
		MDC.put( "endTime", "UNKNOWN" );
	}


	/**
	 * This method finalizes and the event. Only the first invocation
	 * has an effect; subsequent invocations are ignored.
	 */

	@Override
	public void finalize()
	{
		if( this.end_time == -1 ) {
			this.end_time = System.currentTimeMillis();
			MDC.put( "endTime", Long.toString(this.end_time) );
		}
	}

	@Override
	public long getStartTime() { return this.start_time; }

	@Override
	public long getEndTime() { return this.end_time; }

	@Override
	public abstract String toString();
}
