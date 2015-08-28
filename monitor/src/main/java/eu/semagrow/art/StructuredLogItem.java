package eu.semagrow.art;

import java.util.UUID;

import org.slf4j.Logger;

/**
 * Structured Log Item
 * 
 * <p>
 * Implementations of this interface provide to loggers
 * structured information about a logged event.
 * </p> 
 *  
 * @author Stasinos Konstantopoulos
 */

public interface StructuredLogItem
{

	public UUID getQueryUUID();
	public long getStartTime();
	public long getEndTime();
	void finalize();

}
