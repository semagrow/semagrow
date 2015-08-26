package eu.semagrow.art;

import java.util.UUID;

/**
 * Structured Log Item
 * 
 * <p>
 * Any implementation of this interface can be used to provide to the
 * structured appender information about a logged event.
 * </p> 
 *  
 * @author Stasinos Konstantopoulos
 */

public interface StructuredLogItem
{

	public UUID getQueryUUID();
	public long getStartTime();
	public long getEndTime();

}
