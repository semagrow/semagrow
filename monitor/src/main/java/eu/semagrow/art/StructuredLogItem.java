package eu.semagrow.art;


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

	public long getStartTime();
	public long getEndTime();
	void finalize();

}
