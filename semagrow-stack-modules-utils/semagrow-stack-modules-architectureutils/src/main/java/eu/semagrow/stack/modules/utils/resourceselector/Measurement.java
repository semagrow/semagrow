package eu.semagrow.stack.modules.utils.resourceselector;

/**
 * This interface specifies classes of load info and similar measurements
 * regarding the current performance and availability of a data source.
 * 
 * @author Giannis Mouchakis
 * @author Stasinos Konstantopoulos
 *
 */
public interface Measurement {

	/**
	 * This method returns the unique numerical identifier of this
	 * measurement instance. This identifier is a timestamp, an
	 * increment, or, in general, any strictly monotone value series.
	 * This guarantees that a client only needs to retain the largest
	 * id received in order to be able to request all subsequent
	 * Measurement instances.
	 * 
	 * @return the measurement id
	 */
	public long getId();

	/**
	 * This method returns a textual name or description of the kind
	 * of information held in this Measurement instance. 
	 * 
	 * @return the type of the measurement
	 */
	public String getType();

	/**
	 * This method returns the actual value of the measurement. 
	 * 
	 * @return the value of the measurement
	 */
	public int getValue();

}