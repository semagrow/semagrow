/**
 * 
 */
package eu.semagrow.stack.modules.utils;

import java.net.URI;

/**
 * @author Giannis Mouchakis
 *
 */
public class SesameStoreAnswer {
	
	private URI endpoint;
	private int vol;
	private int var;
	private int proximity;
	
	/**
	 * @param endpoint
	 * @param vol
	 * @param var
	 * @param proximity
	 */
	public SesameStoreAnswer(URI endpoint, int vol, int var, int proximity) {
		super();
		this.endpoint = endpoint;
		this.vol = vol;
		this.var = var;
		this.proximity = proximity;
	}

	/**
	 * @return the endpoint
	 */
	public URI getEndpoint() {
		return endpoint;
	}

	/**
	 * @return the vol
	 */
	public int getVol() {
		return vol;
	}

	/**
	 * @return the var
	 */
	public int getVar() {
		return var;
	}

	/**
	 * @return the proximity
	 */
	public int getProximity() {
		return proximity;
	}
	
	
	

}
