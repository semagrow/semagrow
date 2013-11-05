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
	
	/**
	 * @param endpoint
	 * @param vol
	 * @param var
	 */
	public SesameStoreAnswer(URI endpoint, int vol, int var) {
		super();
		this.endpoint = endpoint;
		this.vol = vol;
		this.var = var;
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
	

}
