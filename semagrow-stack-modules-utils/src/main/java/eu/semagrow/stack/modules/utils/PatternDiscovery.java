/**
 * 
 */
package eu.semagrow.stack.modules.utils;

import java.net.URI;
import java.util.ArrayList;

/**
 * @author Giannis Mouchakis
 *
 */
public class PatternDiscovery {
	
	private URI uri;

	/**
	 * @param uri
	 */
	public PatternDiscovery(URI uri) {
		super();
		this.uri = uri;
	}
	
	public ArrayList<EquivalentURI> retrieveEquivalentPatterns() {
		ArrayList<EquivalentURI> list = new ArrayList<EquivalentURI>();
		EquivalentURI equri = new EquivalentURI(this.uri, 1, this.uri);
		EquivalentURI equri2 = new EquivalentURI(this.uri, 2, this.uri);
		list.add(equri);
		list.add(equri2);
		return list;
	}

}
