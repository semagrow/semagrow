/**
 * 
 */
package eu.semagrow.stack.modules.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

/**
 * @author Giannis Mouchakis
 *
 */
public class InstanceIndex {
	
	/**
	 * 
	 */
	public InstanceIndex() {
		super();
	}

	public ArrayList<SelectedResource> getEndpoints(URI uri) {//TODO:remove dummy
		ArrayList<SelectedResource> list = new ArrayList<SelectedResource>();
		try {
			SelectedResource selectedResource1 = new SelectedResource(new URI("http//:a"), 100, 1);
			SelectedResource selectedResource2 = new SelectedResource(new URI("http//:b"), 10, 2);
			SelectedResource selectedResource3 = new SelectedResource(new URI("http//:c"), 100, 1);
			list.add(selectedResource1);
			list.add(selectedResource2);
			list.add(selectedResource3);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}

}
