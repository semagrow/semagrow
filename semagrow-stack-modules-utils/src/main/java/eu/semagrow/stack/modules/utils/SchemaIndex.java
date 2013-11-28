/**
 * 
 */
package eu.semagrow.stack.modules.utils;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * @author Giannis Mouchakis
 *
 */
public class SchemaIndex {
	
	/**
	 *
	 */
	public SchemaIndex() {
		super();
	}

	public List<SelectedResource> getEndpoints(URI uri) {//TODO:remove dummy
		List<SelectedResource> list = new ArrayList<SelectedResource>();
		ValueFactory valueFactory = new ValueFactoryImpl();
		SelectedResource selectedResource1 = new SelectedResource(valueFactory.createURI("http://a"), 100, 1);
		SelectedResource selectedResource2 = new SelectedResource(valueFactory.createURI("http://b"), 10, 2);
		SelectedResource selectedResource3 = new SelectedResource(valueFactory.createURI("http://c"), 100, 1);
		SelectedResource selectedResource4 = new SelectedResource(valueFactory.createURI("http://d"), 100, 1);
		SelectedResource selectedResource5 = new SelectedResource(valueFactory.createURI("http://e"), 100, 1);
		list.add(selectedResource1);
		list.add(selectedResource2);
		list.add(selectedResource3);
		list.add(selectedResource4);
		list.add(selectedResource5);
		return list;
	}

}
