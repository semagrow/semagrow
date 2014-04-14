/**
 * 
 */
package eu.semagrow.stack.modules.utils.resourceselector.impl;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import eu.semagrow.stack.modules.utils.resourceselector.InstanceIndex;
import eu.semagrow.stack.modules.utils.resourceselector.SelectedResource;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/* (non-Javadoc)
 * @see eu.semagrow.stack.modules.utils.resourceselector.InstanceIndex
 */
public class InstanceIndexImpl implements InstanceIndex {
	
	/**
	 * 
	 */
	public InstanceIndexImpl() {
		super();
	}

	/* (non-Javadoc)
	 * @see eu.semagrow.stack.modules.utils.resourceselector.InstanceIndexInterface#getEndpoints(org.openrdf.model.URI)
	 */
	public List<SelectedResource> getEndpoints(URI uri) {//TODO:remove dummy	
		String logger_message = "try to discover possible sources for URI:" + uri.toString();
		Logger.getLogger(InstanceIndexImpl.class.getName()).log(Level.INFO, logger_message);
		List<SelectedResource> list = new ArrayList<SelectedResource>();
		ValueFactory valueFactory = new ValueFactoryImpl();
		String uri_str = uri.toString();
		if (uri_str.startsWith("http://ontologies.seamless-ip.org/crop.owl#")) {
			list.add(new SelectedResourceImpl(valueFactory.createURI("http://www.semagrow.eu:8080/bigdata_seamless/sparql"), 5, 2));
		} else if (uri_str.startsWith("http://ontologies.seamless-ip.org/farm.owl#")) {
			list.add(new SelectedResourceImpl(valueFactory.createURI("http://www.semagrow.eu:8080/bigdata_seamless/sparql"), 35, 11));			
		} else if (uri_str.startsWith("http://semagrow.eu/schemas/eururalis#")) {
			list.add(new SelectedResourceImpl(valueFactory.createURI("http://www.semagrow.eu:8080/bigdata_t4f/sparql"), 8, 3));
			list.add(new SelectedResourceImpl(valueFactory.createURI("http://www.semagrow.eu:8080/bigdata_eururalis/sparql"), 2, 1));
		} else if (uri_str.startsWith("http://semagrow.eu/schemas/t4f#")) {
			list.add(new SelectedResourceImpl(valueFactory.createURI("http://www.semagrow.eu:8080/bigdata_t4f/sparql"), 32, 10));	
		} else if (uri_str.startsWith("http://purl.org/dc/terms/")) {
			list.add(new SelectedResourceImpl(valueFactory.createURI("http://www.semagrow.eu:8080/bigdata_laflor/sparql"), 9, 5));	
			list.add(new SelectedResourceImpl(valueFactory.createURI("http://www.semagrow.eu:8080/bigdata_t4f/sparql"), 8, 4));	
		} else if (uri_str.startsWith("http://semagrow.eu/schemas/laflor#")) {
			list.add(new SelectedResourceImpl(valueFactory.createURI("http://www.semagrow.eu:8080/bigdata_laflor/sparql"), 18, 9));	
		} else if (uri_str.startsWith("http://www.w3.org/2003/01/geo/wgs84_pos#")) {
			list.add(new SelectedResourceImpl(valueFactory.createURI("http://www.semagrow.eu:8080/bigdata_laflor/sparql"), 18, 9));
		} else if (uri_str.startsWith("http://aims.fao.org/aos/agrovoc/")) {
			list.add(new SelectedResourceImpl(valueFactory.createURI("http://www.semagrow.eu:8080/bigdata_laflor/sparql"), 9, 5));
		} else if (uri_str.startsWith("http://schema.org/")) {
			list.add(new SelectedResourceImpl(valueFactory.createURI("http://www.semagrow.eu:8080/bigdata_laflor/sparql"), 9, 6));
		} else if (uri_str.startsWith("http://id.loc.gov/")) {
			list.add(new SelectedResourceImpl(valueFactory.createURI("http://www.semagrow.eu:8080/bigdata_laflor/sparql"), 9, 4));
		}
		String logger_message_result = "";
		for (SelectedResource selectedResource : list) {
			logger_message_result += "found candidate source " + selectedResource.getEndpoint().toString() + " containing " + selectedResource.getVol() + " results\n";
		}
		Logger.getLogger(InstanceIndexImpl.class.getName()).log(Level.INFO, logger_message_result);
		/*SelectedResource selectedResource1 = new SelectedResourceImpl(valueFactory.createURI("http://a"), 100, 1);
		SelectedResource selectedResource2 = new SelectedResourceImpl(valueFactory.createURI("http://b"), 10, 2);
		SelectedResource selectedResource3 = new SelectedResourceImpl(valueFactory.createURI("http://c"), 100, 1);
		list.add(selectedResource1);
		list.add(selectedResource2);
		list.add(selectedResource3);*/
		return list;
	}

}
