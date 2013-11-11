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
public class SelectedResource {
	
	private URI endpoint;
	private int vol;
	private int var;
	private URI subject = null;
	private int subjectProximity = 0;
	private URI predicate = null;
	private int predicateProximity = 0;
	private URI object = null;
	private int objectProximity = 0;
	private ArrayList<Measurement> loadInfo = null;
	
	/**
	 * @param endpoint
	 * @param vol
	 * @param var
	 */
	public SelectedResource(URI endpoint, int vol, int var) {
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
	
	/**
	 * @return the subject
	 */
	public URI getSubject() {
		return subject;
	}
	
	/**
	 * @param subject the subject to set
	 */
	public void setSubject(URI subject) {
		this.subject = subject;
	}
	
	/**
	 * @return the subjectProximity
	 */
	public int getSubjectProximity() {
		return subjectProximity;
	}
	
	/**
	 * @param subjectProximity the subjectProximity to set
	 */
	public void setSubjectProximity(int subjectProximity) {
		this.subjectProximity = subjectProximity;
	}
	
	/**
	 * @return the predicate
	 */
	public URI getPredicate() {
		return predicate;
	}
	
	/**
	 * @param predicate the predicate to set
	 */
	public void setPredicate(URI predicate) {
		this.predicate = predicate;
	}
	
	/**
	 * @return the predicateProximity
	 */
	public int getPredicateProximity() {
		return predicateProximity;
	}
	
	/**
	 * @param predicateProximity the predicateProximity to set
	 */
	public void setPredicateProximity(int predicateProximity) {
		this.predicateProximity = predicateProximity;
	}
	
	/**
	 * @return the object
	 */
	public URI getObject() {
		return object;
	}
	
	/**
	 * @param object the object to set
	 */
	public void setObject(URI object) {
		this.object = object;
	}
	
	/**
	 * @return the objectProximity
	 */
	public int getObjectProximity() {
		return objectProximity;
	}
	
	/**
	 * @param objectProximity the objectProximity to set
	 */
	public void setObjectProximity(int objectProximity) {
		this.objectProximity = objectProximity;
	}
	

	/**
	 * @return the loadInfo
	 */
	public ArrayList<Measurement> getLoadInfo() {
		return loadInfo;
	}

	/**
	 * @param loadInfo the loadInfo to set
	 */
	public void setLoadInfo(ArrayList<Measurement> loadInfo) {
		this.loadInfo = loadInfo;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SelectedResource [endpoint=" + endpoint + ", vol=" + vol
				+ ", var=" + var + ", subject=" + subject
				+ ", subjectProximity=" + subjectProximity + ", predicate="
				+ predicate + ", predicateProximity=" + predicateProximity
				+ ", object=" + object + ", objectProximity=" + objectProximity
				+ ", loadInfo=" + loadInfo + "]";
	}
	

}
