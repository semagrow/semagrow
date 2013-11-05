/**
 * 
 */
package eu.semagrow.stack.modules.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

/**
 * @author Giannis Mouchakis
 *
 */
public class ResourceSelector {
	
	private StatementPattern statementPattern;

	/**
	 * @param statementPattern
	 */
	public ResourceSelector(StatementPattern statementPattern) {
		super();
		this.statementPattern = statementPattern;
	}
	
	
	public ProcessedStatement processStatement() {
		ProcessedStatement processedStatement = new ProcessedStatement();
		
		Var subject = statementPattern.getSubjectVar();
		Var predicate = statementPattern.getPredicateVar();
		Var object = statementPattern.getObjectVar();
		
		if (subject.hasValue()) {
			URI uri = extractURI(subject.getValue().toString());
			processedStatement.setSubject(uri);
		}
		if (predicate.hasValue()) {
			URI uri = extractURI(predicate.getValue().toString());
			processedStatement.setPredicate(uri);
		}
		if (object.hasValue()) {
			URI uri = extractURI(object.getValue().toString());
			processedStatement.setObject(uri);
		}
		
		return processedStatement;//TODO: the cases of bindings?
	}
	
	public StatementEquivalents getEquivalents(ProcessedStatement processedStatement) {
		StatementEquivalents statementEquivalents = new StatementEquivalents();
		if (processedStatement.getSubject() != null) {
			PatternDiscovery patternDiscovery = new PatternDiscovery(processedStatement.getSubject());
			statementEquivalents.setSubject_equivalents(patternDiscovery.retrieveEquivalentPatterns());
		}
		if (processedStatement.getPredicate() != null) {
			PatternDiscovery antonis = new PatternDiscovery(processedStatement.getPredicate());
			statementEquivalents.setPredicate_equivalents(antonis.retrieveEquivalentPatterns());
		}
		if (processedStatement.getObject() != null) {
			PatternDiscovery antonis = new PatternDiscovery(processedStatement.getObject());
			statementEquivalents.setObject_equivalents(antonis.retrieveEquivalentPatterns());
		}
		return statementEquivalents;
	}
	
	
	public ArrayList<SesameStoreAnswer> getEndpoints(StatementEquivalents statementEquivalents) {
		
		ArrayList<SesameStoreAnswer> subject_results = new ArrayList<SesameStoreAnswer>();
		ArrayList<SesameStoreAnswer> object_results = new ArrayList<SesameStoreAnswer>();
		ArrayList<SesameStoreAnswer> predicate_results = new ArrayList<SesameStoreAnswer>();
		
		InstanceIndex instanceIndex = new InstanceIndex();
		SchemaIndex shcemaIndex = new SchemaIndex();
		
		instanceIndex.setEquivalent_uris(statementEquivalents.getSubject_equivalents());
		subject_results.addAll(instanceIndex.getEndpoints());
		
		instanceIndex.setEquivalent_uris(statementEquivalents.getObject_equivalents());
		object_results.addAll(instanceIndex.getEndpoints());
		
		shcemaIndex.setEquivalent_uris(statementEquivalents.getPredicate_equivalents());
		predicate_results.addAll(shcemaIndex.getEndpoints());
				
		if (subject_results.isEmpty() && object_results.isEmpty() && predicate_results.isEmpty()) {
			return new ArrayList<SesameStoreAnswer>();//nothing to do, return empty list
		} else if ( !subject_results.isEmpty() && object_results.isEmpty() && predicate_results.isEmpty()) {
			return subject_results;
		} else if ( subject_results.isEmpty() && !object_results.isEmpty() && predicate_results.isEmpty()) {
			return object_results;
		} else if ( subject_results.isEmpty() && object_results.isEmpty() && !predicate_results.isEmpty()) {
			return predicate_results;
		} else if ( !subject_results.isEmpty() && !object_results.isEmpty() && predicate_results.isEmpty()) {
			return findDuplicates(subject_results, object_results);
		} else if ( !subject_results.isEmpty() && object_results.isEmpty() && !predicate_results.isEmpty()) {
			return findDuplicates(subject_results, predicate_results);
		} else if ( subject_results.isEmpty() && !object_results.isEmpty() && !predicate_results.isEmpty()) {
			return findDuplicates(object_results, predicate_results);
		} else {
			ArrayList<SesameStoreAnswer> temp_list = findDuplicates(subject_results, object_results);
			ArrayList<SesameStoreAnswer> final_list = findDuplicates(temp_list, predicate_results);
			return final_list;
		}

	}

	private ArrayList<SesameStoreAnswer> findDuplicates(ArrayList<SesameStoreAnswer> first_list, ArrayList<SesameStoreAnswer> second_list) {
		ArrayList<SesameStoreAnswer> final_list = new ArrayList<SesameStoreAnswer>();
		for (SesameStoreAnswer element_first : first_list) {
			URI first_endpoint = element_first.getEndpoint();
			int first_vol = element_first.getVol();
			int first_var = element_first.getVar();
			
			for (SesameStoreAnswer element_second : second_list) {
				URI second_endpoint = element_second.getEndpoint();
				int second_vol = element_second.getVol();
				int second_var = element_second.getVar();
				if (first_endpoint.equals(second_endpoint) && first_vol == second_vol) {
					int var;
					if (first_var<=second_var) {
						var = first_var;
					} else {
						var = second_var;
					}
					SesameStoreAnswer sesameStoreAnswer = new SesameStoreAnswer(first_endpoint, first_vol, var);
					final_list.add(sesameStoreAnswer);
				}
				
			}
		}
		while (keep_only_min(final_list));
		return final_list;
	}
	
	private boolean keep_only_min(ArrayList<SesameStoreAnswer> list) {
		boolean found = false;
		for (SesameStoreAnswer element_first : list) {
			URI first_endpoint = element_first.getEndpoint();
			int first_vol = element_first.getVol();
			int first_var = element_first.getVar();		
			for (SesameStoreAnswer element_second : list) {
				URI second_endpoint = element_second.getEndpoint();
				int second_vol = element_second.getVol();
				int second_var = element_second.getVar();
				if (first_endpoint.equals(second_endpoint) && first_vol == second_vol && first_var != second_var) {
					if (first_var<second_var) {
						list.remove(element_second);
					} else {
						list.remove(element_first);
					}
					found = true;
					return found;
				}
				
			}
		}
		return found;
	}
	
	
	private URI extractURI(String value) {
		try {
			URI uri = new URI(value);
			return uri;
		} catch (URISyntaxException e) {
			return null;
		}
	}
	

}
