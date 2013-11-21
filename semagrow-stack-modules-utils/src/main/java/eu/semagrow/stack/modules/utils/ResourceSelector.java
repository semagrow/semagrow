/**
 * 
 */
package eu.semagrow.stack.modules.utils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.openrdf.model.Resource;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

/**
 * @author Giannis Mouchakis
 *
 */
public class ResourceSelector {
	
	private StatementPattern statementPattern;
	private int measurement_id;

	/**
	 * @param statementPattern
	 * @param measurement_id
	 */
	public ResourceSelector(StatementPattern statementPattern, int measurement_id) {
		super();
		this.statementPattern = statementPattern;
		this.measurement_id = measurement_id;
	}
	
	public ArrayList<SelectedResource> getSelectedResources() throws URISyntaxException, SQLException, ClassNotFoundException, IOException {
		ProcessedStatement processedStatement = processStatement();
		StatementEquivalents statementEquivalents = getEquivalents(processedStatement);
		ArrayList<SelectedResource> resourceList = runResourceDiscovery(statementEquivalents);
		//add load info for each endpoint
		for (SelectedResource resource : resourceList) {
			ArrayList<Measurement> loadInfo = getLoadInfo(resource.getEndpoint());
			resource.setLoadInfo(loadInfo);
		}
		return resourceList;
	}
	
	private ProcessedStatement processStatement() throws URISyntaxException {
		ProcessedStatement processedStatement = new ProcessedStatement();
		
		Var subject = statementPattern.getSubjectVar();
		Var predicate = statementPattern.getPredicateVar();
		Var object = statementPattern.getObjectVar();
		
		if (subject.hasValue() && (subject.getValue() instanceof Resource)) {
			URI uri = new URI(subject.getValue().toString());
			processedStatement.setSubject(uri);
		}
		if (predicate.hasValue() && (predicate.getValue() instanceof Resource)) {
			URI uri = new URI(predicate.getValue().toString());
			processedStatement.setPredicate(uri);
		}
		if (object.hasValue() && (object.getValue() instanceof Resource)) {
			URI uri = new URI(object.getValue().toString());
			processedStatement.setObject(uri);
		}
		
		return processedStatement;
	}
	
	private StatementEquivalents getEquivalents(ProcessedStatement processedStatement) throws ClassNotFoundException, IOException, SQLException, URISyntaxException {
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
	
	
	private ArrayList<SelectedResource> runResourceDiscovery(StatementEquivalents statementEquivalents) throws MalformedURLException, SQLException {
		
		ArrayList<SelectedResource> subject_results = new ArrayList<SelectedResource>();
		ArrayList<SelectedResource> object_results = new ArrayList<SelectedResource>();
		ArrayList<SelectedResource> predicate_results = new ArrayList<SelectedResource>();
		
		for (EquivalentURI equivalentURI : statementEquivalents.getSubject_equivalents()) {
			InstanceIndex instanceIndex = new InstanceIndex();
			URI uri = equivalentURI.getEquivalent_URI();
			ArrayList<SelectedResource> list = instanceIndex.getEndpoints(uri);
			for (SelectedResource resource : list) {
				resource.setSubject(uri);
				resource.setSubjectProximity(equivalentURI.getProximity());
				subject_results.add(resource);
			}
		}
		
		for (EquivalentURI equivalentURI : statementEquivalents.getObject_equivalents()) {
			InstanceIndex instanceIndex = new InstanceIndex();
			URI uri = equivalentURI.getEquivalent_URI();
			ArrayList<SelectedResource> list = instanceIndex.getEndpoints(uri);
			for (SelectedResource resource : list) {
				resource.setObject(uri);
				resource.setObjectProximity(equivalentURI.getProximity());
				object_results.add(resource);
			}
		}
		
		for (EquivalentURI equivalentURI : statementEquivalents.getPredicate_equivalents()) {
			SchemaIndex schemaIndex = new SchemaIndex();
			URI uri = equivalentURI.getEquivalent_URI();
			ArrayList<SelectedResource> list = schemaIndex.getEndpoints(uri);
			for (SelectedResource resource : list) {
				resource.setPredicate(uri);
				resource.setPredicateProximity(equivalentURI.getProximity());
				predicate_results.add(resource);
			}
		}
				
		if (subject_results.isEmpty() && object_results.isEmpty() && predicate_results.isEmpty()) {
			return new ArrayList<SelectedResource>();//nothing to do, return empty list
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
		} else {//none is empty
			ArrayList<SelectedResource> temp_list = findDuplicates(subject_results, object_results);
			ArrayList<SelectedResource> final_list = findDuplicates(temp_list, predicate_results);
			return final_list;
		}

	}

	private ArrayList<SelectedResource> findDuplicates(ArrayList<SelectedResource> first_list, ArrayList<SelectedResource> second_list) {
		ArrayList<SelectedResource> final_list = new ArrayList<SelectedResource>();
		for (SelectedResource element_first : first_list) {
			URI first_endpoint = element_first.getEndpoint();
			int first_vol = element_first.getVol();
			int first_var = element_first.getVar();
			for (SelectedResource element_second : second_list) {
				URI second_endpoint = element_second.getEndpoint();
				int second_vol = element_second.getVol();
				int second_var = element_second.getVar();
				if (first_endpoint.equals(second_endpoint)) {
					int vol;
					int var;
					if (first_vol == second_vol) {
						vol = first_vol;
						if (first_var >= second_var) {
							var = first_var;
						} else {
							var = second_var;
						}
					} else if (first_vol < second_vol) {
						vol = first_vol;
						var = first_var;
					} else {
						vol = second_vol;
						var = second_var;
					}
					
					SelectedResource selectedResource = new SelectedResource(first_endpoint, vol, var);
										
					URI subject;
					int subjectProximity;
					URI predicate;
					int predicateProximity;
					URI object;
					int objectProximity;
					if (element_first.getSubject() != null) {
						subject = element_first.getSubject();
						subjectProximity = element_first.getSubjectProximity();
					} else {
						subject = element_second.getSubject();
						subjectProximity = element_second.getSubjectProximity();
					}
					if (element_first.getPredicate() != null) {
						predicate = element_first.getPredicate();
						predicateProximity = element_first.getPredicateProximity();
					} else {
						predicate = element_second.getPredicate();
						predicateProximity = element_second.getPredicateProximity();
					}
					if (element_first.getObject() != null) {
						object = element_first.getObject();
						objectProximity = element_first.getObjectProximity();
					} else {
						object = element_second.getObject();
						objectProximity = element_second.getObjectProximity();;
					}
					selectedResource.setSubject(subject);
					selectedResource.setSubjectProximity(subjectProximity);
					selectedResource.setPredicate(predicate);
					selectedResource.setPredicateProximity(predicateProximity);
					selectedResource.setObject(object);
					selectedResource.setObjectProximity(objectProximity);
					
					final_list.add(selectedResource);
				}
			}
		}
		return final_list;
	} 
	
	private ArrayList<Measurement> getLoadInfo(URI endpoint) throws SQLException, MalformedURLException {
		ArrayList<Measurement> loadInfo = new ArrayList<Measurement>();
		Connection connection = null;
		try {
			connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/loadinfoDB", "postgres", "root");
			String sql = "SELECT id, measurement_type, measurement FROM load_info WHERE endpoint = ? AND id >=?;";
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setString(1, endpoint.toString());
			preparedStatement.setInt(2, this.measurement_id);
			ResultSet resultSet = preparedStatement.executeQuery();
			while(resultSet.next()) {
				int id = resultSet.getInt("id");
				String measurement_type = resultSet.getString("measurement_type");
				int measurement = resultSet.getInt("measurement");
				Measurement measurementOjb = new Measurement(id, measurement_type, measurement);
				loadInfo.add(measurementOjb);
			}
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
		return loadInfo;
	}

}
