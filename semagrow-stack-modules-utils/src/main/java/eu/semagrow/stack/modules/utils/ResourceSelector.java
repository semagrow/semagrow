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
 * The resource discovery component provides an annotated list of candidate data
 * sources that possibly hold triples matching a given query pattern; including
 * sources that follow a different (but aligned) schema than that of the query
 * pattern. The sources are annotated with schema and instance-level metadata
 * and predicted response volume from the data summaries endpoint; as well as
 * run-time information about current source load. When a source following an
 * aligned schema is used, the annotation also includes relevant
 * meta-information, such as the semantic proximity of the query schema and the
 * source schema.
 * 
 * @author Giannis Mouchakis
 * 
 */
public class ResourceSelector {
	
	private StatementPattern statementPattern;
	private int measurement_id;

	/**
	 * @param statementPattern the StatementPattern to examine
	 * @param measurement_id used to determine how many load info measurements should be returned for each source endpoint.
	 */
	public ResourceSelector(StatementPattern statementPattern, int measurement_id) {
		super();
		this.statementPattern = statementPattern;
		this.measurement_id = measurement_id;
	}
	
	/**
	 * 
	 * Public method to run the ResourceSelector module.
	 * 
	 * @return A list of {@link SelectedResource} objects. Empty list if none found.
	 * @throws URISyntaxException
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
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
	
	/**
	 * This method creates a {@link ProcessedStatement} object with null
	 * subject, predicate, and object field. Then extracts the subject, predicate and
	 * object of the input {@link StatementPattern} and if their value is a {@link org.openrdf.model.URI}
	 * replaces the equivalent field of the created {@link ProcessedStatement}.
	 * 
	 * @return a {@link ProcessedStatement} object
	 * @throws URISyntaxException
	 */
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
	
	/**
	 * Uses {@link PatternDiscovery} to get {@link StatementEquivalents} for the given {@link StatementPattern}.
	 * 
	 * @param processedStatement
	 * @return {@link StatementEquivalents}
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws SQLException
	 * @throws URISyntaxException
	 */
	private StatementEquivalents getEquivalents(ProcessedStatement processedStatement) throws ClassNotFoundException, IOException, SQLException, URISyntaxException {
		StatementEquivalents statementEquivalents = new StatementEquivalents();
		if (processedStatement.getSubject() != null) {
			PatternDiscovery patternDiscovery = new PatternDiscovery(processedStatement.getSubject());
			statementEquivalents.setSubject_equivalents(patternDiscovery.retrieveEquivalentPatterns());
		}
		if (processedStatement.getPredicate() != null) {
			PatternDiscovery patternDiscovery = new PatternDiscovery(processedStatement.getPredicate());
			statementEquivalents.setPredicate_equivalents(patternDiscovery.retrieveEquivalentPatterns());
		}
		if (processedStatement.getObject() != null) {
			PatternDiscovery patternDiscovery = new PatternDiscovery(processedStatement.getObject());
			statementEquivalents.setObject_equivalents(patternDiscovery.retrieveEquivalentPatterns());
		}
		return statementEquivalents;
	}
	
	/**
	 * 
	 * @param statementEquivalents
	 * @return A list of {@link SelectedResource}s after asking the instance and
	 *         schema indices which sources hold triples for the provided
	 *         equivalent URIs
	 * @throws MalformedURLException
	 * @throws SQLException
	 */
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

	/**
	 * Used to determine which combinations of equivalent URIs can be found in a
	 * source endpoint in order to create a candidate query pattern
	 * 
	 * @param first_list
	 * @param second_list
	 * @return a list tha contains only valid URI combinations
	 */
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
	
	/**
	 * 
	 * This method is used to get the load info of an endpoint. The info is
	 * stored in postgresql database, in a table named load_info which contains
	 * the following columns:
	 * 
	 * id (integer): the id of the measurement (incremental). 
	 * measurement_type (text): the type of the measurement.
	 * measurement (integer): the value of the measurement.
	 * endpoint (text): the endpoint for which the measurement was taken.
	 * 
	 * The method returns the load info of an endpoint where id >= measurement_id (passed as {@link ResourceSelector} parameter)
	 * 
	 * @param endpoint the endpoint for which the load info is returned.
	 * @return a list of {@link Measurement}. Empty list if no results.
	 * @throws SQLException
	 * @throws MalformedURLException
	 */
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
