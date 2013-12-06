/**
 * 
 */
package eu.semagrow.stack.modules.utils.impl;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.URI;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

import eu.semagrow.stack.modules.utils.EquivalentURI;
import eu.semagrow.stack.modules.utils.InstanceIndex;
import eu.semagrow.stack.modules.utils.Measurement;
import eu.semagrow.stack.modules.utils.PatternDiscovery;
import eu.semagrow.stack.modules.utils.ProsessedStatement;
import eu.semagrow.stack.modules.utils.ResourceSelector;
import eu.semagrow.stack.modules.utils.SchemaIndex;
import eu.semagrow.stack.modules.utils.SelectedResource;
import eu.semagrow.stack.modules.utils.StatementEquivalents;


/* (non-Javadoc)
 * @see eu.semagrow.stack.modules.utils.ResourceSelector
 */
public class ResourceSelectorImpl implements ResourceSelector {
	

	/**
	 * constructor for the ResourceSelectorImpl.
	 */
	public ResourceSelectorImpl() {
		super();
	}
	
	/* (non-Javadoc)
	 * @see eu.semagrow.stack.modules.utils.ResourceSelector#getSelectedResources()
	 */
	public List<SelectedResource> getSelectedResources(StatementPattern statementPattern, int measurement_id) {
		ProsessedStatement processedStatement = processStatement(statementPattern);
		StatementEquivalents statementEquivalents = null;
		try {
			statementEquivalents = getEquivalents(processedStatement);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (statementEquivalents == null) {
			return null;
		}
		List<SelectedResource> resourceList = runResourceDiscovery(statementEquivalents);
		//add load info for each endpoint
		try {
			for (SelectedResource resource : resourceList) {
				List<Measurement> loadInfo = getLoadInfo(resource.getEndpoint(), measurement_id);
				resource.setLoadInfo(loadInfo);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return resourceList;
	}
	
	/**
	 * This method creates a {@link ProcessedStatement} object with null
	 * subject, predicate, and object field. Then extracts the subject, predicate and
	 * object of the input {@link StatementPattern} and if their value is a {@link org.openrdf.model.URI}
	 * replaces the equivalent field of the created {@link ProcessedStatement}.
	 * 
	 * @param statementPattern the StatementPattern to examine
	 * @return a {@link ProcessedStatement} object
	 */
	private ProsessedStatement processStatement(StatementPattern statementPattern) {
		ProsessedStatement processedStatement = new ProcessedStatementImpl();
		
		Var subject = statementPattern.getSubjectVar();
		Var predicate = statementPattern.getPredicateVar();
		Var object = statementPattern.getObjectVar();
		
		if (subject.hasValue() && (subject.getValue() instanceof URI)) {
			processedStatement.setSubject((URI)subject.getValue());
		}
		if (predicate.hasValue() && (predicate.getValue() instanceof URI)) {
			processedStatement.setPredicate((URI)predicate.getValue());
		}
		if (object.hasValue() && (object.getValue() instanceof URI)) {
			processedStatement.setObject((URI)object.getValue());
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
	 */
	private StatementEquivalents getEquivalents(ProsessedStatement processedStatement) throws ClassNotFoundException, IOException, SQLException {
		StatementEquivalents statementEquivalents = new StatementEquivalentsImpl();
		if (processedStatement.getSubject() != null) {
			PatternDiscovery patternDiscovery = new PatternDiscoveryImpl(processedStatement.getSubject());
			statementEquivalents.setSubject_equivalents(patternDiscovery.retrieveEquivalentPatterns());
		}
		if (processedStatement.getPredicate() != null) {
			PatternDiscovery patternDiscovery = new PatternDiscoveryImpl(processedStatement.getPredicate());
			statementEquivalents.setPredicate_equivalents(patternDiscovery.retrieveEquivalentPatterns());
		}
		if (processedStatement.getObject() != null) {
			PatternDiscovery patternDiscovery = new PatternDiscoveryImpl(processedStatement.getObject());
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
	 * @throws SQLException
	 */
	private List<SelectedResource> runResourceDiscovery(StatementEquivalents statementEquivalents) {
		
		List<SelectedResource> subject_results = new ArrayList<SelectedResource>();
		List<SelectedResource> object_results = new ArrayList<SelectedResource>();
		List<SelectedResource> predicate_results = new ArrayList<SelectedResource>();
		
		for (EquivalentURI equivalentURI : statementEquivalents.getSubject_equivalents()) {
			InstanceIndex instanceIndex = new InstanceIndexImpl();
			URI uri = equivalentURI.getEquivalent_URI();
			List<SelectedResource> list = instanceIndex.getEndpoints(uri);
			for (SelectedResource resource : list) {
				resource.setSubject(uri);
				resource.setSubjectProximity(equivalentURI.getProximity());
				subject_results.add(resource);
			}
		}
		
		for (EquivalentURI equivalentURI : statementEquivalents.getObject_equivalents()) {
			InstanceIndex instanceIndex = new InstanceIndexImpl();
			URI uri = equivalentURI.getEquivalent_URI();
			List<SelectedResource> list = instanceIndex.getEndpoints(uri);
			for (SelectedResource resource : list) {
				resource.setObject(uri);
				resource.setObjectProximity(equivalentURI.getProximity());
				object_results.add(resource);
			}
		}
		
		for (EquivalentURI equivalentURI : statementEquivalents.getPredicate_equivalents()) {
			SchemaIndex schemaIndex = new SchemaIndexImpl();
			URI uri = equivalentURI.getEquivalent_URI();
			List<SelectedResource> list = schemaIndex.getEndpoints(uri);
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
			List<SelectedResource> temp_list = findDuplicates(subject_results, object_results);
			List<SelectedResource> final_list = findDuplicates(temp_list, predicate_results);
			return final_list;
		}

	}

	/**
	 * Used to determine which combinations of equivalent URIs can be found in a
	 * source endpoint in order to create a candidate query pattern
	 * 
	 * @param first_list
	 * @param second_list
	 * @return a list that contains only valid URI combinations
	 */
	private List<SelectedResource> findDuplicates(List<SelectedResource> first_list, List<SelectedResource> second_list) {
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
					
					SelectedResource selectedResource = new SelectedResourceImpl(first_endpoint, vol, var);
										
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
	 * The method returns the load info of an endpoint where id >= measurement_id
	 * 
	 * @param endpoint the endpoint for which the load info is returned.
	 * @param measurement_id used to determine how many load info measurements should be returned for each source endpoint.
	 * @return a list of {@link Measurement}. Empty list if no results.
	 * @throws SQLException
	 */
	private List<Measurement> getLoadInfo(URI endpoint, int measurement_id) throws SQLException {
		List<Measurement> loadInfo = new ArrayList<Measurement>();
		Connection connection = null;
		try {
			connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/loadinfoDB", "postgres", "root");
			String sql = "SELECT id, measurement_type, measurement FROM load_info WHERE endpoint = ? AND id >=?;";
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setString(1, endpoint.toString());
			preparedStatement.setInt(2, measurement_id);
			ResultSet resultSet = preparedStatement.executeQuery();
			while(resultSet.next()) {
				int id = resultSet.getInt("id");
				String measurement_type = resultSet.getString("measurement_type");
				int measurement = resultSet.getInt("measurement");
				Measurement measurementOjb = new MeasurementImpl(id, measurement_type, measurement);
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
