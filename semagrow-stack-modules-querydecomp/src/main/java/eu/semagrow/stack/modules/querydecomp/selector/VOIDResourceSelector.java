package eu.semagrow.stack.modules.querydecomp.selector;

import eu.semagrow.stack.modules.api.Measurement;
import eu.semagrow.stack.modules.api.ResourceSelector;
import eu.semagrow.stack.modules.api.SelectedResource;
import eu.semagrow.stack.modules.utils.resourceselector.impl.MeasurementImpl;
import eu.semagrow.stack.modules.utils.resourceselector.impl.SelectedResourceImpl;
import eu.semagrow.stack.modules.vocabulary.VOID;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.*;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import java.util.*;

import static org.openrdf.query.QueryLanguage.SPARQL;

/**
 * Created by angel on 3/17/14.
 */
public class VOIDResourceSelector implements ResourceSelector {

    private static final String NCSRDNamespace = "http://rdf.iit.demokritos.gr/2014/smo#";

    private static final String PREFIX =
            "PREFIX void: <" + VOID.NAMESPACE + ">\n" +
            "PREFIX rdf: <" + RDF.NAMESPACE +">\n" +
            "PREFIX ncsrd: <" + NCSRDNamespace + ">\n";

    private static final String VAR_TYPE  = "$TYPE$";

    private static final String VAR_PRED  = "$PRED$";

    private static final String VAR_DATASET = "$DATASET$";

    private static final String TRIPLE_COUNT = PREFIX +
            "SELECT ?count WHERE {" +
            "  <" + VAR_DATASET +"> rdf:a void:Dataset ;" +
            "     void:triples ?count." +
            "}";

    private static final String PRED_TRIPLES2 = PREFIX +
            "SELECT ?count WHERE {" +
            " <" + VAR_DATASET + "> rdf:a void:Dataset ; " +
            "    void:property \"" + VAR_PRED + "\" ;" +
            "    void:triples ?count ." +
            "}";

    private static final String SUBJECTS_TRIPLES2 = PREFIX +
            "SELECT ?count WHERE {" +
            " <" + VAR_DATASET +"> rdf:a void:Dataset ; " +
            "    void:uriRegexPattern ?pattern ; " +
            "    void:triples ?count ." +
            " FILTER REGEX(" + VAR_TYPE + ", ?pattern)" +
            "}";

    private static final String SUBJECTS_DATASETS = PREFIX +
            "SELECT ?dataset WHERE {" +
            " ?dataset rdf:a void:Dataset ; " +
            "    void:uriRegexPattern \"" + VAR_TYPE + "\"" +
            "}";

    private static final String PRED_DATASETS = PREFIX +
            "SELECT ?dataset WHERE {" +
            " ?dataset rdf:type void:Dataset ; " +
            "    void:property <" + VAR_PRED + "> ." +
            "}";

    private static final String DATASET_SOURCE = PREFIX +
            "SELECT ?source WHERE {" +
            " <" + VAR_DATASET +"> rdf:a void:Dataset ; " +
            "  void:sparqlEndpoint ?source. " +
            "}";

    private static final String DATASET_PARENT = PREFIX +
            "SELECT ?dataset WHERE {" +
            " <" + VAR_DATASET + "> rdf:a void:Dataset ; " +
            "          void:subset ?dataset" +
            "}";

    private static final ValueFactory factory = ValueFactoryImpl.getInstance();

    private Repository voidRepository = null;

    public Repository getRepository() {
        return voidRepository;
    }

    public void setRepository(Repository voidRepository) {
        this.voidRepository = voidRepository;
    }

    /**
     * Returns the count value defined by the supplied query and variable substitutions.
     *
     * @param query the query to be executed on the voiD repository.
     * @param vars the variable bindings to be substituted in the query.
     * @return the resulting count value.
     */
    private long getCount(String query, String... vars) {

        // replace query variables
        for (int i = 0; i < vars.length; i++) {
            query = query.replace(vars[i], vars[++i]);
        }

        List<String> bindings = evalQuery(query, "count");

        // check result validity
        if (bindings.size() == 0) {

            return -1;
        }

        return Long.parseLong(bindings.get(0));
    }

    /**
     * Evaluates the given query and returns the result values for the specified binding name.
     *
     * @param query the query to evaluate.
     * @param bindingName the desired result bindings.
     * @return a list of result binding values.
     */
    private List<String> evalQuery(String query, String bindingName) {
        try {
            RepositoryConnection con = this.getRepository().getConnection();
            try {
                TupleQuery tupleQuery = con.prepareTupleQuery(SPARQL, query);
                TupleQueryResult result = tupleQuery.evaluate();

                try {
                    List<String> bindings = new ArrayList<String>();
                    while (result.hasNext()) {
                        bindings.add(result.next().getValue(bindingName).stringValue());
                    }
                    return bindings;
                } catch (QueryEvaluationException e) {

                } finally {
                    result.close();
                }
            } catch (IllegalArgumentException e) {

            } catch (RepositoryException e) {

            } catch (MalformedQueryException e) {

            } catch (QueryEvaluationException e) {

            } finally {
                con.close();
            }
        } catch (RepositoryException e) {

        }
        return null;
    }


    private List<Value> evalQueryValue(String query, String bindingName) {
        try {
            RepositoryConnection con = this.getRepository().getConnection();
            try {
                TupleQuery tupleQuery = con.prepareTupleQuery(SPARQL, query);
                TupleQueryResult result = tupleQuery.evaluate();

                try {
                    List<Value> bindings = new ArrayList<Value>();
                    while (result.hasNext()) {
                        bindings.add(result.next().getValue(bindingName));
                    }
                    return bindings;
                } catch (QueryEvaluationException e) {

                } finally {
                    result.close();
                }
            } catch (IllegalArgumentException e) {

            } catch (RepositoryException e) {

            } catch (MalformedQueryException e) {

            } catch (QueryEvaluationException e) {

            } finally {
                con.close();
            }
        } catch (RepositoryException e) {

        }
        return null;
    }

    public Set<URI> getDatasets(StatementPattern pattern) {
        Value s = pattern.getSubjectVar().getValue();
        Value p = pattern.getPredicateVar().getValue();
        Value o = pattern.getObjectVar().getValue();

        return getDatasets(s == null ? null : s.stringValue(),
                p == null ? null : p.stringValue(),
                o == null ? null : o.stringValue());
    }

    public Set<URI> getDatasets(String sValue, String pValue, String oValue) {

        Set<URI> sources = null;

        String query = null;

        if (pValue != null) {
            query = PRED_DATASETS.replace(VAR_PRED, pValue);

            if (sources == null)
                sources = new HashSet<URI>();

            List<String> result = evalQuery(query, "dataset");
            if (result != null) {
                for (String dataset : result)
                    sources.add(factory.createURI(dataset));
            }
        }

        if (sValue != null) {
            query = SUBJECTS_DATASETS.replace(VAR_TYPE, sValue);

            Set<URI> sources2 = new HashSet<URI>();
            List<String> result = evalQuery(query, "dataset");
            if (result != null) {

                for (String dataset : evalQuery(query, "dataset"))
                    sources2.add(factory.createURI(dataset));

                if (sources == null) {
                    sources = sources2;
                } else {
                    sources.retainAll(sources2);
                }
            }
        }

        if (sources == null)
            sources = new HashSet<URI>();

        return sources;
    }

    public Set<URI> getSource(URI dataset) {
        String query = DATASET_SOURCE.replace(VAR_DATASET, dataset.toString());
        List<String> result = evalQuery(query, "source");
        if (result != null && result.size() > 0) {
            Set<URI> sources = new HashSet<URI>();
            for (String r : result)
                sources.add(factory.createURI(r));

            return sources;
        }
        else
        {
            // find parent dataset and repeat
            URI parentDataset = getParent(dataset);
            if (parentDataset != null)
                return getSource(parentDataset);
            else
                return new HashSet<URI>();
        }
    }

    private URI getParent(URI dataset) {
        String query = DATASET_PARENT.replace(VAR_DATASET, dataset.toString());
        List<String> result = evalQuery(query, "dataset");
        if (result == null || result.size() == 0)
            return null;

        return factory.createURI(result.iterator().next());
    }

    public long getTripleCount(URI g) {
        return getCount(TRIPLE_COUNT, VAR_DATASET, g.stringValue());
    }

    public long getPredicateCount(URI g, String predicate) {
        return getCount(PRED_TRIPLES2, VAR_DATASET, g.stringValue(), VAR_PRED, predicate);
    }

    public long getSubjectCount(URI g, String subject) {
        return getCount(SUBJECTS_TRIPLES2, VAR_DATASET, g.stringValue(), VAR_TYPE, subject);
    }

    public long getPatternCount(URI source, StatementPattern pattern) {
        Value sVal = pattern.getSubjectVar().getValue();
        Value pVal = pattern.getPredicateVar().getValue();
        Value oVal = pattern.getObjectVar().getValue();

        if (sVal != null && pVal != null && oVal != null)
            return 1;

        Number resultSize;

        if (pVal == null) {
            resultSize = getTripleCount(source);
        } else {
            resultSize = getPredicateCount(source, pVal.stringValue());
        }

        /*
        // object is bound

        if (oVal != null) {

                long pCount = getDistinctPredicates(source);
                long distObj = getDistinctObjects(source);
                return (long) resultSize.doubleValue() * pCount / distObj;
        }
        */

        // subject is bound
        if (sVal != null) {
                return (long) resultSize.doubleValue() / getSubjectCount(source, sVal.stringValue());
                //long pCount = getDistinctPredicates(source);
                //long distSubj = getDistinctSubjects(source);
                //return (long) resultSize.doubleValue() * pCount / distSubj;
        }

        // use triple count containing the predicate
        return resultSize.longValue();
    }

    private SelectedResource getResource(List<SelectedResource> resources) {
        if (resources.size() == 0)
            return null;

        int estimatedVol = Integer.MAX_VALUE;
        SelectedResource selected = null;

        for (SelectedResource r  : resources) {
            if (r.getVol() < estimatedVol) {
                selected = r;
            }
        }

        return selected;
    }

    private int getUserPreference(URI endpoint) {
        String query = PREFIX +
                "SELECT ?preference WHERE {\n"+
                " [] rdf:a void:Dataset ;\n"  +
                "  void:sparqlEndpoint  \"" + endpoint + "\";\n" +
                "  ncsrd:userPreference ?preference.\n" +
                "}";

        List<String> bindings = evalQuery(query, "preference");

        // check result validity
        if (bindings.size() == 0) {

            return -1;
        }

        return (int) Long.parseLong(bindings.get(0));
    }

    private List<Measurement> getLoadInfo(URI endpoint) {
        int preference = getUserPreference(endpoint);
        Measurement measurement = new MeasurementImpl(1, "userPreference", preference);
        List<Measurement> list = new ArrayList<Measurement>();
        list.add(measurement);
        return list;
    }

    public List<SelectedResource> getSelectedResources(StatementPattern statementPattern, long measurement_id) {

        Set<URI> datasets = getDatasets(statementPattern);
        ArrayList<SelectedResource> resources = new ArrayList<SelectedResource>(datasets.size());

        Map<URI, List<SelectedResource>> resourcesByEndpoint = new HashMap<URI, List<SelectedResource>>();

        for (URI dataset : datasets) {
            long vol = getPatternCount(dataset, statementPattern);
            Set<URI> sources = getSource(dataset);

            for (URI source : sources) {
                SelectedResource selected = new SelectedResourceImpl(source, (int)vol, 0);
                selected.setLoadInfo(getLoadInfo(source));
                if (resourcesByEndpoint.containsKey(source)) {
                    resourcesByEndpoint.get(source).add(selected);
                } else {
                    List<SelectedResource> list = new ArrayList<SelectedResource>();
                    list.add(selected);
                    resourcesByEndpoint.put(source, list);
                }
            }
        }


        for (URI endpoint : resourcesByEndpoint.keySet()) {
            resources.add(getResource(resourcesByEndpoint.get(endpoint)));
        }

        return resources;
    }
}

