package org.semagrow.selector;

import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.semagrow.model.vocabulary.SEVOD;
import org.semagrow.model.vocabulary.VOID;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by angel on 6/3/14.
 */
public abstract class VOIDBase {

    private Repository voidRepository;

    protected VOIDBase(Repository voidRepository) {
        setRepository(voidRepository);
    }

    protected Repository getRepository() {
        return voidRepository;
    }

    protected void setRepository(Repository voidRepository) {
        this.voidRepository = voidRepository;
    }

    protected Set<Resource> getAllDatasets() {
        String q = "SELECT ?dataset { ?dataset <" + VOID.SPARQLENDPOINT + "> ?endpoint. }";
        QueryBindingSet bindings = new QueryBindingSet();
        return evalQuerySet(q, bindings, "dataset");
    }

    protected Set<Resource> getMatchingDatasetsOfPredicate(IRI pred) {
        String q = "SELECT ?dataset { ?dataset <" + VOID.PROPERTY + "> ?prop. \n" +
                          "?dataset <"+ VOID.TRIPLES + "> ?triples. FILTER (?triples > 0) .  }";
        //String q = "SELECT ?dataset { ?dataset ?p ?p1. }";
        QueryBindingSet bindings = new QueryBindingSet();
        bindings.addBinding("prop", pred);
        return evalQuerySet(q, bindings, "dataset");
    }

    protected Set<Resource> getMatchingDatasetsOfSubject(IRI subject) {
        //String q = "SELECT ?dataset { ?dataset <" + VOID.URIREGEXPATTERN + "> ?pattern . FILTER regex(str(?subject), str(?pattern)). }";
        String q = "SELECT ?dataset { ?dataset <" + SEVOD.SUBJECTREGEXPATTERN + "> ?pattern .\n" +
                           "?dataset <"+ VOID.TRIPLES + "> ?triples. FILTER (?triples > 0) . FILTER regex(str(?subject), str(?pattern)). }";
        QueryBindingSet bindings = new QueryBindingSet();
        bindings.addBinding("subject", subject);
        return evalQuerySet(q, bindings, "dataset");
    }

    protected Set<Resource> getMatchingDatasetsOfObject(IRI subject) {
        //String q = "SELECT ?dataset { ?dataset <" + VOID.URIREGEXPATTERN + "> ?pattern . FILTER regex(str(?subject), str(?pattern)). }";
        String q = "SELECT ?dataset { ?dataset <" + SEVOD.OBJECTREGEXPATTERN + "> ?pattern . "+
                "?dataset <"+ VOID.TRIPLES + "> ?triples. FILTER (?triples > 0) . FILTER regex(str(?subject), str(?pattern)). }";
        QueryBindingSet bindings = new QueryBindingSet();
        bindings.addBinding("subject", subject);
        return evalQuerySet(q, bindings, "dataset");
    }

    protected Set<Resource> getMatchingDatasetsOfEndpoint(IRI endpoint) {
        String q = "SELECT ?dataset { ?dataset <" + VOID.SPARQLENDPOINT + "> ?endpoint. }";
        QueryBindingSet bindings = new QueryBindingSet();
        bindings.addBinding("endpoint", endpoint);
        return evalQuerySet(q, bindings, "dataset");
    }

    protected Set<Resource> getMatchingDatasetsOfClass(IRI c) {
        String q = "SELECT ?dataset { ?dataset <" + VOID.CLASS + "> ?class. }";
        QueryBindingSet bindings = new QueryBindingSet();
        bindings.addBinding("class", c);
        return evalQuerySet(q, bindings, "dataset");
    }

    protected Set<Resource> evalQuerySet(String queryString, BindingSet bindingSet, String proj) {
        RepositoryConnection conn = null;
        try {
            conn = voidRepository.getConnection();
            TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
            q.setIncludeInferred(true);
            for (Binding b : bindingSet)
                q.setBinding(b.getName(), b.getValue());
            return createSet(q.evaluate(), proj);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null)
                try { conn.close(); } catch (Exception e) { }
        }
        return null;
    }

    protected IRI getEndpoint(Resource dataset) {
        String qStr = "SELECT ?endpoint { ?dataset <" + VOID.SPARQLENDPOINT + "> ?endpoint }";
        RepositoryConnection conn = null;
        try {
            conn = voidRepository.getConnection();
            TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, qStr);
            q.setIncludeInferred(true);
            q.setBinding("dataset", dataset);
            TupleQueryResult r = q.evaluate();
            if (!r.hasNext())
                return null;
            else
                return (IRI)r.next().getBinding("endpoint").getValue();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null)
                try { conn.close(); }catch(Exception e){ }
        }
        return null;
    }

    protected Set<IRI> getEndpoints() {
        String qStr = "SELECT DISTINCT ?endpoint { ?dataset <" + VOID.SPARQLENDPOINT + "> ?endpoint }";
        RepositoryConnection conn = null;

        Set<IRI> endpoints = new HashSet<IRI>();

        try {
            conn = voidRepository.getConnection();
            TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, qStr);
            //q.setIncludeInferred(true);
            //q.setBinding("dataset", dataset);
            TupleQueryResult r = q.evaluate();

            while (r.hasNext()) {
                IRI e = (IRI)r.next().getBinding("endpoint").getValue();
                if (e != null) {
                    endpoints.add(e);
                }
            }
            return endpoints;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null)
                try { conn.close(); }catch(Exception e){ }
        }
        return null;
    }

    protected Long getTriples(Resource dataset){
        String qStr = "SELECT ?triples { ?dataset <" + VOID.TRIPLES + "> ?triples }";
        RepositoryConnection conn = null;
        try {
            conn = voidRepository.getConnection();
            TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, qStr);
            q.setIncludeInferred(true);
            q.setBinding("dataset", dataset);
            TupleQueryResult r = q.evaluate();
            if (!r.hasNext())
                return null;
            else
                return Long.parseLong(r.next().getBinding("triples").getValue().stringValue());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null)
                try { conn.close(); }catch(Exception e){ }
        }
        return null;
    }

    protected Long getDistinctSubjects(Resource dataset){
        String qStr = "SELECT ?triples { ?dataset <" + VOID.DISTINCTSUBJECTS + "> ?triples }";
        RepositoryConnection conn = null;
        try {
            conn = voidRepository.getConnection();
            TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, qStr);
            q.setIncludeInferred(true);
            q.setBinding("dataset", dataset);
            TupleQueryResult r = q.evaluate();
            if (!r.hasNext())
                return null;
            else
                return Long.parseLong(r.next().getBinding("triples").getValue().stringValue());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null)
                try { conn.close(); }catch(Exception e){ }
        }
        return null;
    }

    protected Long getDistinctObjects(Resource dataset){
        String qStr = "SELECT ?triples { ?dataset <" + VOID.DISTINCTOBJECTS + "> ?triples }";
        RepositoryConnection conn = null;
        try {
            conn = voidRepository.getConnection();
            TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, qStr);
            q.setIncludeInferred(true);
            q.setBinding("dataset", dataset);
            TupleQueryResult r = q.evaluate();
            if (!r.hasNext())
                return null;
            else
                return Long.parseLong(r.next().getBinding("triples").getValue().stringValue());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null)
                try { conn.close(); }catch(Exception e){ }
        }
        return null;
    }

    protected Long getDistinctPredicates(Resource dataset){
        String qStr = "SELECT ?triples { ?dataset <" + VOID.PROPERTIES + "> ?triples }";
        RepositoryConnection conn = null;
        try {
            conn = voidRepository.getConnection();
            TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, qStr);
            q.setIncludeInferred(true);
            q.setBinding("dataset", dataset);
            TupleQueryResult r = q.evaluate();
            if (!r.hasNext())
                return null;
            else
                return Long.parseLong(r.next().getBinding("triples").getValue().stringValue());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null)
                try { conn.close(); }catch(Exception e){ }
        }
        return null;
    }

    protected Long getEntities(Resource dataset) {

        String qStr = "SELECT ?triples { ?dataset <" + VOID.ENTITIES + "> ?triples }";
        RepositoryConnection conn = null;
        try {
            conn = voidRepository.getConnection();
            TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, qStr);
            q.setIncludeInferred(true);
            q.setBinding("dataset", dataset);
            TupleQueryResult r = q.evaluate();
            if (!r.hasNext())
                return null;
            else
                return Long.parseLong(r.next().getBinding("triples").getValue().stringValue());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null)
                try { conn.close(); }catch(Exception e){ }
        }
        return null;
    }

    private Set<Resource> createSet(TupleQueryResult result, String binding) {
        Set<Resource> set = new HashSet<Resource>();

        if (result == null)
            return set;

        try {
            while (result.hasNext()) {
                Value v = result.next().getBinding(binding).getValue();
                if (v instanceof Resource) {
                    set.add((Resource) v);
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

        return set;
    }
}
