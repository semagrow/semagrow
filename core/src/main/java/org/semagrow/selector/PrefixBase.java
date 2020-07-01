package org.semagrow.selector;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.sparqlbuilder.core.SparqlBuilder;
import org.eclipse.rdf4j.sparqlbuilder.core.Variable;
import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries;
import org.eclipse.rdf4j.sparqlbuilder.core.query.SelectQuery;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPattern;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPatterns;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.TriplePattern;
import org.semagrow.model.vocabulary.SEVOD;
import org.semagrow.model.vocabulary.VOID;

import java.util.*;

public class PrefixBase {

    private Repository metadata;

    public void setMetadata(Repository metadata) {
        this.metadata = metadata;
    }

    public Collection<String> getSubjectRegexPattern(StatementPattern pattern, Resource endpoint) {

        if (pattern.getPredicateVar().hasValue()) {

            IRI property = (IRI) pattern.getPredicateVar().getValue();

            if (property.equals(RDF.TYPE) && pattern.getObjectVar().hasValue()) {

                IRI clazz = (IRI) pattern.getObjectVar().getValue();

                Variable prefix = SparqlBuilder.var("prefix");
                Variable d = SparqlBuilder.var("d");
                Variable p = SparqlBuilder.var("p");

                TriplePattern t1 = d.has(RDF.TYPE, VOID.DATASET);
                TriplePattern t2 = d.has(VOID.SPARQLENDPOINT, endpoint);
                TriplePattern t3 = d.has(VOID.CLASSPARTITION, p);
                TriplePattern t4 = p.has(VOID.CLASS, clazz);
                TriplePattern t5 = p.has(SEVOD.SUBJECTREGEXPATTERN, prefix);

                GraphPattern body = GraphPatterns.and(t1,t2,t3,t4,t5);

                SelectQuery selectQuery = Queries.SELECT().select(prefix).where(body);

                Collection<String> result = runQuery(selectQuery.getQueryString());

                if (!result.isEmpty()) {
                    return result;
                }
            }

            Variable prefix = SparqlBuilder.var("prefix");
            Variable d = SparqlBuilder.var("d");
            Variable p = SparqlBuilder.var("p");

            TriplePattern t1 = d.has(RDF.TYPE, VOID.DATASET);
            TriplePattern t2 = d.has(VOID.SPARQLENDPOINT, endpoint);
            TriplePattern t3 = d.has(VOID.PROPERTYPARTITION, p);
            TriplePattern t4 = p.has(VOID.PROPERTY, property);
            TriplePattern t5 = p.has(SEVOD.SUBJECTREGEXPATTERN, prefix);

            GraphPattern body = GraphPatterns.and(t1,t2,t3,t4,t5);

            SelectQuery selectQuery = Queries.SELECT().select(prefix).where(body);

            Collection<String> result = runQuery(selectQuery.getQueryString());

            if (!result.isEmpty()) {
                return result;
            }
        }
        return Collections.singletonList("ΑΝΥ");
    }

    public Collection<String> getObjectRegexPattern(StatementPattern pattern, Resource endpoint) {

        if (pattern.getPredicateVar().hasValue()) {

            IRI property = (IRI) pattern.getPredicateVar().getValue();

            Variable prefix = SparqlBuilder.var("prefix");
            Variable d = SparqlBuilder.var("d");
            Variable p = SparqlBuilder.var("p");

            TriplePattern t1 = d.has(RDF.TYPE, VOID.DATASET);
            TriplePattern t2 = d.has(VOID.SPARQLENDPOINT, endpoint);
            TriplePattern t3 = d.has(VOID.PROPERTYPARTITION, p);
            TriplePattern t4 = p.has(VOID.PROPERTY, property);
            TriplePattern t5 = p.has(SEVOD.OBJECTREGEXPATTERN, prefix);

            GraphPattern body = GraphPatterns.and(t1,t2,t3,t4,t5);

            SelectQuery selectQuery = Queries.SELECT().select(prefix).where(body);

            Collection<String> result = runQuery(selectQuery.getQueryString());

            if (!result.isEmpty()) {
                return result;
            }
        }
        return Collections.singletonList("ΑΝΥ");
    }

    private Collection<String> runQuery(String qStr){
        RepositoryConnection conn = null;
        List<String> result = new ArrayList<>();
        try {
            conn = metadata.getConnection();
            TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, qStr);
            TupleQueryResult r = q.evaluate();
            while (r.hasNext()) {
                result.add(r.next().getBinding("prefix").getValue().stringValue());
            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null)
                try { conn.close(); } catch (Exception e){ }
        }
        return Collections.emptyList();
    }
}
