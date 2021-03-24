package org.semagrow.geospatial.selector;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.sparqlbuilder.core.SparqlBuilder;
import org.eclipse.rdf4j.sparqlbuilder.core.Variable;
import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries;
import org.eclipse.rdf4j.sparqlbuilder.core.query.SelectQuery;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPattern;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPatterns;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.TriplePattern;
import org.semagrow.geospatial.helpers.WktHelpers;
import org.semagrow.geospatial.vocabulary.SEVOD_GEO;
import org.semagrow.model.vocabulary.VOID;

public class BBoxBase {

    private Repository metadata;

    public void setMetadata(Repository metadata) {
        this.metadata = metadata;
    }

    public Literal getDatasetBoundingBox(Resource endpoint) {

        Variable dataset = SparqlBuilder.var("dataset");
        Variable mbb = SparqlBuilder.var("mbb");

        TriplePattern t1 = dataset.has(RDF.TYPE, VOID.DATASET);
        TriplePattern t2 = dataset.has(VOID.SPARQLENDPOINT, endpoint);
        TriplePattern t3 = dataset.has(SEVOD_GEO.BOUNDING_POLYGON, mbb);

        GraphPattern body = GraphPatterns.and(t1,t2,t3);
        SelectQuery selectQuery = Queries.SELECT().select(mbb).where(body);

        Value wktLiteral = runQuery("mbb", selectQuery.getQueryString());

        if (wktLiteral != null) {
            return (Literal) wktLiteral;
        }
        return WktHelpers.infMBBoxLiteral();
    }

    private Value runQuery(String varName, String qStr){
        RepositoryConnection conn = null;
        try {
            conn = metadata.getConnection();
            TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, qStr);
            TupleQueryResult r = q.evaluate();
            while (r.hasNext()) {
                return r.next().getBinding(varName).getValue();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null)
                try { conn.close(); } catch (Exception e){ }
        }
        return null;
    }
}
