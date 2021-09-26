package org.semagrow.geospatial.disjoint;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.semagrow.geospatial.helpers.WktHelpers;
import org.semagrow.geospatial.selector.BBoxBase;
import org.semagrow.model.vocabulary.SEVOD;
import org.semagrow.model.vocabulary.VOID;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeoDisjointBase {

    private BBoxBase base = new BBoxBase();

    private Repository metadata;
    private ValueFactory vf = SimpleValueFactory.getInstance();

    private Map<Pair<Resource,Resource>,Boolean> disjointSpatialMap = new HashMap<>();
    private Map<Pair<Resource,Resource>,Boolean> disjointThematicMap = new HashMap<>();

    public void setMetadata(Repository metadata) {
        this.metadata = metadata;
        base.setMetadata(metadata);
    }

    public boolean areGeoDisjoint(List<Resource> endpoints) {
        for (Resource endpoint1: endpoints) {
            for (Resource endpoint2: endpoints) {
                if (!endpoint1.stringValue().equals(endpoint2.stringValue())) {
                    if (!areDisjointSpatial(endpoint1, endpoint2)) {
                        return false;
                    }
                    if (!areDisjointThematic(endpoint1, endpoint2)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private boolean areDisjointSpatial(Resource endpoint1, Resource endpoint2) {
        Pair<Resource,Resource> pair = Pair.of(endpoint1,endpoint2);
        if (!disjointSpatialMap.containsKey(pair)) {
            boolean b = areDisjointSpatialInternal(endpoint1, endpoint2);
            disjointSpatialMap.put(pair, b);
            disjointSpatialMap.put(Pair.of(endpoint2, endpoint1), b);
        }
        return disjointSpatialMap.get(pair);
    }

    private boolean areDisjointSpatialInternal(Resource endpoint1, Resource endoiint2) {
        Literal wkt1 = base.getDatasetBoundingBox(endpoint1);
        Literal wkt2 = base.getDatasetBoundingBox(endoiint2);
        try {
            Geometry bbox1 = WktHelpers.createGeometry(wkt1, WktHelpers.getCRS(wkt1));
            Geometry bbox2 = WktHelpers.createGeometry(wkt2, WktHelpers.getCRS(wkt2));
            return bbox1.disjoint(bbox2);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean areDisjointThematic(Resource endpoint1, Resource endpoint2) {
        Pair<Resource,Resource> pair = Pair.of(endpoint1,endpoint2);
        if (!disjointThematicMap.containsKey(pair)) {
            boolean b = areDisjointThematicInternal(endpoint1, endpoint2);
            disjointThematicMap.put(pair, b);
            disjointThematicMap.put(Pair.of(endpoint2, endpoint1), b);
        }
        return disjointThematicMap.get(pair);
    }

    private boolean areDisjointThematicInternal(Resource endpoint1, Resource endpoint2) {

        Variable join = SparqlBuilder.var("join");
        Variable dataset1 = SparqlBuilder.var("dataset1");
        Variable dataset2 = SparqlBuilder.var("dataset2");
        Variable sel = SparqlBuilder.var("sel");
        Literal zero = vf.createLiteral("0", XMLSchema.INT);

        TriplePattern t1 = join.has(RDF.TYPE, SEVOD.JOIN);
        TriplePattern t2 = join.has(SEVOD.JOINS, dataset1);
        TriplePattern t3 = join.has(SEVOD.JOINS, dataset1);
        TriplePattern t4 = dataset1.has(RDF.TYPE, VOID.DATASET);
        TriplePattern t5 = dataset2.has(RDF.TYPE, VOID.DATASET);
        TriplePattern t6 = dataset1.has(VOID.SPARQLENDPOINT, endpoint1);
        TriplePattern t7 = dataset2.has(VOID.SPARQLENDPOINT, endpoint2);
        TriplePattern t8 = join.has(SEVOD.SELECTIVITY, sel);
        TriplePattern t9 = sel.has(SEVOD.SELECTIVITYVALUE, zero);

        GraphPattern body = GraphPatterns.and(t1,t2,t3,t4,t5,t6,t7,t8,t9);
        SelectQuery selectQuery = Queries.SELECT().where(body);

        return (runQuery("join", selectQuery.getQueryString()) != null);
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
