package org.semagrow.geospatial.disjoint;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.GEOF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.repository.Repository;
import org.semagrow.art.LogUtils;
import org.semagrow.connector.sparql.SPARQLSite;
import org.semagrow.plan.Plan;
import org.semagrow.plan.QueryCompiler;
import org.semagrow.plan.operators.SourceQuery;
import org.semagrow.plan.util.FilterCollector;
import org.semagrow.selector.QueryAwareSourceSelector;
import org.semagrow.selector.Site;
import org.semagrow.selector.SourceMetadata;
import org.semagrow.selector.SourceSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

public class GeoDisjointCompiler implements QueryCompiler {

    protected final Logger logger = LoggerFactory.getLogger(GeoDisjointCompiler.class);

    GeoDisjointBase base = new GeoDisjointBase();
    QueryCompiler innerQueryCompiler;
    SourceSelector sourceSelector;

    private static final Set<String> nonDisjointStRelations;

    static {
        nonDisjointStRelations = new HashSet<>();

        nonDisjointStRelations.add(GEOF.SF_EQUALS.stringValue());
        nonDisjointStRelations.add(GEOF.SF_INTERSECTS.stringValue());
        nonDisjointStRelations.add(GEOF.SF_TOUCHES.stringValue());
        nonDisjointStRelations.add(GEOF.SF_WITHIN.stringValue());
        nonDisjointStRelations.add(GEOF.SF_CONTAINS.stringValue());
        nonDisjointStRelations.add(GEOF.SF_OVERLAPS.stringValue());
        nonDisjointStRelations.add(GEOF.SF_CROSSES.stringValue());
    }

    public GeoDisjointCompiler(QueryCompiler innerQueryCompiler, SourceSelector sourceSelector, Repository metadata) {
        this.innerQueryCompiler = innerQueryCompiler;
        this.sourceSelector = sourceSelector;
        base.setMetadata(metadata);
    }

    @Override
    public Plan compile(QueryRoot query, Dataset dataset, BindingSet bindings) {

        long t1 = System.currentTimeMillis();

        sourceSelection(query);

        long t2 = System.currentTimeMillis();

        logger.info("Source Selection Time: " + (t2-t1));

        List<Resource> sources = getSources(query);

        if (base.areGeoDisjoint(sources) && canOptimize(query)) {

            Plan plan = new Plan(unionize(query.getArg(), sources));
            logger.info("Forward query {} in {}", query.getArg(), sources);

            long t3 = System.currentTimeMillis();

            String compilationReport = "" +
                    "Source Selection Time: " + (t2-t1) + " - " +
                    "Compile Time: " + (t3-t2) + " - " +
                    "Sources: " + sources.size();
            LogUtils.appendKobeReport(compilationReport);

            logger.info(compilationReport);
            return plan;
        }
        else {
            logger.info("Fallback to SimpleQueryCompiler");
            return innerQueryCompiler.compile(query, dataset, bindings);
        }
    }

    public boolean canOptimize(QueryRoot query) {
        Collection<StatementPattern> patterns = StatementPatternCollector.process(query);
        Collection<ValueExpr> filters = FilterCollector.process(query);

        for (StatementPattern p1: patterns) {
            for (StatementPattern p2: patterns) {
                if (!p1.equals(p2)) {
                    if (!p1.getObjectVar().hasValue() && !p2.getObjectVar().hasValue() &&
                            p1.getObjectVar().getName().equals(p2.getObjectVar().getName())) {
                        return false;
                    }
                }
            }
        }

        for (ValueExpr f: filters) {
            if (!(f instanceof FunctionCall)) {
                return false;
            }
            else {
                FunctionCall fc = (FunctionCall) f;
                if (!nonDisjointStRelations.contains(fc.getURI())) {
                    return false;
                }
            }
        }

        return true;
    }

    private TupleExpr unionize(TupleExpr query, List<Resource> endpoints) {
        if (endpoints.isEmpty()) {
            return new EmptySet();
        }
        else {
            TupleExpr sq = newSourceQuery(query, endpoints.get(0));

            if (endpoints.size() == 1) {
                return sq;
            }
            else {
                List<Resource> rest = endpoints.subList(1, endpoints.size());
                return new Union(unionize(query, rest), sq);
            }
        }
    }

    private TupleExpr newSourceQuery(TupleExpr query, Resource endpoint) {
        try {
            Site site = new SPARQLSite(new URL(endpoint.stringValue()));
            return new SourceQuery(query, site);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return new EmptySet();
    }

    private void sourceSelection(QueryRoot query) {
        if (sourceSelector instanceof QueryAwareSourceSelector) {
            ((QueryAwareSourceSelector) sourceSelector).processTupleExpr(query);
        }
    }

    private List<Resource> getSources(TupleExpr query) {
        Set<Resource> sources = new HashSet<>();
        Collection<StatementPattern> patterns = StatementPatternCollector.process(query);

        for (StatementPattern pattern: patterns) {
            for (SourceMetadata m: sourceSelector.getSources(pattern, null, null)) {
                sources.add(endpointOfSource(m));
            }
        }
        return new ArrayList<>(sources);
    }

    private Resource endpointOfSource(SourceMetadata source) {
        return source.getSites().iterator().next().getID();
    }
}
