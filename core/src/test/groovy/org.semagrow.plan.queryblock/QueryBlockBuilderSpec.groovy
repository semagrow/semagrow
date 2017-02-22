package org.semagrow.plan.queryblock

import org.eclipse.rdf4j.model.IRI
import org.eclipse.rdf4j.query.algebra.StatementPattern
import org.semagrow.estimator.CardinalityEstimator
import org.semagrow.estimator.CardinalityEstimatorResolver
import org.semagrow.estimator.CostEstimator
import org.semagrow.estimator.CostEstimatorResolver
import org.semagrow.estimator.SimpleCardinalityEstimatorResolver
import org.semagrow.estimator.SimpleCostEstimatorResolver
import org.semagrow.local.LocalSite
import org.semagrow.plan.Cost
import org.semagrow.plan.DefaultCompilerContext
import org.semagrow.plan.InterestingPropertiesVisitor
import org.semagrow.selector.Site
import org.semagrow.selector.SourceMetadata
import org.semagrow.selector.SourceSelector
import spock.lang.Specification;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParserFactory;

/**
 * Created by angel on 12/9/2016.
 */
class QueryBlockBuilderSpec extends Specification {

    def parse(queryStr) {
        def factory = new SPARQLParserFactory()
        def parser  = factory.getParser()
        def parsedQuery = parser.parseQuery(queryStr, "http://test")
        parsedQuery.getTupleExpr()
    }

    /*
    def "single pattern block" () {
        setup :
            def visitor = new SelectMergeVisitor()
        when :
            //def queryStr = "SELECT ?s { ?s ?p ?o . ?s ?p2 ?o2 . ?s ?p3 ?o3 . } GROUP BY ?s ORDER BY ASC(?s)"
            def queryStr = "SELECT ?s1 ?c { {SELECT ((?s + ?o) AS ?c) { ?s ?p ?o }} . ?s1 ?p1 ?c }"
            def block = QueryBlockBuilder.build(parse(queryStr))
            block.visit(visitor);
        then :
            block instanceof SelectBlock
            ((SelectBlock)block).getOutputVariables().size() == 2
            ((SelectBlock)block).getOutputVariables().containsAll(Arrays.asList("s1", "c"))
    }


    def "simple plan" () {
        setup :
            def visitor = new SelectMergeVisitor()
        when :
            //def queryStr = "SELECT ?s { ?s ?p ?o . ?s ?p2 ?o2 . ?s ?p3 ?o3 . } GROUP BY ?s ORDER BY ASC(?s)"
            //def queryStr = "SELECT ?s ?c { ?s ?p1 ?c . ?s ?p2 <http://test2> . { SELECT (?s1 AS ?s) ?o { ?s1 <http://test> ?o . } } FILTER (regex(?o,?p1)) } ORDER BY ?s"
            def queryStr ="SELECT ?film ?director ?genre WHERE {\n" +
                "   ?film <http://dbpedia.org/ontology/director>  ?director .\n" +
                "   ?director <http://dbpedia.org/ontology/nationality> <http://dbpedia.org/resource/Italy> .\n" +
                "   ?x <http://www.w3.org/2002/07/owl#sameAs> ?film .\n" +
                "   ?x <http://data.linkedmdb.org/resource/movie/genre> ?genre .\n" +
                "}"
            def block = QueryBlockBuilder.build(parse(queryStr))
            block.visit(visitor);
            block.visit(new InterestingPropertiesVisitor());
            def context = new DefaultCompilerContext();
            def cardResolve = Mock(CardinalityEstimatorResolver);
            def cardEstim = Mock(CardinalityEstimator);
            cardResolve.resolve(_) >> Optional.of(cardEstim);
            cardEstim.getCardinality(_) >> 100
            def costResolve = Mock(CostEstimatorResolver)
            def costEstim = Mock(CostEstimator)
            def sourceSelector = Mock(SourceSelector)
            costEstim.getCost(_) >> new Cost(200)
            costResolve.resolve(_) >> Optional.of(costEstim)
            sourceSelector.getSources(_,_,_) >> { StatementPattern p, x, y -> Collections.singleton(new SourceMetadata() {
                @Override
                Collection<Site> getSites() {
                    return Collections.singleton(LocalSite.getInstance())
                }

                @Override
                StatementPattern original() {
                    return p
                }

                @Override
                StatementPattern target() {
                    return p
                }

                @Override
                Collection<IRI> getSchema(String var) {
                    return null
                }

                @Override
                boolean isTransformed() {
                    return false
                }

                @Override
                double getSemanticProximity() {
                    return 0
                }
            }) }
            context.setCardinalityEstimatorResolver(cardResolve)
            context.setCostEstimatorResolver(costResolve)
            context.setSourceSelector(sourceSelector)
            def plans = block.getPlans(context)
        then :
            !plans.isEmpty();
    }
    */
}
