package org.semagrow.plan.rel;

import com.google.common.collect.ImmutableList;
import junit.framework.TestCase;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Test;
import org.semagrow.plan.rel.rules.StatementScanRule;
import org.semagrow.plan.rel.semagrow.SemagrowConvention;
import org.semagrow.plan.rel.semagrow.SemagrowRel;
import org.semagrow.plan.rel.semagrow.SemagrowRules;
import org.semagrow.plan.rel.type.RdfDataTypeFactoryImpl;

/**
 * Created by antonis on 6/7/2017.
 */
public class TransformTest extends TestCase {

    private String q1 = "" +
            "SELECT DISTINCT ?party ?page  WHERE {\n" +
            "   <http://dbpedia.org/resource/Barack_Obama> <http://dbpedia.org/ontology/party> ?party .\n" +
            "   ?x <http://data.nytimes.com/elements/topicPage> ?page .\n" +
            "   ?x <http://www.w3.org/2002/07/owl#sameAs> <http://dbpedia.org/resource/Barack_Obama> .\n" +
            "}";

    private String q2 = "" +
            "SELECT ?predicate ?object WHERE {\n" +
            "   { <http://dbpedia.org/resource/Barack_Obama> ?predicate ?object }\n" +
            "   UNION  {  \n" +
            "      ?subject <http://www.w3.org/2002/07/owl#sameAs> <http://dbpedia.org/resource/Barack_Obama> .\n" +
            "     ?subject ?predicate ?object } \n" +
            "}";

    private String q3 = "" +
            "SELECT $drug $transform $mass WHERE {  \n" +
            "  { $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/affectedOrganism>  'Humans and other mammals'.\n" +
            "    $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/casRegistryNumber> $cas .\n" +
            "    $keggDrug <http://bio2rdf.org/ns/bio2rdf#xRef> $cas .\n" +
            "    $keggDrug <http://bio2rdf.org/ns/bio2rdf#mass> $mass\n" +
            "    FILTER ( $mass > 5 )\n" +
            "  } \n" +
            "  OPTIONAL { $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/biotransformation> $transform . " +
            " FILTER ($transform > 4)" +
            "} \n" +
            "}";


    private String q4 = "" +
            "SELECT $drug $cas (COUNT($drug) AS ?c) WHERE   \n" +
            "  { $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/affectedOrganism>  'Humans and other mammals'.\n" +
            "    $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/casRegistryNumber> $cas .\n" +
            "    $keggDrug <http://bio2rdf.org/ns/bio2rdf#xRef> $cas .\n" +
            "    $keggDrug <http://bio2rdf.org/ns/bio2rdf#mass> $mass\n" +
            "    FILTER ( $mass > '5' )\n" +
            "  } \n"+
            "  GROUP BY $drug $cas \n" +
            "";


    private String q5 = "" +
            "SELECT $drug WHERE   \n" +
            "  { $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/affectedOrganism>  'Humans and other mammals'.\n" +
            "    $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/casRegistryNumber> $cas .\n" +
            "    $keggDrug <http://bio2rdf.org/ns/bio2rdf#xRef> $cas .\n" +
            "    $keggDrug <http://bio2rdf.org/ns/bio2rdf#mass> $mass\n" +
            "    FILTER ( $mass > '5' || $mass < 2)." +
            "    FILTER EXISTS { SELECT $cas1 { $drug1 <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/casRegistryNumber> $cas1 ." +
            "                     FILTER ($drug = $drug1) }}\n" +
            "  } \n";

    private String q6 = "" +
            "PREFIX movie: <http://data.linkedmdb.org/resource/movie/>\n" +
            "PREFIX dc: <http://purl.org/dc/terms/>\n" +
            "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \n" +
            "SELECT ?a " +
            "(?title as ?titel) \n" +
            " (xsd:float(?runtimeStr) as ?runtime)\n" +
            //" (xsd:int(substr(?dateStr,1,4)) as ?year) \n" +
            "WHERE {\n" +
            " ?a a movie:film.\n" +
            " ?a movie:runtime ?runtimeStr.\n" +
            " ?a dc:title ?title.\n" +
            " ?a dc:date ?dateStr.\n" +
            "}";

    @Test
    public void testTransformSimple() {

        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        RelOptCluster cluster = RelOptCluster.create(
                planner,
                new RdfRexBuilder(new RdfDataTypeFactoryImpl(RelDataTypeSystem.DEFAULT))
        );
        TupleExprToRelConverter tupleExprToRelConverter = new TupleExprToRelConverter(cluster, CatalogReader.INSTANCE);
        RelToTupleExprConverter relToTupleExprConverter = new RelToTupleExprConverter();
        SPARQLParser parser = new SPARQLParser();

        String queries[] = {
                //q1
                q2,
                //q4,
                //q6
        };

        for (String queryString: queries) {
            TupleExpr expr = parser.parseQuery(queryString, null).getTupleExpr();
            QueryRoot sparqlroot = new QueryRoot(expr);
            RelRoot relroot = tupleExprToRelConverter.convertQuery(sparqlroot);
            RelNode rel = relroot.rel;

            HepProgram hepProgram = HepProgram.builder()
                    .addRuleInstance(SubQueryRemoveRule.FILTER)
                    .addRuleInstance(SubQueryRemoveRule.JOIN)
                    .addRuleInstance(SubQueryRemoveRule.PROJECT)
                    .addRuleInstance(ProjectMergeRule.INSTANCE)
                    .addRuleInstance(StatementScanRule.INSTANCE)
                    .build();

            Program p  = Programs.of(hepProgram, false, null);

            rel = p.run(cluster.getPlanner(), rel, null, ImmutableList.of(), ImmutableList.of());
            System.out.println(RelOptUtil.dumpPlan("", rel, false, SqlExplainLevel.ALL_ATTRIBUTES));

            //p = Programs.ofRules(FilterJoinRule.FILTER_ON_JOIN);
            p = Programs.ofRules(SemagrowRules.RULES);
            rel = p.run(cluster.getPlanner(),
                    rel,
                    cluster.traitSet().replace(SemagrowConvention.INSTANCE),
                    ImmutableList.of(),
                    ImmutableList.of());

            System.out.println(sparqlroot);
            System.out.println(RelOptUtil.dumpPlan("", rel, false, SqlExplainLevel.ALL_ATTRIBUTES));

            //TupleExpr sparqlnew = relToTupleExprConverter.convertQuery(RelRoot.of(rel, SqlKind.SELECT));
            //System.out.println(sparqlnew);
        }
    }
}
