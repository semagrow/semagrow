package eu.semagrow.core.impl.optimizer;

import eu.semagrow.core.impl.algebra.BindJoin;
import eu.semagrow.core.impl.algebra.SourceQuery;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.sparql.SPARQLParser;

import java.util.List;

/**
 * Created by antonis on 12/3/2015.
 */
public class StaticOptimizer {

    private URI ChEBI;
    private URI Dbpedia;
    private URI DrugBank;
    private URI GeoNames;
    private URI jamendo;
    private URI KEGG;
    private URI LinkedMDB;
    private URI NYT;

    private TupleExpr cd2;
    private TupleExpr cd3;
    private TupleExpr ls3;
    private TupleExpr ls4;
    private TupleExpr ls5;
    private TupleExpr ls6;
    private TupleExpr ls7;

    private String qcd2 = "SELECT ?party ?page WHERE {\n" +
    " <http://dbpedia.org/resource/Barack_Obama> <http://dbpedia.org/ontology/party> ?party .\n" +
            " ?x <http://data.nytimes.com/elements/topicPage> ?page .\n" +
            " ?x <http://www.w3.org/2002/07/owl#sameAs> <http://dbpedia.org/resource/Barack_Obama> .\n" +
            "}";

    private String qcd3 = "SELECT ?president ?party ?page WHERE {\n" +
    " ?president <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/President> .\n" +
            " ?president <http://dbpedia.org/ontology/nationality> <http://dbpedia.org/resource/United_States> .\n" +
            " ?president <http://dbpedia.org/ontology/party> ?party .\n" +
            " ?x <http://data.nytimes.com/elements/topicPage> ?page .\n" +
            " ?x <http://www.w3.org/2002/07/owl#sameAs> ?president .\n" +
            "}";

    private String qls3 = "SELECT ?Drug ?IntDrug ?IntEffect WHERE {\n" +
    " ?Drug <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Drug> .\n" +
            " ?y <http://www.w3.org/2002/07/owl#sameAs> ?Drug .\n" +
            " ?Int <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/interactionDrug1> ?y .\n" +
            " ?Int <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/interactionDrug2> ?IntDrug .\n" +
            " ?Int <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/text> ?IntEffect . \n" +
            "}";

    private String qls4 = "SELECT ?drugDesc ?cpd ?equation WHERE {\n" +
    " ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugCategory> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugcategory/cathartics> .\n" +
            " ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/keggCompoundId> ?cpd .\n" +
            " ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/description> ?drugDesc .\n" +
            " ?enzyme <http://bio2rdf.org/ns/kegg#xSubstrate> ?cpd .\n" +
            " ?enzyme <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://bio2rdf.org/ns/kegg#Enzyme> .\n" +
            " ?reaction <http://bio2rdf.org/ns/kegg#xEnzyme> ?enzyme .\n" +
            " ?reaction <http://bio2rdf.org/ns/kegg#equation> ?equation . \n" +
            "}";

    private String qls5 = "SELECT $drug $keggUrl $chebiImage WHERE {\n" +
    " $drug <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugs> .\n" +
            " $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/keggCompoundId> $keggDrug .\n" +
            " $keggDrug <http://bio2rdf.org/ns/bio2rdf#url> $keggUrl .\n" +
            " $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/genericName> $drugBankName .\n" +
            " $chebiDrug <http://purl.org/dc/elements/1.1/title> $drugBankName .\n" +
            " $chebiDrug <http://bio2rdf.org/ns/bio2rdf#image> $chebiImage .\n" +
            "} ";

    private String qls6 = "SELECT ?drug ?title WHERE { \n" +
    " ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugCategory> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugcategory/micronutrient> .\n" +
            " ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/casRegistryNumber> ?id .\n" +
            " ?keggDrug <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://bio2rdf.org/ns/kegg#Drug> .\n" +
            " ?keggDrug <http://bio2rdf.org/ns/bio2rdf#xRef> ?id .\n" +
            " ?keggDrug <http://purl.org/dc/elements/1.1/title> ?title .\n" +
            "}";

    private String qls7 = "SELECT $drug $transform $mass WHERE { \n" +
    " { $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/affectedOrganism> 'Humans and other mammals'.\n" +
            " $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/casRegistryNumber> $cas .\n" +
            " $keggDrug <http://bio2rdf.org/ns/bio2rdf#xRef> $cas .\n" +
            " $keggDrug <http://bio2rdf.org/ns/bio2rdf#mass> $mass\n" +
            " FILTER ( $mass > '5' )\n" +
            " } \n" +
            " OPTIONAL { $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/biotransformation> $transform . } \n" +
            "}";

    public StaticOptimizer() throws MalformedQueryException {
        ValueFactory valueFactory = ValueFactoryImpl.getInstance();
        SPARQLParser parser = new SPARQLParser();

        ChEBI = valueFactory.createURI("http://10.0.100.57:8890/sparql");
        Dbpedia = valueFactory.createURI("http://10.0.100.57:8891/sparql");
        DrugBank = valueFactory.createURI("http://10.0.100.57:8892/sparql");
        GeoNames = valueFactory.createURI("http://10.0.100.57:8893/sparql");
        jamendo = valueFactory.createURI("http://10.0.100.57:8894/sparql");
        KEGG = valueFactory.createURI("http://10.0.100.57:8895/sparql");
        LinkedMDB = valueFactory.createURI("http://10.0.100.57:8896/sparql");
        NYT = valueFactory.createURI("http://10.0.100.57:8897/sparql");

        cd2 = parser.parseQuery(qcd2, null).getTupleExpr();
        cd3 = parser.parseQuery(qcd3, null).getTupleExpr();
        ls3 = parser.parseQuery(qls3, null).getTupleExpr();
        ls4 = parser.parseQuery(qls4, null).getTupleExpr();
        ls5 = parser.parseQuery(qls5, null).getTupleExpr();
        ls6 = parser.parseQuery(qls6, null).getTupleExpr();
        ls7 = parser.parseQuery(qls7, null).getTupleExpr();
    }

    public TupleExpr decompose(TupleExpr q) throws MalformedQueryException {
        if (q.toString().compareTo(cd2.toString()) == 0)
            return cd2plan(q);
        if (q.toString().compareTo(cd3.toString()) == 0)
            return cd3plan(q);
        if (q.toString().compareTo(ls3.toString()) == 0)
            return ls3plan(q);
        if (q.toString().compareTo(ls4.toString()) == 0)
            return ls4plan(q);
        if (q.toString().compareTo(ls5.toString()) == 0)
            return ls5plan(q);
        if (q.toString().compareTo(ls6.toString()) == 0)
            return ls6plan(q);
        if (q.toString().compareTo(ls7.toString()) == 0)
            return ls7plan(q);

        return null;
    }

    private TupleExpr cd2plan(TupleExpr q) {
        List<StatementPattern> p = StatementPatternCollector.process(q);

        return new Projection(
                new BindJoin(
                        new SourceQuery(new Join(p.get(1), p.get(2)), NYT),
                        new SourceQuery(p.get(0), Dbpedia)
                ), ((Projection) q).getProjectionElemList());
    }

    private TupleExpr cd3plan(TupleExpr q) {
        List<StatementPattern> p = StatementPatternCollector.process(q);

        return new Projection(
                new BindJoin(
                        new BindJoin(
                                new SourceQuery(new Join(new Join(p.get(0),p.get(1)), p.get(2)), Dbpedia),
                                groupPatternCD(p.get(4))
                        ),
                        new SourceQuery(p.get(3), NYT)
                ), ((Projection) q).getProjectionElemList());
    }

    private TupleExpr ls3plan(TupleExpr q) {
        List<StatementPattern> p = StatementPatternCollector.process(q);

        return new Projection(
                new BindJoin(
                        new BindJoin(
                                new SourceQuery(p.get(0),Dbpedia),
                                new Union(new Union(new SourceQuery(p.get(1),Dbpedia), new SourceQuery(p.get(1),DrugBank)), new SourceQuery(p.get(1),KEGG))
                        ),
                        new SourceQuery(new Join(new Join(p.get(2),p.get(3)), p.get(4)), DrugBank)
                ), ((Projection) q).getProjectionElemList());
    }

    private TupleExpr ls4plan(TupleExpr q) {
        List<StatementPattern> p = StatementPatternCollector.process(q);

        return new Projection(
                new BindJoin(
                        new SourceQuery(new Join(new Join(p.get(0),p.get(1)), p.get(2)), DrugBank),
                        new SourceQuery(new Join(new Join(new Join(p.get(3),p.get(4)),p.get(5)), p.get(6)), KEGG)
                ), ((Projection) q).getProjectionElemList());
    }

    private TupleExpr ls5plan(TupleExpr q) {
        List<StatementPattern> p = StatementPatternCollector.process(q);

        return new Projection(
                new BindJoin(
                        new BindJoin(
                                new BindJoin(
                                        new SourceQuery(new Join(new Join(p.get(0),p.get(1)), p.get(3)), DrugBank),
                                        new Union(new SourceQuery(p.get(2),KEGG), new SourceQuery(p.get(2),ChEBI))
                                ),
                                new Union(new SourceQuery(p.get(4),KEGG), new SourceQuery(p.get(4),ChEBI))
                        ), new SourceQuery(p.get(5),ChEBI)
                ), ((Projection) q).getProjectionElemList());
    }

    private TupleExpr ls6plan(TupleExpr q) {
        List<StatementPattern> p = StatementPatternCollector.process(q);

        return new Projection(
                new BindJoin(
                        new BindJoin(
                                new BindJoin(
                                        new SourceQuery(new Join(p.get(0), p.get(1)), DrugBank),
                                        new SourceQuery(p.get(2),KEGG)
                                ),
                                new Union(new SourceQuery(p.get(3),KEGG), new SourceQuery(p.get(3),ChEBI))
                        ),
                        new Union(new SourceQuery(p.get(4),KEGG), new SourceQuery(p.get(4),ChEBI))
                ), ((Projection) q).getProjectionElemList());
    }

    private TupleExpr ls7plan(TupleExpr q) {
        List<StatementPattern> p = StatementPatternCollector.process(q);

        String str = "SELECT * where { $keggDrug <http://bio2rdf.org/ns/bio2rdf#mass> $mass FILTER ( $mass > '5' ) }";
        TupleExpr x = null;
        try {
            x = new SPARQLParser().parseQuery(str, null).getTupleExpr();
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        }

        return new Projection(
                new BindJoin(
                        new BindJoin(
                                new BindJoin(
                                        new SourceQuery(new Join(p.get(0), p.get(1)), DrugBank),
                                        new Union(new SourceQuery(p.get(2),KEGG), new SourceQuery(p.get(2),ChEBI))
                                ),
                                new SourceQuery(((Projection) x).getArg(),KEGG)
                        ),
                        new SourceQuery(p.get(4),DrugBank)
                ), ((Projection) q).getProjectionElemList());
    }

    private TupleExpr groupPatternCD(StatementPattern x) {
        return new Union(new Union(new Union(new Union(
                new SourceQuery(x,Dbpedia), new SourceQuery(x,GeoNames)), new SourceQuery(x,jamendo)), new SourceQuery(x,LinkedMDB)), new SourceQuery(x,NYT));
    }

}
