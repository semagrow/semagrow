package org.semagrow;

import org.eclipse.rdf4j.common.iteration.Iterations;
import junit.framework.TestCase;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

/**
 * Created by antonis on 3/6/2015.
 */
public class UnionVsValuesTest extends TestCase {

    public void testUnionVsValues() throws Exception {
        /*
        // ChEBI dataset is needed

        String virtuosoEndpoint = "http://10.0.100.57:8890/sparql";
        String fourStoreEndpoint = "http://10.0.100.57:8990/sparql/";

        String valuesQuery = "SELECT * WHERE { \n" +
                "  ?keggDrug <http://bio2rdf.org/ns/bio2rdf#xRef> ?id .\n" +
                "} BINDINGS (?id) {\n" +
                "  (<http://bio2rdf.org/cas:54-47-7>)\n" +
                "  (<http://bio2rdf.org/cas:135-16-0>)\n" +
                "  (<http://bio2rdf.org/cas:71-00-1>)\n" +
                "  (<http://bio2rdf.org/cas:29908-03-0>)\n" +
                "  (<http://bio2rdf.org/cas:127-17-3>)\n" +
                "  (<http://bio2rdf.org/cas:63-91-2>)\n" +
                "  (<http://bio2rdf.org/cas:58-85-5>)\n" +
                "  (<http://bio2rdf.org/cas:62-49-7>)\n" +
                "  (<http://bio2rdf.org/cas:56-87-1>)\n" +
                "  (<http://bio2rdf.org/cas:74-79-3>)\n" +
                "  (<http://bio2rdf.org/cas:71-44-3>)\n" +
                "  (<http://bio2rdf.org/cas:56-84-8>)\n" +
                "  (<http://bio2rdf.org/cas:70-26-8>)\n" +
                "  (<http://bio2rdf.org/cas:56-85-9>)\n" +
                "  (<http://bio2rdf.org/cas:61-19-8>)\n" +
                "}";

        String unionQuery = "SELECT * WHERE {\n" +
                "  { ?keggDrug_1  <http://bio2rdf.org/ns/bio2rdf#xRef> <http://bio2rdf.org/cas:54-47-7>  } UNION\n" +
                "  { ?keggDrug_2  <http://bio2rdf.org/ns/bio2rdf#xRef> <http://bio2rdf.org/cas:135-16-0>   } UNION\n" +
                "  { ?keggDrug_3  <http://bio2rdf.org/ns/bio2rdf#xRef> <http://bio2rdf.org/cas:71-00-1>    } UNION\n" +
                "  { ?keggDrug_4  <http://bio2rdf.org/ns/bio2rdf#xRef> <http://bio2rdf.org/cas:29908-03-0> } UNION\n" +
                "  { ?keggDrug_5  <http://bio2rdf.org/ns/bio2rdf#xRef> <http://bio2rdf.org/cas:127-17-3>   } UNION\n" +
                "  { ?keggDrug_6  <http://bio2rdf.org/ns/bio2rdf#xRef> <http://bio2rdf.org/cas:63-91-2> } UNION\n" +
                "  { ?keggDrug_7  <http://bio2rdf.org/ns/bio2rdf#xRef> <http://bio2rdf.org/cas:58-85-5> } UNION\n" +
                "  { ?keggDrug_8  <http://bio2rdf.org/ns/bio2rdf#xRef> <http://bio2rdf.org/cas:62-49-7> } UNION\n" +
                "  { ?keggDrug_9  <http://bio2rdf.org/ns/bio2rdf#xRef> <http://bio2rdf.org/cas:56-87-1> } UNION\n" +
                "  { ?keggDrug_10 <http://bio2rdf.org/ns/bio2rdf#xRef> <http://bio2rdf.org/cas:74-79-3> } UNION\n" +
                "  { ?keggDrug_11 <http://bio2rdf.org/ns/bio2rdf#xRef> <http://bio2rdf.org/cas:71-44-3> } UNION\n" +
                "  { ?keggDrug_12 <http://bio2rdf.org/ns/bio2rdf#xRef> <http://bio2rdf.org/cas:56-84-8> } UNION\n" +
                "  { ?keggDrug_13 <http://bio2rdf.org/ns/bio2rdf#xRef> <http://bio2rdf.org/cas:70-26-8> } UNION\n" +
                "  { ?keggDrug_14 <http://bio2rdf.org/ns/bio2rdf#xRef> <http://bio2rdf.org/cas:56-85-9> } UNION\n" +
                "  { ?keggDrug_15 <http://bio2rdf.org/ns/bio2rdf#xRef> <http://bio2rdf.org/cas:61-19-8> }\n" +
                "}";

        long vv = runQuery(valuesQuery,virtuosoEndpoint);
        long uv = runQuery(unionQuery, virtuosoEndpoint);
        //long vf = runQuery(valuesQuery, fourStoreEndpoint);
        long vf = -1;
        long uf = runQuery(unionQuery, fourStoreEndpoint);


        System.out.println("Store\t\tValues\tUnion");
        System.out.println("Virtuoso\t" + vv + "\t\t" + uv);
        System.out.println("4store\t" + vf + "\t" + uf);
        */
    }

    /*
    private long runQuery(String sparqlQuery, String endpoint) throws Exception {

        Repository repo = new SPARQLRepository(endpoint);
        repo.initialize();
        RepositoryConnection conn = repo.getConnection();

        TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);

        long start = System.currentTimeMillis();

        TupleQueryResult result = query.evaluate();
        System.out.println(Iterations.toString(result, "\n"));
        Iterations.closeCloseable(result);

        long end = System.currentTimeMillis();

        conn.close();

        return (end - start);

    }*/
}
