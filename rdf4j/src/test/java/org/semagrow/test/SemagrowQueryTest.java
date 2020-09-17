package org.semagrow.test;

import junit.framework.TestCase;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.semagrow.cli.CliMain;
import org.semagrow.query.SemagrowTupleQuery;
import org.semagrow.repository.impl.SemagrowSailRepository;
import org.semagrow.sail.SemagrowSail;
import org.semagrow.sail.config.SemagrowSailConfig;
import org.semagrow.sail.config.SemagrowSailFactory;

public class SemagrowQueryTest extends TestCase {

    private static final String REPOSITORY_TTL = "/etc/default/semagrow/repository.ttl";
    private static final String RESULTS_JSON = "/tmp/results.json";

    private static final String q1 = "";

    public static void main(String[] args) {
        decompose(q1);
        // execute(q1);
    }

    public static void decompose(String queryString) {
        try {
            SemagrowSailFactory factory = new SemagrowSailFactory();
            SemagrowSailConfig config = new SemagrowSailConfig();
            Repository repository = new SemagrowSailRepository((SemagrowSail) factory.getSail(config));
            repository.initialize();
            RepositoryConnection conn = repository.getConnection();
            TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
            TupleExpr plan = ((SemagrowTupleQuery) query).getDecomposedQuery();

        } catch (RepositoryConfigException e) {
            e.printStackTrace();
        } catch (RepositoryException e) {
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        } catch (TupleQueryResultHandlerException e) {
            e.printStackTrace();
        }
    }

    private static void execute(String queryString) {
        String[] argv = {REPOSITORY_TTL, queryString, RESULTS_JSON};
        CliMain.main(argv);
    }

}