package eu.semagrow.core.impl.evaluation.util;

import eu.semagrow.core.impl.plan.ops.SourceQuery;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Query;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.repository.RepositoryConnection;
import org.slf4j.Logger;

/**
 * Various static functions for logging specific messages.
 *
 * @author Antonis Troumpoukis
 */
public class LoggingUtil {

    public static void logSourceQuery(Logger logger, SourceQuery expr) {
        logger.info("sq {} - Source query [{}] at source {}",
                Math.abs(expr.hashCode()),
                SPARQLQueryStringUtil.tupleExpr2Str(expr),
                expr.getSources());
    }

    public static void logFirstResult(Logger logger, String query, URI endpoint) {
        logger.info("rq {} - Found first result.",
                Math.abs((query + endpoint).hashCode()));
    }

    public static void logResult(Logger logger, String query, URI endpoint, BindingSet bindingSet) {
        logger.debug("rq {} - Found {}",
                Math.abs((query + endpoint).hashCode()),
                bindingSet);
    }

    public static void logEnd(Logger logger, String query, URI endpoint, int results) {
        logger.info("rq {} - Found {} results.",
                Math.abs((query + endpoint).hashCode()),
                results);
    }

    public static void logRemote(Logger logger, RepositoryConnection conn, String sparqlQuery, URI endpoint, TupleExpr expr, Query query) {
        logger.info("rc {} - rq {} - sq {} - Sending to [{}] query [{}] with {}",
                conn.hashCode(),
                Math.abs((sparqlQuery+endpoint).hashCode()),
                Math.abs(expr.getParentNode().getParentNode().hashCode()),
                endpoint.stringValue(),
                sparqlQuery.replace('\n', ' '),
                query.getBindings());
    }
}
