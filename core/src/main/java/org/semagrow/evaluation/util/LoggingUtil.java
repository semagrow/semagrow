package org.semagrow.evaluation.util;

import org.semagrow.plan.operators.SourceQuery;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;

import java.net.URL;

/**
 * Various static functions for logging specific messages.
 *
 * @author Antonis Troumpoukis
 */
public class LoggingUtil {

    public static void logSourceQuery(Logger logger, SourceQuery expr) {
        logger.debug("sq {} - Source query [{}] at source {}",
                Math.abs(expr.hashCode()),
                expr,
                //SPARQLQueryStringUtil.tupleExpr2Str(expr),
                expr.getSite());
    }

    public static void logFirstResult(Logger logger, String query, URL endpoint) {
        logger.debug("rq {} - Found first result.",
                Math.abs((query + endpoint).hashCode()));
    }

    public static void logResult(Logger logger, String query, URL endpoint, BindingSet bindingSet) {
        logger.debug("rq {} - Found {}",
                Math.abs((query + endpoint).hashCode()),
                bindingSet);
    }

    public static void logEnd(Logger logger, String query, URL endpoint, int results) {
        logger.debug("rq {} - Found {} results.",
                Math.abs((query + endpoint).hashCode()),
                results);
    }

    public static void logRemote(Logger logger, RepositoryConnection conn, String sparqlQuery, java.net.URL endpoint, TupleExpr expr, Query query) {
        logger.debug("rc {} - rq {} - sq {} - Sending to [{}] query [{}] with {}",
                conn.hashCode(),
                Math.abs((sparqlQuery+endpoint).hashCode()),
                Math.abs(expr.getParentNode().getParentNode().hashCode()),
                endpoint,
                sparqlQuery.replace('\n', ' '),
                query.getBindings());
    }
}
