package org.semagrow.http.controllers;


import org.eclipse.rdf4j.common.lang.FileFormat;
import org.eclipse.rdf4j.common.lang.service.FileFormatServiceRegistry;
import org.eclipse.rdf4j.http.server.ClientHTTPException;
import org.eclipse.rdf4j.http.server.HTTPException;
import org.eclipse.rdf4j.http.server.ProtocolUtil;
import org.eclipse.rdf4j.http.server.ServerHTTPException;
import org.eclipse.rdf4j.http.server.repository.QueryResultView;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.resultio.*;
import org.eclipse.rdf4j.rio.RDFWriterRegistry;
import org.semagrow.http.views.GraphQueryResultView;
import org.semagrow.http.views.TupleQueryResultView;
import org.semagrow.http.views.BooleanQueryResultView;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.View;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;

import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_SERVICE_UNAVAILABLE;

/**
 * Created by angel on 17/6/2016.
 */
public class QueryController extends AbstractQueryController {

    public QueryController() {
        super();
    }

    protected ModelAndView handleQuery(Query query, boolean headersOnly, HttpServletRequest request, HttpServletResponse response)
            throws HTTPException
    {
        View view;
        Object queryResult;
        FileFormatServiceRegistry<? extends FileFormat, ?> registry;

        try {
            if (query instanceof TupleQuery) {
                TupleQuery tQuery = (TupleQuery)query;

                //FIXME: should use evaluate(handler)
                queryResult = headersOnly ? null : tQuery;
                registry = TupleQueryResultWriterRegistry.getInstance();
                view = TupleQueryResultView.getInstance();
            }
            else if (query instanceof GraphQuery) {
                GraphQuery gQuery = (GraphQuery)query;

                //FIXME: should use evaluate(handler)
                queryResult = headersOnly ? null : gQuery;
                registry = RDFWriterRegistry.getInstance();
                view = GraphQueryResultView.getInstance();
            }
            else if (query instanceof BooleanQuery) {
                BooleanQuery bQuery = (BooleanQuery)query;

                queryResult = headersOnly ? null : bQuery;
                registry = BooleanQueryResultWriterRegistry.getInstance();
                view = BooleanQueryResultView.getInstance();
            }
            else {
                throw new ClientHTTPException(SC_BAD_REQUEST, "Unsupported query type: "
                        + query.getClass().getName());
            }
        }
        catch (QueryInterruptedException e) {
            logger.info("Query interrupted", e);
            throw new ServerHTTPException(SC_SERVICE_UNAVAILABLE, "Query evaluation took too long");
        }
        catch (QueryEvaluationException e) {
            logger.info("Query evaluation error", e);
            if (e.getCause() != null && e.getCause() instanceof HTTPException) {
                // custom signal from the backend, throw as HTTPException
                // directly (see SES-1016).
                throw (HTTPException)e.getCause();
            }
            else {
                throw new ServerHTTPException("Query evaluation error: " + e.getMessage());
            }
        }
        Object factory = ProtocolUtil.getAcceptableService(request, response, registry);

        Map<String, Object> model = new HashMap<String, Object>();
        model.put(QueryResultView.FILENAME_HINT_KEY, "query-result");
        model.put(QueryResultView.QUERY_RESULT_KEY, queryResult);
        model.put(QueryResultView.FACTORY_KEY, factory);
        model.put(QueryResultView.HEADERS_ONLY, headersOnly);

        return new ModelAndView(view, model);
    }


}
