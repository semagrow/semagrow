package org.semagrow.http.controllers;

import org.eclipse.rdf4j.http.server.ClientHTTPException;
import org.eclipse.rdf4j.http.server.HTTPException;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.semagrow.http.views.ExplainView;
import org.semagrow.query.SemagrowTupleQuery;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;

import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;

/**
 * Created by angel on 17/6/2016.
 */
public class ExplainQueryController extends AbstractQueryController {

    @Override
    protected ModelAndView handleQuery(Query query, boolean headersOnly, HttpServletRequest request, HttpServletResponse response)
            throws HTTPException
    {
        if (query instanceof SemagrowTupleQuery) {
            SemagrowTupleQuery stQuery = (SemagrowTupleQuery) query;
            TupleExpr decomposedQuery = stQuery.getDecomposedQuery();
            Map<String, Object> model = new HashMap<String, Object>();
            model.put(ExplainView.DECOMPOSED,decomposedQuery);
            model.put(ExplainView.HEADERS_ONLY, headersOnly);
            return new ModelAndView(ExplainView.getInstance(), model);
        } else
            throw new ClientHTTPException(SC_BAD_REQUEST, "Unsupported query type: "
                    + query.getClass().getName());

    }
}
