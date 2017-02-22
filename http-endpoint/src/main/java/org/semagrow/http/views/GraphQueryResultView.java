package org.semagrow.http.views;

import org.eclipse.rdf4j.http.server.repository.QueryResultView;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 * Created by angel on 22/6/2016.
 */
public class GraphQueryResultView extends QueryResultView {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final GraphQueryResultView INSTANCE = new GraphQueryResultView();

    public static GraphQueryResultView getInstance() {
        return INSTANCE;
    }

    private GraphQueryResultView() {
    }

    public String getContentType() {
        return null;
    }

    protected void renderInternal(Map model, HttpServletRequest request, HttpServletResponse response) throws IOException {
        RDFWriterFactory rdfWriterFactory = (RDFWriterFactory)model.get("factory");
        RDFFormat rdfFormat = rdfWriterFactory.getRDFFormat();
        response.setStatus(200);
        this.setContentType(response, rdfFormat);
        this.setContentDisposition(model, response, rdfFormat);
        boolean headersOnly = ((Boolean)model.get("headersOnly")).booleanValue();
        if(!headersOnly) {
            ServletOutputStream out = response.getOutputStream();

            try {
                RDFWriter e = rdfWriterFactory.getWriter(out);
                GraphQuery graphQuery = (GraphQuery)model.get("queryResult");
                graphQuery.evaluate(e);
            } catch (QueryInterruptedException var15) {
                this.logger.error("Query interrupted", var15);
                response.sendError(503, "Query evaluation took too long");
            } catch (QueryEvaluationException var16) {
                this.logger.error("Query evaluation error", var16);
                response.sendError(500, "Query evaluation error: " + var16.getMessage());
            } catch (RDFHandlerException var17) {
                this.logger.error("Serialization error", var17);
                response.sendError(500, "Serialization error: " + var17.getMessage());
            } finally {
                out.close();
            }
        }

        this.logEndOfRequest(request);
    }
}
