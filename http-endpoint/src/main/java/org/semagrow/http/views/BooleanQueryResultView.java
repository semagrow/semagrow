package org.semagrow.http.views;

import org.eclipse.rdf4j.http.server.repository.QueryResultView;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultWriter;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultWriterFactory;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 * Created by angel on 22/6/2016.
 */
public class BooleanQueryResultView extends QueryResultView {

    private static final BooleanQueryResultView INSTANCE = new BooleanQueryResultView();

    public static BooleanQueryResultView getInstance() {
        return INSTANCE;
    }

    private BooleanQueryResultView() {
    }

    public String getContentType() {
        return null;
    }

    protected void renderInternal(Map model, HttpServletRequest request, HttpServletResponse response) throws IOException {
        BooleanQueryResultWriterFactory brWriterFactory = (BooleanQueryResultWriterFactory)model.get("factory");
        BooleanQueryResultFormat brFormat = brWriterFactory.getBooleanQueryResultFormat();
        response.setStatus(200);
        this.setContentType(response, brFormat);
        this.setContentDisposition(model, response, brFormat);
        boolean headersOnly = ((Boolean)model.get("headersOnly")).booleanValue();
        if(!headersOnly) {
            ServletOutputStream out = response.getOutputStream();

            try {
                BooleanQueryResultWriter e = brWriterFactory.getWriter(out);
                BooleanQuery query = ((BooleanQuery)model.get("queryResult"));
                boolean value = query.evaluate();
                e.handleBoolean(value);
            } catch (QueryResultHandlerException var13) {
                if(var13.getCause() != null && var13.getCause() instanceof IOException) {
                    throw (IOException)var13.getCause();
                }

                throw new IOException(var13);
            } finally {
                out.close();
            }
        }

        this.logEndOfRequest(request);
    }
}
