package org.semagrow.http.views;

import org.eclipse.rdf4j.http.server.repository.QueryResultView;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.resultio.BasicQueryWriterSettings;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriter;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by angel on 22/6/2016.
 */
public class TupleQueryResultView extends QueryResultView {


    private Logger logger = LoggerFactory.getLogger(this.getClass());
    protected static final String DEFAULT_JSONP_CALLBACK_PARAMETER = "callback";
    protected static final Pattern JSONP_VALIDATOR = Pattern.compile("^[A-Za-z]\\w+$");
    private static final TupleQueryResultView INSTANCE = new TupleQueryResultView();

    public static TupleQueryResultView getInstance() {
        return INSTANCE;
    }

    private TupleQueryResultView() { }

    public String getContentType() {
        return null;
    }

    protected void renderInternal(Map model, HttpServletRequest request, HttpServletResponse response) throws IOException {
        TupleQueryResultWriterFactory qrWriterFactory = (TupleQueryResultWriterFactory)model.get("factory");
        TupleQueryResultFormat qrFormat = qrWriterFactory.getTupleQueryResultFormat();
        response.setStatus(200);
        this.setContentType(response, qrFormat);
        this.setContentDisposition(model, response, qrFormat);
        Boolean headersOnly = (Boolean)model.get("headersOnly");
        if(headersOnly == null || !headersOnly.booleanValue()) {
            ServletOutputStream out = response.getOutputStream();

            try {
                TupleQueryResultWriter e = qrWriterFactory.getWriter(out);
                TupleQuery tupleQuery = (TupleQuery)model.get("queryResult");
                if(e.getSupportedSettings().contains(BasicQueryWriterSettings.JSONP_CALLBACK)) {
                    String parameter = request.getParameter("callback");
                    if(parameter != null) {
                        parameter = parameter.trim();
                        if(parameter.isEmpty()) {
                            parameter = (String)BasicQueryWriterSettings.JSONP_CALLBACK.getDefaultValue();
                        }

                        if(!JSONP_VALIDATOR.matcher(parameter).matches()) {
                            throw new IOException("Callback function name was invalid");
                        }

                        e.getWriterConfig().set(BasicQueryWriterSettings.JSONP_CALLBACK, parameter);
                    }
                }

                tupleQuery.evaluate(e);
            } catch (QueryInterruptedException var16) {
                this.logger.error("Query interrupted", var16);
                response.sendError(503, "Query evaluation took too long");
            } catch (QueryEvaluationException var17) {
                this.logger.error("Query evaluation error", var17);
                response.sendError(500, "Query evaluation error: " + var17.getMessage());
            } catch (TupleQueryResultHandlerException var18) {
                this.logger.error("Serialization error", var18);
                response.sendError(500, "Serialization error: " + var18.getMessage());
            } catch (Exception e) {
                this.logger.error("Unknown internal error", e);
                response.sendError(500, "Internal error: " + e.getMessage());
            } finally {
                out.close();
            }
        }

        this.logEndOfRequest(request);
    }
}
