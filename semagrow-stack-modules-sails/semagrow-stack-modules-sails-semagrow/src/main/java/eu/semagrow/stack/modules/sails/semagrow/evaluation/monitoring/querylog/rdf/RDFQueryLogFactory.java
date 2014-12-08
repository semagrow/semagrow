package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog.rdf;

import eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog.QueryLogException;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog.QueryLogFactory;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog.QueryLogWriter;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;

import java.io.OutputStream;

/**
 * Created by angel on 10/21/14.
 */
public class RDFQueryLogFactory implements QueryLogFactory {

    private RDFWriterFactory writerFactory;

    public RDFQueryLogFactory(RDFWriterFactory writerFactory) {
        this.writerFactory = writerFactory;
    }

    @Override
    public QueryLogWriter getQueryLogger(OutputStream out) {

        RDFWriter writer = writerFactory.getWriter(out);

        QueryLogWriter handler = new RDFQueryLogWriter(writer);
        try {
            handler.startQueryLog();
        } catch (QueryLogException e) {
            e.printStackTrace();
        }
        return handler;
    }
}
