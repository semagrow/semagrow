package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog.rdf;

import eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog.QueryLogException;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog.QueryLogRecord;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog.QueryLogWriter;
import eu.semagrow.stack.modules.vocabulary.QFR;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.queryrender.sparql.SPARQLQueryRenderer;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;

import java.util.List;
import java.util.UUID;

/**
 * Created by angel on 10/20/14.
 */
public class RDFQueryLogWriter implements QueryLogWriter {

    private RDFWriter handler;
    private ValueFactory vf = ValueFactoryImpl.getInstance();

    public RDFQueryLogWriter(RDFWriter handler) {
        this.handler = handler;
    }

    @Override
    public void startQueryLog() throws QueryLogException {
        try {
            handler.startRDF();
        } catch (RDFHandlerException e) {
            throw new QueryLogException(e);
        }
    }


    @Override
    public void handleQueryRecord(QueryLogRecord queryLogRecord) throws QueryLogException {
        createQueryRecord(queryLogRecord);
    }

    private void createStatement(Resource subject, URI predicate, Value object)
            throws QueryLogException
    {
        try {
            if (subject != null && predicate != null && object != null)
                handler.handleStatement(vf.createStatement(subject, predicate, object));

        } catch (RDFHandlerException e) {
            throw new QueryLogException(e);
        }
    }

    private Value createQueryRecord(QueryLogRecord qr) throws QueryLogException {

        Resource record = vf.createURI("urn:" + UUID.randomUUID().toString());

        createStatement(record, RDF.TYPE, QFR.QUERYRECORD);
        createStatement(record, QFR.SESSION, qr.getSession().getSessionId().toURI());
        createStatement(record, QFR.ENDPOINT, qr.getEndpoint());
        createStatement(record, QFR.RESULTFILE, qr.getResults().getId());
        createStatement(record, QFR.CARDINALITY, vf.createLiteral(qr.getCardinality()));
        createStatement(record, QFR.START, vf.createLiteral(qr.getStartTime()));
        createStatement(record, QFR.END, vf.createLiteral(qr.getEndTime()));
        createStatement(record, QFR.DURATION, vf.createLiteral(qr.getDuration()));
        createStatement(record, QFR.QUERY, createTupleExpr(qr.getQuery(), EmptyBindingSet.getInstance()));

        return record;
    }

    private Value createTupleExpr(TupleExpr expr, BindingSet bindings)
            throws QueryLogException
    {
        try {
            String queryString = null;
            ParsedTupleQuery query = new ParsedTupleQuery(expr);
            queryString = new SPARQLQueryRenderer().render(query);
            return vf.createLiteral(queryString);
        } catch (Exception e) {
            throw new QueryLogException(e);
        }
    }

    @Override
    public void endQueryLog() throws QueryLogException {
        try {
            handler.endRDF();
        } catch (RDFHandlerException e) {
            throw new QueryLogException(e);
        }
    }

}
