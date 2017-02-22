package org.semagrow.querylog.impl.rdf;

import org.semagrow.querylog.api.QueryLogException;
import org.semagrow.querylog.api.QueryLogRecord;
import org.semagrow.querylog.api.QueryLogWriter;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.semagrow.querylog.impl.rdf.vocabulary.QFR;

import java.util.*;

/**
 * Created by angel on 10/20/14.
 */
public class RDFQueryLogWriter implements QueryLogWriter {

    private RDFWriter handler;
    private ValueFactory vf = SimpleValueFactory.getInstance();


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

    private void createStatement(Resource subject, IRI predicate, Value object)
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

        Resource record = vf.createIRI("urn:" + UUID.randomUUID().toString());

        createStatement(record, RDF.TYPE, QFR.QUERYRECORD);
       // createStatement(record, QFR.SESSION, qr.getSession().getSessionId().toURI());
        createStatement(record, QFR.ENDPOINT, qr.getEndpoint());
        createStatement(record, QFR.RESULTFILE, qr.getResults());
        createStatement(record, QFR.CARDINALITY, vf.createLiteral(qr.getCardinality()));
        createStatement(record, QFR.START, vf.createLiteral(qr.getStartTime()));
        createStatement(record, QFR.END, vf.createLiteral(qr.getEndTime()));
        createStatement(record, QFR.DURATION, vf.createLiteral(qr.getDuration()));
        createStatement(record, QFR.QUERY, createTupleExpr(qr.getQuery(), EmptyBindingSet.getInstance()));

        return record;
    }

    private Value createTupleExpr(String expr, BindingSet bindings)
            throws QueryLogException
    {
        try {
            return vf.createLiteral(expr.toString());
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
