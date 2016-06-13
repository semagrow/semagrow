package eu.semagrow.querylog.impl.rdf;

import eu.semagrow.querylog.api.QueryLogException;
import eu.semagrow.querylog.api.QueryLogHandler;
import eu.semagrow.querylog.api.QueryLogParser;
import eu.semagrow.querylog.api.QueryLogRecord;
import eu.semagrow.querylog.impl.QueryLogRecordImpl;
import eu.semagrow.querylog.impl.rdf.vocabulary.QFR;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.repository.sail.SailTupleQuery;
import org.eclipse.rdf4j.repository.sparql.query.SPARQLTupleQuery;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;

/**
 * Created by angel on 31/7/2015.
 */

public class RDFQueryLogParser implements QueryLogParser {

    private QueryLogHandler handler;

    private Model model;

    public RDFQueryLogParser()  { }

    public RDFQueryLogParser(QueryLogHandler handler) {
        this();
        setQueryRecordHandler(handler);
    }

    @Override
    public void setQueryRecordHandler(QueryLogHandler handler) {
        this.handler = handler;
    }

    @Override
    public void parseQueryLog(InputStream in) throws IOException, QueryLogException {
        if (handler == null)
            throw new QueryLogException("No query log handler defined");

        try {
            model = Rio.parse(in, "", RDFFormat.NTRIPLES);
        } catch (Exception e) {
            throw new QueryLogException(e);
        }

        Model queryRecords = model.filter(null, RDF.TYPE, QFR.QUERYRECORD);

        for (Resource qr : queryRecords.subjects()) {
            QueryLogRecord record = parseQueryRecord(qr,model);
            handler.handleQueryRecord(record);
        }
    }

    private QueryLogRecord parseQueryRecord(Resource qr, Model model) {

        Optional<IRI> optionalEndpoint = Models.objectIRI(model.filter(qr, QFR.ENDPOINT, null));
        Optional<IRI> optionalResults  = Models.objectIRI(model.filter(qr, QFR.RESULTFILE, null));

        Date startTime = parseDate(Models.objectLiteral(model.filter(qr, QFR.START, null)).get(), model);
        Date endTime = parseDate(Models.objectLiteral(model.filter(qr, QFR.END, null)).get(), model);

        long cardinality = parseCardinality(Models.objectLiteral(model.filter(qr, QFR.CARDINALITY, null)).get(), model);
        String expr = parseQuery(Models.object(model.filter(qr, QFR.QUERY, null)).get(), model).toString();

        QueryLogRecord r = new QueryLogRecordImpl(null, optionalEndpoint.get(), expr , EmptyBindingSet.getInstance(), Collections.<String>emptyList());

        //r.setDuration(startTime, endTime);

        r.setCardinality(cardinality);
        r.setDuration(startTime.getTime(), endTime.getTime());
        r.setResults(optionalResults.get());
        return r;
    }

    private Date parseDate(Literal literal, Model model) {
        return literal.calendarValue().toGregorianCalendar().getTime();
    }

    private long parseCardinality(Literal literal, Model model) {
        return literal.longValue();
    }

    private TupleExpr parseQuery(Value literal, Model model) {
        if (literal instanceof Literal) {
            String queryString = ((Literal)literal).stringValue();
            try {
                ParsedQuery query = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, queryString, "http://www.iamabsoluteuri.com");
                return query.getTupleExpr();
            } catch (MalformedQueryException e) {
                return null;
            }
        }

        return null;
    }
}