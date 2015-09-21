package eu.semagrow.querylog.impl.rdf;

import eu.semagrow.querylog.api.QueryLogException;
import eu.semagrow.querylog.api.QueryLogHandler;
import eu.semagrow.querylog.api.QueryLogParser;
import eu.semagrow.querylog.api.QueryLogRecord;
import eu.semagrow.querylog.impl.QueryLogRecordImpl;
import eu.semagrow.querylog.impl.rdf.vocabulary.QFR;
import org.openrdf.model.*;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Date;

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

        URI endpoint = model.filter(qr, QFR.ENDPOINT, null).objectURI();
        URI results  = model.filter(qr, QFR.RESULTFILE, null).objectURI();

        Date startTime = parseDate(model.filter(qr, QFR.START, null).objectLiteral(), model);
        Date endTime = parseDate(model.filter(qr,QFR.END, null).objectLiteral(), model);

        long cardinality = parseCardinality(model.filter(qr, QFR.CARDINALITY, null).objectLiteral(), model);
        TupleExpr expr = parseQuery(model.filter(qr, QFR.QUERY, null).objectValue(), model);

        QueryLogRecord r = new QueryLogRecordImpl(null, endpoint, expr, EmptyBindingSet.getInstance(), Collections.<String>emptyList());

        //r.setDuration(startTime, endTime);

        r.setCardinality(cardinality);
        r.setDuration(startTime.getTime(), endTime.getTime());
        r.setResults(results);
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