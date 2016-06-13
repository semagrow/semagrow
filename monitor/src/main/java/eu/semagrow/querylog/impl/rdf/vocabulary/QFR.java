package eu.semagrow.querylog.impl.rdf.vocabulary;

import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Created by angel on 10/21/14.
 */
public final class QFR {

    public static final String NAMESPACE = "http://rdf.demokritos.gr/2014/qfr#";

    public static final String PREFIX = "qfr";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    public static IRI QUERYRECORD;

    public static IRI CARDINALITY;

    public static IRI ENDPOINT;

    public static IRI RESULTFILE;

    public static IRI DURATION;

    public static IRI START;

    public static IRI END;

    public static IRI SESSION;

    public static IRI QUERY;

    public static IRI PATTERN;

    public static IRI BINDING;

    static {
        ValueFactory factory = SimpleValueFactory.getInstance();
        QUERYRECORD = factory.createIRI(QFR.NAMESPACE, "QueryRecord");
        CARDINALITY = factory.createIRI(QFR.NAMESPACE, "cardinality");
        ENDPOINT = factory.createIRI(QFR.NAMESPACE, "endpoint");
        RESULTFILE = factory.createIRI(QFR.NAMESPACE, "resultFile");
        DURATION = factory.createIRI(QFR.NAMESPACE, "duration");
        START = factory.createIRI(QFR.NAMESPACE, "start");
        END = factory.createIRI(QFR.NAMESPACE, "end");
        DURATION = factory.createIRI(QFR.NAMESPACE, "duration");
        SESSION = factory.createIRI(QFR.NAMESPACE, "session");
        QUERY = factory.createIRI(QFR.NAMESPACE, "query");
        PATTERN = factory.createIRI(QFR.NAMESPACE, "pattern");
        BINDING = factory.createIRI(QFR.NAMESPACE, "binding");
    }
}

