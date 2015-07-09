package eu.semagrow.querylog.impl.rdf.vocabulary;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Created by angel on 10/21/14.
 */
public final class QFR {

    public static final String NAMESPACE = "http://rdf.demokritos.gr/2014/qfr#";

    public static final String PREFIX = "qfr";

    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    public static URI QUERYRECORD;

    public static URI CARDINALITY;

    public static URI ENDPOINT;

    public static URI RESULTFILE;

    public static URI DURATION;

    public static URI START;

    public static URI END;

    public static URI SESSION;

    public static URI QUERY;

    public static URI PATTERN;

    public static URI BINDING;

    static {
        ValueFactory factory = ValueFactoryImpl.getInstance();
        QUERYRECORD = factory.createURI(QFR.NAMESPACE, "QueryRecord");
        CARDINALITY = factory.createURI(QFR.NAMESPACE, "cardinality");
        ENDPOINT = factory.createURI(QFR.NAMESPACE, "endpoint");
        RESULTFILE = factory.createURI(QFR.NAMESPACE, "resultFile");
        DURATION = factory.createURI(QFR.NAMESPACE, "duration");
        START = factory.createURI(QFR.NAMESPACE, "start");
        END = factory.createURI(QFR.NAMESPACE, "end");
        DURATION = factory.createURI(QFR.NAMESPACE, "duration");
        SESSION = factory.createURI(QFR.NAMESPACE, "session");
        QUERY = factory.createURI(QFR.NAMESPACE, "query");
        PATTERN = factory.createURI(QFR.NAMESPACE, "pattern");
        BINDING = factory.createURI(QFR.NAMESPACE, "binding");
    }
}

