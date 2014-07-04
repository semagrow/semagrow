package eu.semagrow.stack.modules.vocabulary;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Created by angel on 7/4/14.
 */
public final class SEVOD {

    public static final String NAMESPACE = "http://rdf.iit.demokritos.gr/2013/sevod#";

    public static final String PREFIX = "svd";

    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    public final static URI SUBJECTREGEXPATTERN;

    public final static URI OBJECTREGEXPATTERN;

    public final static URI SUBJECTCLASS;

    public final static URI OBJECTCLASS;

    static {
        ValueFactory factory = ValueFactoryImpl.getInstance();
        SUBJECTREGEXPATTERN = factory.createURI(SEVOD.NAMESPACE, "subjectRegexPattern");
        OBJECTREGEXPATTERN = factory.createURI(SEVOD.NAMESPACE, "objectRegexPattern");
        OBJECTCLASS = factory.createURI(SEVOD.NAMESPACE, "objectClass");
        SUBJECTCLASS = factory.createURI(SEVOD.NAMESPACE, "subjectClass");
    }
}
