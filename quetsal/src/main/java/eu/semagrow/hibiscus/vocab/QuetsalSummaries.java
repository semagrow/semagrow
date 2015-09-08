package eu.semagrow.hibiscus.vocab;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Created by antonis on 28/7/2015.
 */
public final class QuetsalSummaries {

    public static final String NAMESPACE = "http://aksw.org/fedsum/";

    public static final String PREFIX = "ds";

    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    public final static URI SERVICE;
    public final static URI URL;
    public final static URI PREDICATE;
    public final static URI CAPABILITY;
    public final static URI SBJAUTHORITY;
    public final static URI OBJAUTHORITY;

    static {
        ValueFactory factory = ValueFactoryImpl.getInstance();
        SERVICE      = factory.createURI(NAMESPACE, "Service");
        URL          = factory.createURI(NAMESPACE, "url");
        PREDICATE    = factory.createURI(NAMESPACE, "predicate");
        CAPABILITY   = factory.createURI(NAMESPACE, "capability");
        SBJAUTHORITY = factory.createURI(NAMESPACE, "sbjAuthority");
        OBJAUTHORITY = factory.createURI(NAMESPACE, "objAuthority");
    }
}
