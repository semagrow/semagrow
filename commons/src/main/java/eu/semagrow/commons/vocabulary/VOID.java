package eu.semagrow.commons.vocabulary;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Created by angel on 4/28/14.
 */
public final class VOID {

    public static final String NAMESPACE = "http://rdfs.org/ns/void#";

    public static final String PREFIX = "void";

    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    public final static URI DATASET;

    public final static URI LINKSET;

    public final static URI SUBSET;
    public final static URI PROPERTYPARTITION;
    public final static URI CLASSPARTITION;
    public final static URI SPARQLENDPOINT;

    public final static URI TRIPLES;
    public final static URI PROPERTIES;
    public final static URI CLASSES;
    public final static URI ENTITIES;
    public final static URI DISTINCTSUBJECTS;
    public final static URI DISTINCTOBJECTS;
    public final static URI URIREGEXPATTERN;
    public final static URI PROPERTY;
    public final static URI CLASS;
    public final static URI URISPACE;

    static {
        ValueFactory factory = ValueFactoryImpl.getInstance();
        DATASET = factory.createURI(VOID.NAMESPACE, "Dataset");
        LINKSET = factory.createURI(VOID.NAMESPACE, "Linkset");
        SUBSET = factory.createURI(VOID.NAMESPACE, "subset");
        PROPERTYPARTITION = factory.createURI(VOID.NAMESPACE, "propertyPartition");
        CLASSPARTITION = factory.createURI(VOID.NAMESPACE, "classPartition");
        SPARQLENDPOINT = factory.createURI(VOID.NAMESPACE, "sparqlEndpoint");
        TRIPLES = factory.createURI(VOID.NAMESPACE, "triples");
        PROPERTIES = factory.createURI(VOID.NAMESPACE, "properties");
        CLASSES = factory.createURI(VOID.NAMESPACE, "classes");
        ENTITIES = factory.createURI(VOID.NAMESPACE, "entities");
        DISTINCTSUBJECTS = factory.createURI(VOID.NAMESPACE, "distinctSubjects");
        DISTINCTOBJECTS = factory.createURI(VOID.NAMESPACE, "distinctObjects");
        URIREGEXPATTERN = factory.createURI(VOID.NAMESPACE, "uriRegexPattern");
        PROPERTY = factory.createURI(VOID.NAMESPACE, "property");
        CLASS = factory.createURI(VOID.NAMESPACE, "class");
        URISPACE = factory.createURI(VOID.NAMESPACE, "uriSpace");
    }

}
