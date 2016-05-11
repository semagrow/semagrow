package eu.semagrow.cassandra.vocab;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Created by antonis on 7/4/2016.
 */
public final class CDV {
    public static final String NAMESPACE = "http://schema.org/";

    public static final URI CASSANDRADB;
    public static final URI SCHEMA;

    public static final URI ADDRESS;
    public static final URI PORT;
    public static final URI KEYNOTE;

    public static final URI BASE;

    public static final URI TABLES;
    public static final URI NAME;
    public static final URI TABLESCHEMA;
    public static final URI PRIMARYKEY;
    public static final URI SECONDARYINDEX;

    public static final URI COLUMNS;
    public static final URI COLUMNTYPE;
    public static final URI PARTITION;
    public static final URI CLUSTERING;
    public static final URI REGULAR;
    public static final URI CLUSTERINGPOSITION;
    public static final URI CLUSTERINGORDER;
    public static final URI DATATYPE;

    static {
        ValueFactory vf = ValueFactoryImpl.getInstance();

        CASSANDRADB = vf.createURI(NAMESPACE, "cassandraDB");
        SCHEMA = vf.createURI(NAMESPACE, "cassandraSchema");

        ADDRESS = vf.createURI(NAMESPACE, "address");
        PORT = vf.createURI(NAMESPACE, "port");
        KEYNOTE = vf.createURI(NAMESPACE, "keynote");

        BASE = vf.createURI(NAMESPACE, "base");

        TABLES = vf.createURI(NAMESPACE, "tables");
        NAME = vf.createURI(NAMESPACE, "name");
        TABLESCHEMA = vf.createURI(NAMESPACE, "tableSchema");
        PRIMARYKEY = vf.createURI(NAMESPACE, "primaryKey");
        SECONDARYINDEX = vf.createURI(NAMESPACE, "secondaryIndex");

        COLUMNS = vf.createURI(NAMESPACE, "columns");
        COLUMNTYPE = vf.createURI(NAMESPACE, "columnType");
        PARTITION = vf.createURI(NAMESPACE, "partition");
        CLUSTERING = vf.createURI(NAMESPACE, "clustering");
        REGULAR = vf.createURI(NAMESPACE, "regular");
        CLUSTERINGPOSITION = vf.createURI(NAMESPACE, "clusteringPosition");
        CLUSTERINGORDER = vf.createURI(NAMESPACE, "clusteringOrder");
        DATATYPE = vf.createURI(NAMESPACE, "datatype");
    }
}
