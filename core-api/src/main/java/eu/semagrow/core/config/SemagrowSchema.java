package eu.semagrow.core.config;

import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;


/**
 *
 * @author Angelos Charalambidis
 */
public class SemagrowSchema {

    public static final String NAMESPACE = "http://schema.semagrow.eu/";

    public static final String PREFIX = "semagrow";
    public static final IRI SOURCESELECTOR ;

    public static Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    public static final IRI METADATAINIT;

    public static final IRI EXECUTORBATCHSIZE;

    public static final IRI QUERYTRANSFORMDB;
    public static final IRI QUERYTRANSFORMUSER;
    public static final IRI QUERYTRANSFORMPASSWORD;

    // queryLog handler
    // source selection
    // decomposition algorithm
    //      cost estimation
    //              cardinality estimation
    // load initial metadata ?

    static  {
        ValueFactory vf = SimpleValueFactory.getInstance();
        METADATAINIT = vf.createIRI(NAMESPACE, "metadataInit");
        EXECUTORBATCHSIZE = vf.createIRI(NAMESPACE, "executorBatchSize");
        QUERYTRANSFORMDB = vf.createIRI(NAMESPACE, "queryTransformDB");
        QUERYTRANSFORMUSER = vf.createIRI(NAMESPACE, "queryTransformUser");
        QUERYTRANSFORMPASSWORD = vf.createIRI(NAMESPACE, "queryTransformPass");
        SOURCESELECTOR = vf.createIRI(NAMESPACE, "sourceSelector");
    }
}
