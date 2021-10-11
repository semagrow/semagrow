package org.semagrow.model.vocabulary;

import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Created by angel on 7/4/14.
 */
public final class SEVOD {

    public static final String NAMESPACE = "http://www.w3.org/2015/03/sevod#";

    public static final String PREFIX = "svd";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    public final static IRI SUBJECTREGEXPATTERN;
    public final static IRI OBJECTREGEXPATTERN;
    public final static IRI SUBJECTVOCABULARY;
    public final static IRI OBJECTVOCABULARY;
    public final static IRI SUBJECTCLASS;
    public final static IRI OBJECTCLASS;

    public final static IRI INTINTERVAL;
    public final static IRI DATEINTERVAL;
    public final static IRI FROM;
    public final static IRI TO;

    public final static IRI BOUNDINGWKT;
    public final static IRI BOUNDINGGML;

    public final static IRI JOIN;
    public final static IRI JOINS;
    public final static IRI SELECTIVITY;
    public final static IRI SELECTIVITYVALUE;

    static {
        ValueFactory factory = SimpleValueFactory.getInstance();
        SUBJECTREGEXPATTERN = factory.createIRI(SEVOD.NAMESPACE, "subjectRegexPattern");
        OBJECTREGEXPATTERN = factory.createIRI(SEVOD.NAMESPACE, "objectRegexPattern");
        SUBJECTVOCABULARY = factory.createIRI(SEVOD.NAMESPACE, "subjectVocabulary");
        OBJECTVOCABULARY = factory.createIRI(SEVOD.NAMESPACE, "objectVocabulary");
        OBJECTCLASS = factory.createIRI(SEVOD.NAMESPACE, "objectClass");
        SUBJECTCLASS = factory.createIRI(SEVOD.NAMESPACE, "subjectClass");
        INTINTERVAL = factory.createIRI(SEVOD.NAMESPACE, "intInterval");
        DATEINTERVAL = factory.createIRI(SEVOD.NAMESPACE, "dateInterval");
        FROM = factory.createIRI(SEVOD.NAMESPACE, "from");
        TO = factory.createIRI(SEVOD.NAMESPACE, "to");
        BOUNDINGWKT = factory.createIRI(SEVOD.NAMESPACE, "boundingWKT");
        BOUNDINGGML = factory.createIRI(SEVOD.NAMESPACE, "boundingGML");
        JOIN = factory.createIRI(SEVOD.NAMESPACE, "Join");
        JOINS = factory.createIRI(SEVOD.NAMESPACE, "joins");
        SELECTIVITY = factory.createIRI(SEVOD.NAMESPACE, "selectivity");
        SELECTIVITYVALUE = factory.createIRI(SEVOD.NAMESPACE, "selectivityValue");

    }
}
