package eu.semagrow.hibiscus.util;

import eu.semagrow.commons.vocabulary.SEVOD;
import eu.semagrow.commons.vocabulary.VOID;
import eu.semagrow.hibiscus.vocab.QuetsalSummaries;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.helpers.RDFHandlerBase;

/**
 * Created by tru on 27/7/2015.
 */
public class SummariesGenerator extends RDFHandlerBase {

    private RDFWriter writer;

    private ValueFactory vf = ValueFactoryImpl.getInstance();

    public SummariesGenerator(RDFWriter writer) {
        this.writer = writer;
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        writer.startRDF();
        writer.handleNamespace(QuetsalSummaries.PREFIX, QuetsalSummaries.NAMESPACE);
        super.startRDF();
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {

        if (equal(st.getPredicate(), SEVOD.SUBJECTVOCABULARY)) {
            writer.handleStatement(vf.createStatement(st.getSubject(), QuetsalSummaries.SBJAUTHORITY, removeSlashAtTheEnd(st.getObject())));
        }
        if (equal(st.getPredicate(), SEVOD.OBJECTVOCABULARY)) {
            writer.handleStatement(vf.createStatement(st.getSubject(), QuetsalSummaries.OBJAUTHORITY, removeSlashAtTheEnd(st.getObject())));
        }
        if (equal(st.getPredicate(), VOID.PROPERTYPARTITION)) {
            writer.handleStatement(vf.createStatement(st.getSubject(), QuetsalSummaries.CAPABILITY, st.getObject()));
        }
        if (equal(st.getPredicate(), VOID.SUBSET)) {
            writer.handleStatement(vf.createStatement(st.getSubject(), QuetsalSummaries.CAPABILITY, st.getObject()));
        }
        if (equal(st.getPredicate(), VOID.SPARQLENDPOINT)) {
            writer.handleStatement(vf.createStatement(st.getSubject(), QuetsalSummaries.URL, st.getObject()));
        }
        if (equal(st.getObject(), VOID.DATASET)) {
            writer.handleStatement(vf.createStatement(st.getSubject(), st.getPredicate(), QuetsalSummaries.SERVICE));
        }
        if (equal(st.getPredicate(), VOID.PROPERTY)) {
            writer.handleStatement(vf.createStatement(st.getSubject(), QuetsalSummaries.PREDICATE, st.getObject()));
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        writer.endRDF();
        super.endRDF();
    }

    private boolean equal(Value u, Value v) {
        return u.toString().equals(v.toString());
    }

    private Value removeSlashAtTheEnd(Value val) {
        if (val.toString().endsWith("/"))
            return vf.createURI(val.toString().substring(0, val.toString().lastIndexOf('/')));
        else
            return val;
    }
}
