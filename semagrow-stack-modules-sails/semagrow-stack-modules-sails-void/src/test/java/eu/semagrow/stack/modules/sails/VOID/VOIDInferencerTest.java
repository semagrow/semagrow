package eu.semagrow.stack.modules.sails.VOID;

import eu.semagrow.stack.modules.vocabulary.VOID;
import info.aduna.iteration.CloseableIteration;
import junit.framework.TestCase;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

/**
 * Created by angel on 4/30/14.
 */
public class VOIDInferencerTest extends TestCase {

    private MemoryStore baseLayer = new MemoryStore();

    public void setUp() throws Exception {
        super.setUp();
    }

    public void testInference() throws Exception {
        VOIDInferencer inferencer = new VOIDInferencer(new ForwardChainingRDFSInferencer(baseLayer));
        inferencer.initialize();
        SailConnection connection = inferencer.getConnection();
        connection.begin();
        Resource dataset1 = ValueFactoryImpl.getInstance().createBNode("dataset1");
        Resource dataset2 = ValueFactoryImpl.getInstance().createBNode("dataset2");
        Resource dataset3 = ValueFactoryImpl.getInstance().createBNode("dataset3");

        //connection.addStatement(dataset2, RDF.TYPE, VOID.DATASET);
        connection.addStatement(dataset2, VOID.SUBSET, dataset1);
        connection.addStatement(dataset1, VOID.SPARQLENDPOINT, ValueFactoryImpl.getInstance().createLiteral("test"));
        connection.addStatement(dataset3, VOID.SPARQLENDPOINT, ValueFactoryImpl.getInstance().createLiteral("test2"));
        connection.addStatement(dataset2, VOID.SUBSET, dataset3);
        connection.addStatement(dataset3, VOID.SUBSET, dataset2);
        connection.commit();

        CloseableIteration<? extends Statement, SailException> result = connection.getStatements(null, null, null, true);

        while (result.hasNext()) {
            System.out.println(result.next());
        }
    }
}
