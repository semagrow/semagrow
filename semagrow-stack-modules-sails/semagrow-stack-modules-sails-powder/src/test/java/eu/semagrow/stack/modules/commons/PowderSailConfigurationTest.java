package eu.semagrow.stack.modules.commons;

import eu.semagrow.stack.modules.sails.powder.PowderSail;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.sail.SailException;

/**
 *
 * @author turnguard
 */
public class PowderSailConfigurationTest {
    
    public PowderSailConfigurationTest() {
        System.out.println("Construct");
    }
    
    @BeforeClass
    public static void setUpClass() {
        System.out.println("setUpClass");
    }
    
    @AfterClass
    public static void tearDownClass() {
        System.out.println("tearDownClass");
    }
    
    @Before
    public void setUp() {
        System.out.println("setUp");
    }
    
    @After
    public void tearDown() {
        System.out.println("tearDown");
    }
    
    @Test
    public void configPowderSailByCodeCorrectly() throws SailException {
        PowderSail powderSail = new PowderSail();
                   powderSail.setPostgresHost(new LiteralImpl("http://localhost"));
                   powderSail.setPostgresPort(new LiteralImpl("8080", XMLSchema.LONG));
                   powderSail.setPostgresDatabase(new LiteralImpl("myTripleDB"));
                   powderSail.setPostgresUser(new LiteralImpl("user"));
                   powderSail.setPostgresPassword(new LiteralImpl("pass"));
                   powderSail.initialize();
    }
    
    @Test(expected = SailException.class)
    public void configPowderSailByCodeWithMissingHost() throws SailException {
        PowderSail powderSail = new PowderSail();                   
                   powderSail.setPostgresPort(new LiteralImpl("8080", XMLSchema.LONG));
                   powderSail.setPostgresDatabase(new LiteralImpl("myTripleDB"));
                   powderSail.setPostgresUser(new LiteralImpl("user"));
                   powderSail.setPostgresPassword(new LiteralImpl("pass"));
                   powderSail.initialize();
    }
    
    @Test(expected = SailException.class)
    public void configPowderSailByCodeWithMissingPort() throws SailException {
        PowderSail powderSail = new PowderSail();
                   powderSail.setPostgresHost(new LiteralImpl("http://localhost"));                   
                   powderSail.setPostgresDatabase(new LiteralImpl("myTripleDB"));
                   powderSail.setPostgresUser(new LiteralImpl("user"));
                   powderSail.setPostgresPassword(new LiteralImpl("pass"));
                   powderSail.initialize();
    }    
    
    @Test(expected = SailException.class)
    public void configPowderSailByCodeWithMissingDatabase() throws SailException {
        PowderSail powderSail = new PowderSail();
                   powderSail.setPostgresHost(new LiteralImpl("http://localhost"));
                   powderSail.setPostgresPort(new LiteralImpl("8080", XMLSchema.LONG));                   
                   powderSail.setPostgresUser(new LiteralImpl("user"));
                   powderSail.setPostgresPassword(new LiteralImpl("pass"));
                   powderSail.initialize();
    }    
    
    @Test(expected = SailException.class)
    public void configPowderSailByCodeWithMissingUser() throws SailException {
        PowderSail powderSail = new PowderSail();
                   powderSail.setPostgresHost(new LiteralImpl("http://localhost"));
                   powderSail.setPostgresPort(new LiteralImpl("8080", XMLSchema.LONG));
                   powderSail.setPostgresDatabase(new LiteralImpl("myTripleDB"));                   
                   powderSail.setPostgresPassword(new LiteralImpl("pass"));
                   powderSail.initialize();
    }    
    
    @Test(expected = SailException.class)
    public void configPowderSailByCodeWithMissingPassword() throws SailException {
        PowderSail powderSail = new PowderSail();
                   powderSail.setPostgresHost(new LiteralImpl("http://localhost"));
                   powderSail.setPostgresPort(new LiteralImpl("8080", XMLSchema.LONG));
                   powderSail.setPostgresDatabase(new LiteralImpl("myTripleDB"));                   
                   powderSail.setPostgresUser(new LiteralImpl("pass"));
                   powderSail.initialize();
    }    
}
