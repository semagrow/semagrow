
package eu.semagrow.stack.modules.sails.powder;

import java.io.File;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

/**
 *
 * @author http://www.turnguard.com/turnguard
 */
public class PowderSail implements Sail {

    private Value postgresHost;
    private Value postgresPort;
    private Value postgresDatabase;
    private Value postgresUser;
    private Value postgresPassword;
    
    public void setDataDir(File file) {       
    }

    public File getDataDir() {
        return null;
    }

    public void initialize() throws SailException {
        if(postgresHost==null || postgresPort==null || postgresDatabase==null || postgresUser==null || postgresPassword==null){
            throw new SailException("host, port, database, user and password are required");
        }        
    }

    public void shutDown() throws SailException {        
    }

    public boolean isWritable() throws SailException {
        return true;
    }

    public SailConnection getConnection() throws SailException {
        return new PowderSailConnection(this);
    }

    public ValueFactory getValueFactory() {
        return ValueFactoryImpl.getInstance();
    }

    /* POWDER SAIL SPECIFICS */
    public void setPostgresHost(Value postgresHost){
        this.postgresHost = postgresHost;
    }
    public void setPostgresPort(Value postgresPort){
        this.postgresPort = postgresPort;
    }
    public void setPostgresDatabase(Value postgresDatabase){
        this.postgresDatabase = postgresDatabase;
    }
    public void setPostgresUser(Value postgresUser){
        this.postgresUser = postgresUser;
    }
    public void setPostgresPassword(Value postgresPassword){
        this.postgresPassword = postgresPassword;
    }
}
