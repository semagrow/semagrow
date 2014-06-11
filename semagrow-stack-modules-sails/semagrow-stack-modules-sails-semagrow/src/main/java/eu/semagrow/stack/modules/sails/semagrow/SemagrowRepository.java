package eu.semagrow.stack.modules.sails.semagrow;

import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.SailException;

/**
 * Created by angel on 6/10/14.
 */
public class SemagrowRepository extends SailRepository {

    private SemagrowSail semagrowSail;
    public SemagrowRepository(SemagrowSail sail) {
        super(sail);
        semagrowSail = sail;
    }

    @Override
    public SemagrowRepositoryConnection getConnection() throws RepositoryException {
        try {
            return new SemagrowRepositoryConnection(this, (SemagrowSailConnection) semagrowSail.getConnection());
        }catch(Exception e) {
            throw new RepositoryException(e);
        }
    }
}
