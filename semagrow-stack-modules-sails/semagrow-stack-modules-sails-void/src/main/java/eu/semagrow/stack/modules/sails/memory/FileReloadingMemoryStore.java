package eu.semagrow.stack.modules.sails.memory;

import org.apache.commons.io.filefilter.NameFileFilter;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.*;
import org.openrdf.sail.helpers.NotifyingSailConnectionWrapper;
import org.openrdf.sail.helpers.SailBase;
import org.openrdf.sail.memory.MemoryStore;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by angel on 5/29/14.
 */
public class FileReloadingMemoryStore extends SailBase implements NotifyingSail {

    private MemoryStore store = new MemoryStore();

    public FileReloadingMemoryStore(String filename) {
        // TODO: check that filename exists
        File file = new File(filename);
        handleFileChange(file);
        try {
            attachReload(filename);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected class ReloadListener extends FileAlterationListenerAdaptor {

        private FileReloadingMemoryStore reloadingMemoryStore;

        public ReloadListener(FileReloadingMemoryStore store) {
            reloadingMemoryStore = store;
        }

        @Override
        public void onFileChange(File file) {
            reloadingMemoryStore.handleFileChange(file);
        }
    }

    private FileAlterationMonitor monitor = null;

    private FileAlterationMonitor getMonitor() {
        if (monitor == null)
            monitor = new FileAlterationMonitor(5);
        return monitor;
    }

    public void attachReload(String filename) throws Exception {
        File file = new File(filename);
        if (file.isFile()) {
            FileFilter filter = new NameFileFilter(file.getName());
            FileAlterationObserver observer = new FileAlterationObserver(file.getParentFile().getAbsolutePath(), filter);
            observer.addListener(new ReloadListener(this));
            FileAlterationMonitor monitor = getMonitor();
            monitor.addObserver(observer);
            monitor.start();
        }
    }

    /**
     * Objects that should be notified of changes to the data in this Sail.
     */
    private Set<SailChangedListener> sailChangedListeners = new HashSet<SailChangedListener>(0);

    @Override
    public NotifyingSailConnection getConnection()
            throws SailException
    {
        return store.getConnection();
    }

    private void addSailChangedListenerInternal(SailChangedListener listener) {
        synchronized (sailChangedListeners) {
            sailChangedListeners.add(listener);
        }
    }

    private void removeSailChangedListenerInternal(SailChangedListener listener) {
        synchronized (sailChangedListeners) {
            sailChangedListeners.remove(listener);
        }
    }

    @Override
    protected void shutDownInternal() throws SailException {
        store.shutDown();
        if (monitor != null)
            try {
                monitor.stop();
            } catch (Exception e) {
                throw new SailException(e);
            }
    }

    @Override
    protected NotifyingSailConnection getConnectionInternal() throws SailException {
        return new NotifyingSailConnectionWrapper(store.getConnection());
    }

    public void addSailChangedListener(SailChangedListener listener) {
        addSailChangedListenerInternal(listener);
        store.addSailChangedListener(listener);
    }

    public void removeSailChangedListener(SailChangedListener listener) {
        removeSailChangedListenerInternal(listener);
        store.removeSailChangedListener(listener);
    }

    /**
     * Notifies all registered SailChangedListener's of changes to the contents
     * of this Sail.
     */
    public void notifySailChanged(SailChangedEvent event) {
        synchronized (sailChangedListeners) {
            for (SailChangedListener l : sailChangedListeners) {
                l.sailChanged(event);
            }
        }
    }

    public boolean isWritable() throws SailException {
        return store.isWritable();
    }

    public ValueFactory getValueFactory() {
        return store.getValueFactory();
    }

    public void handleFileChange(File file) {
        MemoryStore memoryStore = new MemoryStore();
        try {
            memoryStore.initialize();
            loadFile(file, memoryStore);
        } catch (SailException e) {
            e.printStackTrace();
        } catch (RepositoryException e) {
            e.printStackTrace();
        } catch (RDFParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        changeStore(memoryStore);
    }

    protected void changeStore(MemoryStore store) {
        assert store != null;

        for (SailChangedListener listener : sailChangedListeners)
            store.addSailChangedListener(listener);

        this.store = store;
        final Sail s = this;

        notifySailChanged(new SailChangedEvent() {
            public Sail getSail() {
                return s;
            }

            public boolean statementsAdded() {
                return true;
            }

            public boolean statementsRemoved() {
                return true;
            }
        });
    }

    private void loadFile(File file, Sail sail)
            throws SailException, RepositoryException, RDFParseException, IOException {
        Repository sailRepo = new SailRepository(sail);
        RepositoryConnection conn = sailRepo.getConnection();
        conn.add(file, "file://" + file.getAbsoluteFile(), RDFFormat.RDFXML);
    }
}
