package eu.semagrow.stack.modules.querydecomp.selector.mock;

import eu.semagrow.stack.modules.utils.resourceselector.Measurement;
import eu.semagrow.stack.modules.utils.resourceselector.ResourceSelector;
import eu.semagrow.stack.modules.utils.resourceselector.SelectedResource;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.StatementPattern;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by angel on 3/14/14.
 */
public class TrivialResourceSelector implements ResourceSelector {

    protected class DummySelectedResource implements SelectedResource {

        private URI endpoint;

        public DummySelectedResource(String endpoint) {
             this.endpoint = ValueFactoryImpl.getInstance().createURI(endpoint);
        }

        public URI getEndpoint() {
            return endpoint;
        }

        public int getVol() {
            return 0;
        }

        public int getVar() {
            return 0;
        }

        public URI getSubject() {
            return null;
        }

        public void setSubject(URI subject) {

        }

        public int getSubjectProximity() {
            return 0;
        }

        public void setSubjectProximity(int subjectProximity) {

        }

        public URI getPredicate() {
            return null;
        }

        public void setPredicate(URI predicate) {

        }

        public int getPredicateProximity() {
            return 0;
        }

        public void setPredicateProximity(int predicateProximity) {

        }

        public URI getObject() {
            return null;
        }

        public void setObject(URI object) {

        }

        public int getObjectProximity() {
            return 0;
        }

        public void setObjectProximity(int objectProximity) {

        }

        public List<Measurement> getLoadInfo() {
            return null;
        }

        public void setLoadInfo(List<Measurement> loadInfo) {

        }
    }

    public List<SelectedResource> getSelectedResources(StatementPattern statementPattern, long measurement_id) {

        SelectedResource resource = null;
        if (statementPattern.getSubjectVar().getName().equals("y"))
            resource = new DummySelectedResource("http://143.233.226.25:8080/bigdata_biblio_20M_2/sparql");
        else
            resource = new DummySelectedResource("http://143.233.226.25:8080/bigdata_sensor_all/sparql");

        ArrayList<SelectedResource> resources = new ArrayList<SelectedResource>(1);
        resources.add(resource);
        return resources;
    }
}
