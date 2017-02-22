package org.semagrow.plan;

import org.semagrow.selector.Site;

import java.util.Optional;

/**
 * @author acharal
 */
public class RequestedPlanProperties {

    private Optional<Site> site;

    private Optional<RequestedDataProperties> dataProps;

    public RequestedPlanProperties() {
        site = Optional.empty();
        dataProps = Optional.empty();
    }

    public void setDataProperties(RequestedDataProperties props) {
        this.dataProps = Optional.of(props);
    }

    public void setSite(Site s) {
        this.site = Optional.of(s);
    }

    public Optional<Site> getSite() { return this.site; }


    public Optional<RequestedDataProperties> getDataProperties() { return this.dataProps; }

}
