package org.semagrow.plan;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 *
 * @author acharal
 */
public class InterestingProperties implements Cloneable {

    private Set<RequestedDataProperties> dataProps = new HashSet<>();

    @Override
    public InterestingProperties clone() {
        InterestingProperties newIntProps = new InterestingProperties();
        newIntProps.dataProps.addAll(this.dataProps);
        return newIntProps;
    }

    public void addStructureProperties(RequestedDataProperties props) {
        dataProps.add(props);
    }

    public void addInterestingProperties(InterestingProperties props) {
        dataProps.addAll(props.dataProps);
    }


    public Set<RequestedDataProperties> getStructureProperties() {
        return dataProps;
    }

    public void dropTrivials() {
        Iterator<RequestedDataProperties> it = dataProps.iterator();
        while (it.hasNext()) {
            RequestedDataProperties props = it.next();
            if (props.isTrivial())
                it.remove();
        }
    }

}
