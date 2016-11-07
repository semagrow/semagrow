package org.semagrow.plan;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Represents the data properties that can be requested by an operator from its inputs.
 * <p>
 * This is a subset of {@link DataProperties} since not every property can be requested.
 * @author acharal
 * @see DataProperties
 */
public class RequestedDataProperties {

    /**
     * The optional ordering of the data.
     */
    Optional<Ordering> ordering;

    /**
     * The optional grouping of the data.
     * Grouping is represented by the set of the grouped fields.
     */
    Optional<Set<String>> groupedFields;

    public RequestedDataProperties(){
        ordering = Optional.empty();
        groupedFields = Optional.empty();
    }

    /**
     * Check whether the requested data properties are satisfied by the given
     * data properties.
     * @param other the dataproperties to check against
     * @return true if {@code other} satisfies the requested data properties
     */
    public boolean isCoveredBy(DataProperties other) {

        if (ordering.isPresent()) {
            // check whether the ordering is covered by the other ordering
            return other.ordering
                    .map( o -> o.isCoveredBy(ordering.get()) )
                    .orElse(false);
        } else if (groupedFields.isPresent()) {
            if (other.groupedFields.isPresent()) {
                // is grouped at least at the same fields
                // or the rest of the fields are unique.
                return other.groupedFields.get().containsAll(groupedFields.get());
            }
        }

        return true;
    }

    /**
     * Check whether the requested properties are trivial, i.e.
     * there is no specific ordering or grouping imposed.
     *
     * @return true if properties are trivial; false otherwise.
     */
    public boolean isTrivial() {
        return !ordering.isPresent() && !groupedFields.isPresent();
    }

    /**
     * Create a {@link RequestedDataProperties} to request a specific ordering
     * @param o the requested ordering
     * @return a valid RequestedDataProperties object the encapsulates the requested ordering
     */
    public static RequestedDataProperties forOrdering(Ordering o) {
        RequestedDataProperties props = new RequestedDataProperties();
        props.ordering = Optional.of(o.clone());
        return props;
    }

    /**
     * Create a {@link RequestedDataProperties} to request a specific grouping
     * @param groupedFields the requested grouping
     * @return a valid RequestedDataProperties object the encapsulates the requested grouping
     */
    public static RequestedDataProperties forGrouping(Set<String> groupedFields) {
        RequestedDataProperties props = new RequestedDataProperties();
        props.groupedFields = Optional.of(new HashSet<>(groupedFields));
        return props;
    }

}
