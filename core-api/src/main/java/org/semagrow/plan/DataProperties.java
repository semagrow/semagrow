package org.semagrow.plan;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * This class is used to describe properties regarding the structure of the data
 * such as ordering and grouping.
 */
public class DataProperties {

    /**
     * The optional ordering of the data.
     */
    Optional<Ordering> ordering;

    /**
     * The optional grouping of the data.
     * Grouping is represented by the set of the grouped fields.
     */
    Optional<Set<String>> groupedFields;

    /**
     * The set of fields that combined identify uniquely the row.
     */
    Set<Set<String>> uniqueFields = new HashSet<>();

    public DataProperties() {
        ordering = Optional.empty();
        groupedFields = Optional.empty();
    }

    public void setGrouping(Set<String> groupedFields) {
        this.groupedFields = Optional.of(new HashSet<>(groupedFields));
    }

    public void setOrdering(Ordering o) {
        this.ordering = Optional.of(o);
    }

    public boolean hasOrdering() { return this.ordering.isPresent(); }

    public void addUnique(Set<String> unique) {
        // FIXME: Check if there is already a superset of unique
        uniqueFields.add(unique);
    }

    public void removeUnique(Set<String> unique) {
        uniqueFields.remove(unique);
    }

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
            } else
                return false;
        }

        return true;
    }

    public boolean isTrivial() {
        return !ordering.isPresent() && !groupedFields.isPresent();
    }

    public RequestedDataProperties asRequestedProperties() {

        if (ordering.isPresent())
            return RequestedDataProperties.forOrdering(ordering.get());
        else if (groupedFields.isPresent())
            return RequestedDataProperties.forGrouping(groupedFields.get());
        else
            return new RequestedDataProperties();
    }

}
