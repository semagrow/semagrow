package org.semagrow.plan;

/**
 * Enumeration representing the order direction.
 * May represent ascending order, descending order or no order.
 * @author acharal
 * @since 2.0
 */
public enum Order {

    /* Represents that there is no order */
    NONE,

    /* Ascending order */
    ASCENDING,

    /* Descending order */
    DESCENDING,

    /* There is ordering but the direction is irrelevant */
    ANY;

    public String getShortName() {
        if (this == ASCENDING)
            return "ASC";
        else if (this == DESCENDING)
            return "DESC";
        else if (this == ANY)
            return "*";
        else
            return "-";
    }

    /**
     *
     * @return true if the object represents an actual order direction; false otherwise
     */
    public boolean isOrdered() { return this != NONE; }

    public boolean isCoveredBy(Order o) {

        // if this is NONE then is always covered
        if (this != NONE) {
            // if other is ANY then this is always covered
            if (o != ANY) {
                // orders must be the same
                return (this == o);
            }
        }
        return true;
    }
}
