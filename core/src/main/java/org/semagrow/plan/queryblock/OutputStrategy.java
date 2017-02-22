package org.semagrow.plan.queryblock;

/**
 * @author acharal
 */
public enum OutputStrategy {

    ENFORCE,          /* output requirement is enforced by an explicit operator in this level */

    PERMIT,           /* there is no guarantee that the output requirement will be met */

    PRESERVE;          /* preserves the requirement imposed by lower blocks */

    public boolean isEnforced() { return this == ENFORCE; }

}
