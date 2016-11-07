package org.semagrow.plan;

import java.util.*;

/**
 * Represents an ordering on a set of variables.
 * @author acharal
 */
public class Ordering implements Cloneable {

    private List<String> variables = new ArrayList<>();

    private List<Order> orders = new ArrayList<>();

    public Ordering() { }

    public Ordering appendOrdering(String val, Order o) {
        if (o == null)
            throw new NullPointerException();
        if (o == Order.NONE)
            throw new IllegalArgumentException("An ordering must not be created with a NONE order");

        this.variables.add(val);
        this.orders.add(o);
        return this;
    }

    /**
     * Checks whether the ordering is covered by another ordering.
     * @param other
     * @return
     */
    public boolean isCoveredBy(Ordering other) {

        // is covered if this is prefix of other and order is either the same or
        // this order is ANY.

        Iterator<String> fieldIt = variables.iterator();
        Iterator<String> otherFieldIt = other.variables.iterator();
        Iterator<Order>  orderIt = orders.iterator();
        Iterator<Order>  otherOrderIt = other.orders.iterator();

        while (otherFieldIt.hasNext()) {

            // if there is no next in this fields then is prefix.
            if (!fieldIt.hasNext())
                return true;

            // variables not equal.
            if (!otherFieldIt.next().equals(fieldIt.next()))
                return false;


            // if this order is not covered by other order then this ordering is not covered as well
            if (!orderIt.next().isCoveredBy(otherOrderIt.next()))
                return false;
        }

        // other fields is empty but this is not empty, therefore this is not a prefix of other
        if (fieldIt.hasNext())
            return false;

        return true;
    }

    public Iterator<OrderedVariable> getOrderedVariables() {
            return new Iterator<OrderedVariable>() {

                    private Iterator<Order> oit = orders.iterator();

                    private Iterator<String> vit = variables.iterator();

                    public boolean hasNext() {
                        return oit.hasNext() && vit.hasNext();
                    }

                    public OrderedVariable next() {
                        return new OrderedVariable(vit.next(), oit.next());
                    }
            };
    }

    public Set<String> getVariables() {
        return new HashSet<>(variables);
    }


    @Override
    public Ordering clone() {

        Ordering o = new Ordering();
        o.variables.addAll(this.variables);
        o.orders.addAll(this.orders);
        return o;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (variables.hashCode());
        result = prime * result + (orders.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null)
            return false;

        if (this.getClass() != obj.getClass())
            return false;

        Ordering other = (Ordering) obj;

        return this.variables.equals(other.variables) && this.orders.equals(other.orders);
    }

    @Override
    public String toString() {

        StringBuffer buffer = new StringBuffer();
        buffer.append("[");
        Iterator<String> v = variables.iterator();
        Iterator<Order> o  = orders.iterator();

        boolean first = true;

        while (v.hasNext() && o.hasNext()) {

            if (!first)
                buffer.append(", ");

            buffer.append(v.next());
            buffer.append(" as ");
            buffer.append(o.next().getShortName());

            first = false;
        }

        buffer.append("]");
        return buffer.toString();
    }

    public class OrderedVariable {

        private String var;

        private Order order;

        public OrderedVariable(String var, Order o) {
                    this.var = var;
                    this.order = o;
                }

        public String getVariable(){ return var; }

        public Order getOrder() {return order; }

        public boolean isCoveredBy(OrderedVariable other) {
            if (this.getVariable().equals(other.getVariable()))
                return this.order.isCoveredBy(other.order);
            else
                        return false;
        }
    }

}
