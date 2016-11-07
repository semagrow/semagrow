package org.semagrow.plan;

/**
 * Created by angel on 26/9/2016.
 */
public class Pair<A, B> {

    final private A first;

    final private B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public A getFirst() { return first; }

    public B getSecond() { return second; }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("(");
        builder.append(first);
        builder.append(",");
        builder.append(second);
        builder.append(")");
        return builder.toString();
    }

}
