package org.semagrow.plan

import spock.genesis.Gen
import spock.lang.Specification

/**
 * Created by angel on 29/8/2016.
 */
class OrderingSpec extends Specification {

    def "Should throw exception when Order.NONE is used" () {
        given : def ordering = new Ordering()
        when  : ordering.appendOrdering(var, Order.NONE)
        then  : thrown(IllegalArgumentException)
        where : var << Gen.string(~/[a-zA-Z]+/).take(1)
    }

    def "Append an Order to an empty Ordering" () {
        given : def ordering = new Ordering()
        when  : ordering.appendOrdering(var, order)
        then  : ordering.equals(singletonOrdering(var, order))
        where :
            order  << Gen.these([Order.ASCENDING, Order.DESCENDING, Order.ANY])
            var    << Gen.string(~/[a-zA-Z]+/).take(3)
    }

    def "Every Ordering is covered by itself" () {
        expect : ordering.isCoveredBy(ordering)
        and    : ordering.isCoveredBy(ordering.clone())
        where  : ordering << someOrderings()
    }

    def "Every ordering covers any of their prefixes but not vice versa" () {
        given  : def ordering = prefix.clone()
                 lst.forEach { v -> ordering.appendOrdering(v.first(), v.last()) }
        expect : prefix.isCoveredBy(ordering)
        and    : !ordering.isCoveredBy(prefix)
        where  :
            prefix << someOrderings().take(10)
            lst    << Gen.list(Gen.tuple(Gen.string(~/[a-zA-Z]+/),
                    Gen.any(Order.ASCENDING, Order.DESCENDING, Order.ANY)).take(100), 1, 10).take(10)
    }

    def "Orderings that differs on a single variable should not cover each other" () {
        given  : def ordering1 = commonPrefix.clone()
                 def ordering2 = commonPrefix.clone()
                 ordering1.appendOrdering(var, Order.ASCENDING)
                 ordering2.appendOrdering(var, Order.DESCENDING)
                 suffix.forEach { v ->
                     ordering1.appendOrdering(v.first(), v.last())
                     ordering2.appendOrdering(v.first(), v.last())
                 }
        expect : !ordering1.isCoveredBy(ordering2)
        and    : !ordering2.isCoveredBy(ordering1)
        where  :
            commonPrefix << someOrderings().take(10)
            suffix       << Gen.list(Gen.tuple(Gen.string(~/[a-zA-Z]+/),
                    Gen.any(Order.ASCENDING, Order.DESCENDING, Order.ANY)).take(100), 1, 10).take(10)
            var          << Gen.string(~/[a-zA-Z]+[0-9]/).take(10)
    }

    def "Empty Ordering is covered by any Ordering" () {
        given  : def emptyOrdering = new Ordering()
        expect : emptyOrdering.isCoveredBy(ordering)
        where  : ordering << someOrderings()
    }

    def "Cloned Orderings should be equals" () {
        expect : ordering.equals(ordering.clone())
        where  : ordering << someOrderings()
    }

    def singletonOrdering(String var, Order o) {
        def ord = new Ordering()
        ord.appendOrdering(var, o)
        ord
    }

    def singletonOrderings() {
        def orderGen = Gen.any(Order.ASCENDING, Order.DESCENDING, Order.ANY)
        def varGen   = Gen.string(~/[a-zA-Z]+/)

        Gen.list(Gen.tuple(varGen, orderGen).take(100), 1, 1).map { it ->
            Ordering o = new Ordering()
            it.forEach { v -> o.appendOrdering(v.first(), v.last()) }
            o
        }
    }

    def someOrderings() {

        def orderGen = Gen.any(Order.ASCENDING, Order.DESCENDING, Order.ANY)
        def varGen   = Gen.string(~/[a-zA-Z]+/)

        Gen.list(Gen.tuple(varGen, orderGen).take(100), 0, 10).map { it ->
            Ordering o = new Ordering()
            it.forEach { v -> o.appendOrdering(v.first(), v.last()) }
            o
        }
    }


}
