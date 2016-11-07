package org.semagrow.plan

import spock.lang.Specification

/**
 * Created by angel on 29/8/2016.
 */
class OrderSpec extends Specification {

    def "ASC, DESC and ANY is ordered" () {
        expect : order.isOrdered()
        where  : order << [Order.ANY, Order.ASCENDING, Order.DESCENDING]
    }

    def "NONE is not Ordered" () {
        expect : ! order.isOrdered()
        where  : order << Order.NONE
    }

    def "Order ANY covers everything" () {
        expect : order.isCoveredBy(Order.ANY)
        where  : order << [Order.ANY, Order.NONE, Order.ASCENDING, Order.DESCENDING]
    }

    def "Order NONE is covered by everything" () {
        expect : Order.NONE.isCoveredBy(order)
        where  : order << [Order.ANY, Order.NONE, Order.ASCENDING, Order.DESCENDING]
    }

    def "Order o covers itself" () {
        expect : order.isCoveredBy(order)
        where  : order << [Order.ANY, Order.NONE, Order.ASCENDING, Order.DESCENDING]
    }

    def "Order ASC and DESC do not cover each other" () {
        expect :
            !Order.ASCENDING.isCoveredBy(Order.DESCENDING)
            !Order.DESCENDING.isCoveredBy(Order.ASCENDING)
    }
}
