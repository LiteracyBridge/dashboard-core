package org.literacybridge.dashboard.aggregation;

import junit.framework.TestCase;
import org.junit.Test;

/**
 * Created by willpugh on 2/9/14.
 */
public class TestAggregations {

    @Test
    public void testEmptyAggregation() {
        Aggregations    aggregations = new Aggregations();
        TestCase.assertEquals(0, aggregations.aggregations.size());
        TestCase.assertEquals(0, aggregations.get(AggregationOf.tenSecondPlays));
    }

    @Test
    public void testSimpleAggregation() {
        Aggregations    aggregations = new Aggregations();
        aggregations.add(AggregationOf.tenSecondPlays, 1);
        TestCase.assertEquals(1, aggregations.aggregations.size());
        TestCase.assertEquals(1, aggregations.get(AggregationOf.tenSecondPlays));

        aggregations.add(AggregationOf.tenSecondPlays, 2);
        TestCase.assertEquals(1, aggregations.aggregations.size());
        TestCase.assertEquals(3, aggregations.get(AggregationOf.tenSecondPlays));
    }

    @Test
    public void testMultipleAggregation() {
        Aggregations    aggregations = new Aggregations();
        aggregations.add(AggregationOf.tenSecondPlays, 3);
        TestCase.assertEquals(1, aggregations.aggregations.size());
        TestCase.assertEquals(3, aggregations.get(AggregationOf.tenSecondPlays));

        aggregations.add(AggregationOf.finishedPlays, 1);
        TestCase.assertEquals(2, aggregations.aggregations.size());
        TestCase.assertEquals(1, aggregations.get(AggregationOf.finishedPlays));
        TestCase.assertEquals(3, aggregations.get(AggregationOf.tenSecondPlays));

    }

}
