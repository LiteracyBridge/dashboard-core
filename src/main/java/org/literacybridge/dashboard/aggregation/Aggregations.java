package org.literacybridge.dashboard.aggregation;

import org.apache.commons.lang.mutable.MutableInt;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by willpugh on 2/9/14.
 */
public class Aggregations {
  final Map<AggregationOf, MutableInt> aggregations = new HashMap<>();

  public int add(AggregationOf aggregationOf, int i) {
    MutableInt mutableInt = assureAgg(aggregationOf);
    mutableInt.add(i);
    return mutableInt.intValue();
  }

  public int get(AggregationOf aggregationOf) {
    MutableInt retVal = aggregations.get(aggregationOf);
    return retVal != null ? retVal.intValue() : 0;
  }

  private MutableInt assureAgg(AggregationOf aggregationOf) {
    MutableInt retVal = aggregations.get(aggregationOf);
    if (retVal == null) {
      retVal = new MutableInt();
      aggregations.put(aggregationOf, retVal);
    }

    return retVal;
  }
}
