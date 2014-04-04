package org.literacybridge.dashboard.aggregation;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by willpugh on 2/9/14.
 */
public class UpdateAggregations {
  public final Map<String, Aggregations> contentAggregations = new TreeMap();
  public final Map<String, Aggregations> perVillageAggregations = new TreeMap<>();
  public final Map<String, Aggregations> perTBAggregations = new TreeMap<>();

  public Map<String, Aggregations> getAggregationMap(Grouping grouping) {
    switch (grouping) {
      case contentId:
        return contentAggregations;
      case village:
        return perVillageAggregations;
      case talkingBook:
        return perTBAggregations;
      default:
        throw new IllegalArgumentException("Invalid AggregationBy parameter: " + grouping);
    }
  }

  public int add(AggregationOf aggregationOf, String contentId, String village, String talkingBook, int valsToAdd) {

    assureAggregations(perVillageAggregations, village).add(aggregationOf, valsToAdd);
    assureAggregations(perTBAggregations, talkingBook).add(aggregationOf, valsToAdd);

    return assureAggregations(contentAggregations, contentId).add(aggregationOf, valsToAdd);
  }

  public static Aggregations assureAggregations(Map<String, Aggregations> map, String id) {
    Aggregations aggregations = map.get(id);
    if (aggregations == null) {
      aggregations = new Aggregations();
      map.put(id, aggregations);
    }
    return aggregations;
  }
}
