package org.literacybridge.dashboard.aggregation;

import org.literacybridge.stats.model.DeploymentId;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class StatAggregator {

  public final Map<DeploymentId, UpdateAggregations> perUpdateAggregations = new HashMap<>();

  public int add(DeploymentId update, AggregationOf aggregationOf, String contentId, String village,
                 String talkingBook, int valsToAdd) {
    return assureUpdate(update).add(aggregationOf, contentId, village, talkingBook, valsToAdd);
  }

  public void clear() {
    perUpdateAggregations.clear();
  }

  public boolean isEmpty() {
    return perUpdateAggregations.isEmpty();
  }

  private UpdateAggregations assureUpdate(DeploymentId updateId) {
    UpdateAggregations aggregations = perUpdateAggregations.get(updateId);
    if (aggregations == null) {
      aggregations = new UpdateAggregations();
      perUpdateAggregations.put(updateId, aggregations);
    }
    return aggregations;
  }

}
