package org.literacybridge.dashboard;

import org.literacybridge.dashboard.aggregation.*;
import org.literacybridge.stats.model.DeploymentId;

import javax.annotation.Nullable;
import java.util.*;

/**
 * @author willpugh
 */
public class ConsistencyChecker {
  public final StatAggregator statsAggregation1;
  public final StatAggregator statsAggregation2;


  public ConsistencyChecker(StatAggregator statsAggregation1, StatAggregator statsAggregation2) {
    this.statsAggregation1 = statsAggregation1;
    this.statsAggregation2 = statsAggregation2;
  }

  public static class ConsistencyRecord {
    public final int count1;
    public final int count2;

    ConsistencyRecord(int count1, int count2) {
      this.count1 = count1;
      this.count2 = count2;
    }
  }





  public Map<DeploymentId, Map<String, ConsistencyRecord>> findDisparities(Grouping grouping,
                                                                              AggregationOf aggregationOf,
                                                                              double threshold) {

    final Set<DeploymentId> allUpdates = new HashSet<>();
    allUpdates.addAll(statsAggregation1.perUpdateAggregations.keySet());
    allUpdates.addAll(statsAggregation2.perUpdateAggregations.keySet());

    final Map<DeploymentId, Map<String, ConsistencyRecord>> retVal = new HashMap<>();
    for (DeploymentId updateId : allUpdates) {
      final Map<String, ConsistencyRecord> updateDisparities = findDisparitiesForUpdate(updateId, grouping,
                                                                                        aggregationOf, threshold);
      if (!updateDisparities.isEmpty()) {
        retVal.put(updateId, updateDisparities);
      }
    }
    return retVal;
  }

  public Map<String, ConsistencyRecord> findDisparitiesForUpdate(DeploymentId deploymentId, Grouping grouping,
                                                                 AggregationOf aggregationOf, double threshold) {
    final UpdateAggregations updateAggregations1 = statsAggregation1.perUpdateAggregations.get(deploymentId);
    final UpdateAggregations updateAggregations2 = statsAggregation2.perUpdateAggregations.get(deploymentId);

    final Set<String> dimensionIds = getAggregationIds(grouping, updateAggregations1, updateAggregations2);

    final Map<String, ConsistencyRecord> retVal = new HashMap<>();
    for (String dimensionId : dimensionIds) {
      final int aggValue1 = getAggValue(grouping, dimensionId, aggregationOf, updateAggregations1);
      final int aggValue2 = getAggValue(grouping, dimensionId, aggregationOf, updateAggregations2);
      final ConsistencyRecord inconsistenRecord = checkForInconsistency(aggValue1, aggValue2, threshold);

      if (inconsistenRecord != null) {
        retVal.put(dimensionId, inconsistenRecord);
      }
    }

    return retVal;
  }


  private Set<String> getAggregationIds(Grouping grouping, UpdateAggregations updateAggregations,
                                        UpdateAggregations updateAggregationsFromStats) {
    final Set<String> retVal = new HashSet<>();
    if (updateAggregations != null) {
      retVal.addAll(updateAggregations.getAggregationMap(grouping).keySet());
    }

    if (updateAggregationsFromStats != null) {
      retVal.addAll(updateAggregationsFromStats.getAggregationMap(grouping).keySet());
    }

    return Collections.unmodifiableSet(retVal);
  }

  private Aggregations getAgg(Grouping grouping, String id, @Nullable UpdateAggregations updateAggregations) {
    return updateAggregations != null ? updateAggregations.getAggregationMap(grouping).get(id) : null;
  }


  private int getAggValue(Grouping grouping, String id, AggregationOf aggregationOf,
                          @Nullable UpdateAggregations updateAggregations) {
    Aggregations agg = getAgg(grouping, id, updateAggregations);
    return (agg != null) ? agg.get(aggregationOf) : 0;
  }


  private double calculateDisparity(int aggValue1, int aggValue2) {

    //In order to make sure we never divide by 0, we will swing large, but non-infinite differences, if the
    //reference value is 0, and the test values are not.
    if (aggValue1 == 0 ) {
      return aggValue2;
    }

    double aggDiff = Math.abs(((double) (aggValue1 - aggValue2)) / (double) aggValue1);
    return aggDiff;
  }

  @Nullable
  private ConsistencyRecord checkForInconsistency(int aggValue1, int aggValue2, double threshold) {

    double aggDisparity = calculateDisparity(aggValue1, aggValue2);
    if (aggDisparity >= threshold) {
      return new ConsistencyRecord(aggValue1, aggValue2);
    }

    return null;
  }


}
