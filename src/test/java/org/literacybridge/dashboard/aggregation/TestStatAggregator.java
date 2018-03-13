package org.literacybridge.dashboard.aggregation;

import junit.framework.TestCase;
import org.junit.Test;
import org.literacybridge.stats.model.DeploymentId;

/**
 * Created by willpugh on 2/9/14.
 */
public class TestStatAggregator {

  public static final DeploymentId UPDATE_ID_1                    = DeploymentId.parseContentUpdate("2012-4");
  public static final DeploymentId UPDATE_ID_1_DIFFERENT_INSTANCE = DeploymentId.parseContentUpdate("2012-4");
  public static final DeploymentId UPDATE_ID_2                    = DeploymentId.parseContentUpdate("2012-5");

  @Test
  public void testEmptyStatAggregator() {
    StatAggregator statAggregator = new StatAggregator();
    TestCase.assertNotNull(statAggregator.perUpdateAggregations);
    TestCase.assertEquals(0, statAggregator.perUpdateAggregations.size());
  }

  @Test
    public void testAddingStats() {
        StatAggregator statAggregator = new StatAggregator();
        statAggregator.add(UPDATE_ID_1, AggregationOf.tenSecondPlays, "TestContentId", "TestVillage", "TestTalkingBook", 3);
        TestCase.assertEquals(1, statAggregator.perUpdateAggregations.size());
        TestCase.assertEquals(3, statAggregator.perUpdateAggregations.get(UPDATE_ID_1).getAggregationMap(Grouping.contentId).get("TestContentId").get(
            AggregationOf.tenSecondPlays));

        statAggregator.add(UPDATE_ID_1_DIFFERENT_INSTANCE, AggregationOf.tenSecondPlays, "TestContentId", "TestVillage2", "TestTalkingBook2", 3);
        TestCase.assertEquals(1, statAggregator.perUpdateAggregations.size());
        TestCase.assertEquals(6, statAggregator.perUpdateAggregations.get(UPDATE_ID_1).getAggregationMap(Grouping.contentId).get("TestContentId").get(
            AggregationOf.tenSecondPlays));
        TestCase.assertEquals(3, statAggregator.perUpdateAggregations.get(UPDATE_ID_1).getAggregationMap(Grouping.village).get("TestVillage2").get(
            AggregationOf.tenSecondPlays));


        statAggregator.add(UPDATE_ID_2, AggregationOf.tenSecondPlays, "TestContentId", "TestVillage", "TestTalkingBook", 5);
        TestCase.assertEquals(2, statAggregator.perUpdateAggregations.size());
        TestCase.assertEquals(5, statAggregator.perUpdateAggregations.get(UPDATE_ID_2).getAggregationMap(Grouping.talkingBook).get("TestTalkingBook").get(
            AggregationOf.tenSecondPlays));

    }

}
