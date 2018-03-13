package org.literacybridge;

import junit.framework.TestCase;
import org.junit.Test;
import org.literacybridge.main.ConsistencyChecker;
import org.literacybridge.dashboard.aggregation.AggregationOf;
import org.literacybridge.dashboard.aggregation.Grouping;
import org.literacybridge.dashboard.aggregation.StatAggregator;
import org.literacybridge.stats.model.DeploymentId;

import java.util.Map;

/**
 * Created by willpugh on 2/9/14.
 */
public class TestConsistencyChecker {

  public static DeploymentId UPDATE_2012_1 = DeploymentId.parseContentUpdate("2012-1");
  public static DeploymentId UPDATE_2013_1 = DeploymentId.parseContentUpdate("2013-1");
  public static DeploymentId UPDATE_2013_2 = DeploymentId.parseContentUpdate("2013-2");

  public static final String CONTENT_1 = "TestContent1";
  public static final String CONTENT_2 = "TestContent2";

  public static final String VILLAGE_1 = "TestVillage1";
  public static final String VILLAGE_2 = "TestVillage2";

  public static final String TB_1 = "TalkingBook1";
  public static final String TB_2 = "TalkingBook2";


  @Test
  public void testEmptyCheck() {
    StatAggregator stats1 = new StatAggregator();
    StatAggregator stats2 = new StatAggregator();

    ConsistencyChecker checker = new ConsistencyChecker(stats1, stats2);
    Map disparities = checker.findDisparitiesForUpdate(UPDATE_2012_1, Grouping.contentId, AggregationOf.tenSecondPlays,
                                                       0);
    TestCase.assertEquals(0, disparities.size());

    disparities = checker.findDisparitiesForUpdate(UPDATE_2012_1, Grouping.contentId, AggregationOf.tenSecondPlays, 100);
        TestCase.assertEquals(0, disparities.size());

        Map allDisparities = checker.findDisparities(Grouping.contentId, AggregationOf.tenSecondPlays, 0);
        TestCase.assertEquals(0, allDisparities.size());
    }

    @Test
    public void testContentDisparitiesNoChanges() {
        StatAggregator  stats1 = new StatAggregator();
        StatAggregator  stats2 = new StatAggregator();

        stats1.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_1, VILLAGE_1, TB_1, 7);
        stats2.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_1, VILLAGE_1, TB_1, 7);

        stats1.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_2, VILLAGE_2, TB_2, 9);
        stats2.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_2, VILLAGE_2, TB_2, 9);


        //Verify that with identical counts, no disparities are found if threshold is non-zero
        ConsistencyChecker checker = new ConsistencyChecker(stats1, stats2);
        Map<String, ConsistencyChecker.ConsistencyRecord>   disparities = checker.findDisparitiesForUpdate(UPDATE_2012_1, Grouping.contentId, AggregationOf.tenSecondPlays, .001);
        TestCase.assertEquals(0, disparities.size());


        //Verify that with identical counts, disparities ARE found if threshold is zero
        disparities = checker.findDisparitiesForUpdate(UPDATE_2012_1, Grouping.contentId, AggregationOf.tenSecondPlays, 0d);
        TestCase.assertEquals(2, disparities.size());
        TestCase.assertEquals(7, disparities.get(CONTENT_1).count1);
        TestCase.assertEquals(7, disparities.get(CONTENT_1).count2);
        TestCase.assertEquals(9, disparities.get(CONTENT_2).count1);
        TestCase.assertEquals(9, disparities.get(CONTENT_2).count2);

    }

    @Test
    public void testAsymetricDisparities() {
        StatAggregator  stats1 = new StatAggregator();
        StatAggregator  stats2 = new StatAggregator();

        stats1.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_1, VILLAGE_1, TB_1, 7);
        stats2.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_2, VILLAGE_2, TB_2, 9);


        //Verify that with identical counts, no disparities are found if threshold is non-zero
        ConsistencyChecker checker = new ConsistencyChecker(stats1, stats2);
        Map<String, ConsistencyChecker.ConsistencyRecord>   disparities = checker.findDisparitiesForUpdate(UPDATE_2012_1, Grouping.contentId, AggregationOf.tenSecondPlays, .001);
        TestCase.assertEquals(2, disparities.size());
        TestCase.assertEquals(7, disparities.get(CONTENT_1).count1);
        TestCase.assertEquals(0, disparities.get(CONTENT_1).count2);
        TestCase.assertEquals(0, disparities.get(CONTENT_2).count1);
        TestCase.assertEquals(9, disparities.get(CONTENT_2).count2);

        //Verify that with identical counts, disparities ARE found if threshold is zero
        disparities = checker.findDisparitiesForUpdate(UPDATE_2012_1, Grouping.contentId, AggregationOf.tenSecondPlays, 1d);
        TestCase.assertEquals(2, disparities.size());
        TestCase.assertEquals(7, disparities.get(CONTENT_1).count1);
        TestCase.assertEquals(0, disparities.get(CONTENT_1).count2);
        TestCase.assertEquals(0, disparities.get(CONTENT_2).count1);
        TestCase.assertEquals(9, disparities.get(CONTENT_2).count2);

    }


    @Test
    public void testDifferentDimensions() {
        StatAggregator  stats1 = new StatAggregator();
        StatAggregator  stats2 = new StatAggregator();

        stats1.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_1, VILLAGE_1, TB_1, 10);
        stats2.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_1, VILLAGE_1, TB_1, 9);

        stats1.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_2, VILLAGE_2, TB_2, 10);
        stats2.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_2, VILLAGE_2, TB_2, 9);

        ConsistencyChecker checker = new ConsistencyChecker(stats1, stats2);

        Map<String, ConsistencyChecker.ConsistencyRecord>   disparities;

        //Verify Grouping.contentId works properly
        disparities = checker.findDisparitiesForUpdate(UPDATE_2012_1, Grouping.contentId, AggregationOf.tenSecondPlays, .001);
        TestCase.assertEquals(2, disparities.size());
        TestCase.assertEquals(10, disparities.get(CONTENT_1).count1);
        TestCase.assertEquals(9, disparities.get(CONTENT_1).count2);
        TestCase.assertEquals(10, disparities.get(CONTENT_2).count1);
        TestCase.assertEquals(9, disparities.get(CONTENT_2).count2);


        //Verify Grouping.contentId works properly
        disparities = checker.findDisparitiesForUpdate(UPDATE_2012_1, Grouping.talkingBook, AggregationOf.tenSecondPlays, .001);
        TestCase.assertEquals(2, disparities.size());
        TestCase.assertEquals(10, disparities.get(TB_1).count1);
        TestCase.assertEquals(9, disparities.get(TB_1).count2);
        TestCase.assertEquals(10, disparities.get(TB_2).count1);
        TestCase.assertEquals(9, disparities.get(TB_2).count2);


        //Verify Grouping.village works properly
        disparities = checker.findDisparitiesForUpdate(UPDATE_2012_1, Grouping.village, AggregationOf.tenSecondPlays, .001);
        TestCase.assertEquals(2, disparities.size());
        TestCase.assertEquals(10, disparities.get(VILLAGE_1).count1);
        TestCase.assertEquals(9, disparities.get(VILLAGE_1).count2);
        TestCase.assertEquals(10, disparities.get(VILLAGE_2).count1);
        TestCase.assertEquals(9, disparities.get(VILLAGE_2).count2);

    }

    @Test
    public void testDifferentThresholds () {

        StatAggregator  stats1 = new StatAggregator();
        StatAggregator  stats2 = new StatAggregator();

        stats1.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_1, VILLAGE_1, TB_1, 10);
        stats2.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_1, VILLAGE_1, TB_1, 9);

        stats1.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_2, VILLAGE_2, TB_2, 10);
        stats2.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_2, VILLAGE_2, TB_2, 8);

        ConsistencyChecker checker = new ConsistencyChecker(stats1, stats2);
        Map<String, ConsistencyChecker.ConsistencyRecord>   disparities;

        //Small threshold, everything should be caught
        disparities = checker.findDisparitiesForUpdate(UPDATE_2012_1, Grouping.talkingBook, AggregationOf.tenSecondPlays, .001);
        TestCase.assertEquals(2, disparities.size());
        TestCase.assertEquals(10, disparities.get(TB_1).count1);
        TestCase.assertEquals(9, disparities.get(TB_1).count2);
        TestCase.assertEquals(10, disparities.get(TB_2).count1);
        TestCase.assertEquals(8, disparities.get(TB_2).count2);


        //Medium threshold, Only 20% differences should be caught
        disparities = checker.findDisparitiesForUpdate(UPDATE_2012_1, Grouping.talkingBook, AggregationOf.tenSecondPlays, .11);
        TestCase.assertEquals(1, disparities.size());
        TestCase.assertEquals(10, disparities.get(TB_2).count1);
        TestCase.assertEquals(8, disparities.get(TB_2).count2);

        //20% threshold, Only 20% differences should be caught
        disparities = checker.findDisparitiesForUpdate(UPDATE_2012_1, Grouping.talkingBook, AggregationOf.tenSecondPlays, .20);
        TestCase.assertEquals(1, disparities.size());
        TestCase.assertEquals(10, disparities.get(TB_2).count1);
        TestCase.assertEquals(8, disparities.get(TB_2).count2);


        //Large threshold, nothing should be caught
        disparities = checker.findDisparitiesForUpdate(UPDATE_2012_1, Grouping.talkingBook, AggregationOf.tenSecondPlays, .21);
        TestCase.assertEquals(0, disparities.size());
    }

    @Test
    public void testAsymtricAllDisparities() {

        StatAggregator  stats1 = new StatAggregator();
        StatAggregator  stats2 = new StatAggregator();

        stats1.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_1, VILLAGE_1, TB_1, 10);
        stats2.add(UPDATE_2013_1, AggregationOf.tenSecondPlays, CONTENT_1, VILLAGE_1, TB_1, 9);


        ConsistencyChecker checker = new ConsistencyChecker(stats1, stats2);
        Map<DeploymentId, Map<String, ConsistencyChecker.ConsistencyRecord>>   disparities;

        //Small threshold, everything should be caught
        disparities = checker.findDisparities(Grouping.talkingBook, AggregationOf.tenSecondPlays, .001);
        TestCase.assertEquals(2,  disparities.size());
        TestCase.assertEquals(1,  disparities.get(UPDATE_2012_1).size());
        TestCase.assertEquals(1,  disparities.get(UPDATE_2013_1).size());

        TestCase.assertEquals(10, disparities.get(UPDATE_2012_1).get(TB_1).count1);
        TestCase.assertEquals(0,  disparities.get(UPDATE_2012_1).get(TB_1).count2);
        TestCase.assertEquals(0,  disparities.get(UPDATE_2013_1).get(TB_1).count1);
        TestCase.assertEquals(9,  disparities.get(UPDATE_2013_1).get(TB_1).count2);

    }

    @Test
    public void testDifferentThresholdsAllDisparities() {

        StatAggregator  stats1 = new StatAggregator();
        StatAggregator  stats2 = new StatAggregator();

        stats1.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_1, VILLAGE_1, TB_1, 10);
        stats2.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_1, VILLAGE_1, TB_1, 9);

        stats1.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_2, VILLAGE_2, TB_2, 10);
        stats2.add(UPDATE_2012_1, AggregationOf.tenSecondPlays, CONTENT_2, VILLAGE_2, TB_2, 8);

        stats1.add(UPDATE_2013_1, AggregationOf.tenSecondPlays, CONTENT_1, VILLAGE_1, TB_1, 100);
        stats2.add(UPDATE_2013_1, AggregationOf.tenSecondPlays, CONTENT_1, VILLAGE_1, TB_1, 90);

        stats1.add(UPDATE_2013_1, AggregationOf.tenSecondPlays, CONTENT_2, VILLAGE_2, TB_2, 100);
        stats2.add(UPDATE_2013_1, AggregationOf.tenSecondPlays, CONTENT_2, VILLAGE_2, TB_2, 80);

        stats1.add(UPDATE_2013_2, AggregationOf.tenSecondPlays, CONTENT_1, VILLAGE_1, TB_1, 1000);
        stats2.add(UPDATE_2013_2, AggregationOf.tenSecondPlays, CONTENT_1, VILLAGE_1, TB_1, 900);

        stats1.add(UPDATE_2013_2, AggregationOf.tenSecondPlays, CONTENT_2, VILLAGE_2, TB_2, 1000);
        stats2.add(UPDATE_2013_2, AggregationOf.tenSecondPlays, CONTENT_2, VILLAGE_2, TB_2, 1000);


        ConsistencyChecker checker = new ConsistencyChecker(stats1, stats2);
        Map<DeploymentId, Map<String, ConsistencyChecker.ConsistencyRecord>>   disparities;

        //Small threshold, everything should be caught
        disparities = checker.findDisparities(Grouping.village, AggregationOf.tenSecondPlays, .001);
        TestCase.assertEquals(3,  disparities.size());
        TestCase.assertEquals(2,  disparities.get(UPDATE_2012_1).size());
        TestCase.assertEquals(2,  disparities.get(UPDATE_2013_1).size());
        TestCase.assertEquals(1,  disparities.get(UPDATE_2013_2).size());

        TestCase.assertEquals(10, disparities.get(UPDATE_2012_1).get(VILLAGE_1).count1);
        TestCase.assertEquals(9,  disparities.get(UPDATE_2012_1).get(VILLAGE_1).count2);
        TestCase.assertEquals(10, disparities.get(UPDATE_2012_1).get(VILLAGE_2).count1);
        TestCase.assertEquals(8,  disparities.get(UPDATE_2012_1).get(VILLAGE_2).count2);

        TestCase.assertEquals(100, disparities.get(UPDATE_2013_1).get(VILLAGE_1).count1);
        TestCase.assertEquals(90,  disparities.get(UPDATE_2013_1).get(VILLAGE_1).count2);
        TestCase.assertEquals(100, disparities.get(UPDATE_2013_1).get(VILLAGE_2).count1);
        TestCase.assertEquals(80,  disparities.get(UPDATE_2013_1).get(VILLAGE_2).count2);

        TestCase.assertEquals(1000, disparities.get(UPDATE_2013_2).get(VILLAGE_1).count1);
        TestCase.assertEquals(900,  disparities.get(UPDATE_2013_2).get(VILLAGE_1).count2);

        //Medium threshold, Only 20% differences should be caught
        disparities = checker.findDisparities(Grouping.village, AggregationOf.tenSecondPlays, .11);
        TestCase.assertEquals(2,  disparities.size());
        TestCase.assertEquals(1,  disparities.get(UPDATE_2012_1).size());
        TestCase.assertEquals(1,  disparities.get(UPDATE_2013_1).size());

        TestCase.assertEquals(10, disparities.get(UPDATE_2012_1).get(VILLAGE_2).count1);
        TestCase.assertEquals(8,  disparities.get(UPDATE_2012_1).get(VILLAGE_2).count2);

        TestCase.assertEquals(100, disparities.get(UPDATE_2013_1).get(VILLAGE_2).count1);
        TestCase.assertEquals(80,  disparities.get(UPDATE_2013_1).get(VILLAGE_2).count2);

        //Large threshold, nothing should be caught
        disparities = checker.findDisparities(Grouping.talkingBook, AggregationOf.tenSecondPlays, .21);
        TestCase.assertEquals(0, disparities.size());

    }
}
