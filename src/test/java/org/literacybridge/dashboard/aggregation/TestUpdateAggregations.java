package org.literacybridge.dashboard.aggregation;

import junit.framework.TestCase;
import org.junit.Test;

/**
 * Created by willpugh on 2/9/14.
 */
public class TestUpdateAggregations {

    @Test
    public void testEmptyAggregations() {
        UpdateAggregations  updateAggregations = new UpdateAggregations();
        TestCase.assertNotNull(updateAggregations.getAggregationMap(Grouping.contentId));
        TestCase.assertNotNull(updateAggregations.getAggregationMap(Grouping.talkingBook));
        TestCase.assertNotNull(updateAggregations.getAggregationMap(Grouping.village));
        TestCase.assertEquals(0, updateAggregations.getAggregationMap(Grouping.contentId).size());
        TestCase.assertEquals(0, updateAggregations.getAggregationMap(Grouping.talkingBook).size());
        TestCase.assertEquals(0, updateAggregations.getAggregationMap(Grouping.village).size());
    }

    @Test
    public void testGettingAndSettingAnUpdate() {
        UpdateAggregations  updateAggregations = new UpdateAggregations();
        updateAggregations.add(AggregationOf.tenSecondPlays, "TestContentId", "TestVillage", "TestTalkingBook", 1);
        TestCase.assertEquals(1, updateAggregations.getAggregationMap(Grouping.contentId).size());
        TestCase.assertEquals(1, updateAggregations.getAggregationMap(Grouping.talkingBook).size());
        TestCase.assertEquals(1, updateAggregations.getAggregationMap(Grouping.village).size());

        TestCase.assertEquals(1,updateAggregations.getAggregationMap(Grouping.contentId).get("TestContentId").get(AggregationOf.tenSecondPlays));
        TestCase.assertEquals(1, updateAggregations.getAggregationMap(Grouping.talkingBook).get("TestTalkingBook").get(AggregationOf.tenSecondPlays));
        TestCase.assertEquals(1, updateAggregations.getAggregationMap(Grouping.village).get("TestVillage").get(AggregationOf.tenSecondPlays));
        TestCase.assertEquals(0, updateAggregations.getAggregationMap(Grouping.village).get("TestVillage").get(AggregationOf.corruptedFiles));
    }

    @Test
    public void testMultipleGettingAndSetting() {
        UpdateAggregations  updateAggregations = new UpdateAggregations();
        updateAggregations.add(AggregationOf.tenSecondPlays, "TestContentId", "TestVillage", "TestTalkingBook", 100);
        updateAggregations.add(AggregationOf.tenSecondPlays, "TestContentId", "TestVillage", "TestTalkingBook2", 200);
        updateAggregations.add(AggregationOf.tenSecondPlays, "TestContentId", "TestVillage2", "TestTalkingBook3", 300);
        updateAggregations.add(AggregationOf.tenSecondPlays, "TestContentId2", "TestVillage", "TestTalkingBook3", 400);


        TestCase.assertEquals(2, updateAggregations.getAggregationMap(Grouping.contentId).size());
        TestCase.assertEquals(3, updateAggregations.getAggregationMap(Grouping.talkingBook).size());
        TestCase.assertEquals(2, updateAggregations.getAggregationMap(Grouping.village).size());

        TestCase.assertEquals(600,updateAggregations.getAggregationMap(Grouping.contentId).get("TestContentId").get(AggregationOf.tenSecondPlays));
        TestCase.assertEquals(400,updateAggregations.getAggregationMap(Grouping.contentId).get("TestContentId2").get(AggregationOf.tenSecondPlays));

        TestCase.assertEquals(100,updateAggregations.getAggregationMap(Grouping.talkingBook).get("TestTalkingBook").get(AggregationOf.tenSecondPlays));
        TestCase.assertEquals(200,updateAggregations.getAggregationMap(Grouping.talkingBook).get("TestTalkingBook2").get(AggregationOf.tenSecondPlays));
        TestCase.assertEquals(700,updateAggregations.getAggregationMap(Grouping.talkingBook).get("TestTalkingBook3").get(AggregationOf.tenSecondPlays));

        TestCase.assertEquals(700,updateAggregations.getAggregationMap(Grouping.village).get("TestVillage").get(AggregationOf.tenSecondPlays));
        TestCase.assertEquals(300,updateAggregations.getAggregationMap(Grouping.village).get("TestVillage2").get(AggregationOf.tenSecondPlays));

    }

    @Test
    public void testMultipleAggregations() {
        UpdateAggregations  updateAggregations = new UpdateAggregations();
        updateAggregations.add(AggregationOf.tenSecondPlays,  "TestContentId", "TestVillage", "TestTalkingBook", 100);
        updateAggregations.add(AggregationOf.finishedPlays, "TestContentId", "TestVillage", "TestTalkingBook", 200);
        updateAggregations.add(AggregationOf.surveyTaken,   "TestContentId", "TestVillage", "TestTalkingBook", 300);


        TestCase.assertEquals(1, updateAggregations.getAggregationMap(Grouping.contentId).size());
        TestCase.assertEquals(1, updateAggregations.getAggregationMap(Grouping.talkingBook).size());
        TestCase.assertEquals(1, updateAggregations.getAggregationMap(Grouping.village).size());

        TestCase.assertEquals(100,updateAggregations.getAggregationMap(Grouping.contentId).get("TestContentId").get(AggregationOf.tenSecondPlays));
        TestCase.assertEquals(200,updateAggregations.getAggregationMap(Grouping.contentId).get("TestContentId").get(AggregationOf.finishedPlays));
        TestCase.assertEquals(300,updateAggregations.getAggregationMap(Grouping.contentId).get("TestContentId").get(AggregationOf.surveyTaken));

        TestCase.assertEquals(100,updateAggregations.getAggregationMap(Grouping.talkingBook).get("TestTalkingBook").get(AggregationOf.tenSecondPlays));
        TestCase.assertEquals(200,updateAggregations.getAggregationMap(Grouping.talkingBook).get("TestTalkingBook").get(AggregationOf.finishedPlays));
        TestCase.assertEquals(300,updateAggregations.getAggregationMap(Grouping.talkingBook).get("TestTalkingBook").get(AggregationOf.surveyTaken));

        TestCase.assertEquals(100,updateAggregations.getAggregationMap(Grouping.village).get("TestVillage").get(AggregationOf.tenSecondPlays));
        TestCase.assertEquals(200,updateAggregations.getAggregationMap(Grouping.village).get("TestVillage").get(AggregationOf.finishedPlays));
        TestCase.assertEquals(300,updateAggregations.getAggregationMap(Grouping.village).get("TestVillage").get(AggregationOf.surveyTaken));
    }


}
