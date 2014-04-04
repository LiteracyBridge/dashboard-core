package org.literacybridge;

import com.google.common.collect.Lists;
import junit.framework.TestCase;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;
import org.literacybridge.dashboard.FullSyncher;
import org.literacybridge.dashboard.aggregation.StatAggregator;
import org.literacybridge.dashboard.api.TalkingBookSyncWriter;
import org.literacybridge.dashboard.model.contentUsage.SyncAggregation;
import org.literacybridge.stats.model.events.PlayedEvent;
import org.literacybridge.dashboard.model.syncOperations.SyncOperationLog;
import org.literacybridge.dashboard.model.syncOperations.TalkingBookCorruption;

import java.io.File;

/**
 * Created by willpugh on 2/10/14.
 */
public class TestFullSyncher {

  //"core" + File.separator + "src" + File.separator + "test" + File.separator + "resources" + File.separator + "testSyncDir" + File.separator + "collected-data"
  public static final File SyncRoot = new File("src/test/resources".replace('/', File.separatorChar));

  @Test
  public void testSyncherWrongDir() throws Exception {
    final File invalidDir = SyncRoot;

    final TalkingBookSyncWriter eventWriter = EasyMock.createMock(TalkingBookSyncWriter.class);

    eventWriter.writeOperationLog(EasyMock.anyObject(SyncOperationLog.class));
    EasyMock.replay(eventWriter);

    FullSyncher syncher = new FullSyncher(0, .1, Lists.newArrayList(eventWriter));
    try {
      syncher.processData(invalidDir);
      TestCase.fail("processData should fail, since the fir is empty");
    } catch (IllegalArgumentException e) {
      //Pass
    }

    StatAggregator statAggregator = syncher.doConsistencyCheck();
    TestCase.assertEquals(0, statAggregator.perUpdateAggregations.size());

    EasyMock.verify(eventWriter, eventWriter);
  }

  @Test
  public void testSyncherNoDisparities() throws Exception {

    final TalkingBookSyncWriter eventWriter = EasyMock.createMock(TalkingBookSyncWriter.class);
    final Capture<SyncOperationLog> finishedMessageCapture = new Capture<SyncOperationLog>();


    eventWriter.writeAggregation(EasyMock.anyObject(SyncAggregation.class));
    EasyMock.expectLastCall().times(3);

    eventWriter.writePlayEvent(EasyMock.anyObject(PlayedEvent.class));
    EasyMock.expectLastCall().times(4);

    eventWriter.writeTalkingBookCorruption(EasyMock.anyObject(TalkingBookCorruption.class));
    eventWriter.writeOperationLog(EasyMock.capture(finishedMessageCapture));

    EasyMock.replay(eventWriter);

    FullSyncher syncher = new FullSyncher(0, 1d, Lists.newArrayList(eventWriter));
    syncher.processData(new File(SyncRoot, "testSyncDir"));
    StatAggregator statAggregator = syncher.doConsistencyCheck();
    TestCase.assertEquals(1, statAggregator.perUpdateAggregations.size());

    EasyMock.verify(eventWriter);
    TestCase.assertEquals(SyncOperationLog.OPERATION_SYNC, finishedMessageCapture.getValue().getOperation());
    TestCase.assertEquals(SyncOperationLog.NORMAL, finishedMessageCapture.getValue().getLogType());

  }



  @Test
  public void testSyncherWithDisparities() throws Exception {

    final TalkingBookSyncWriter eventWriter = EasyMock.createMock(TalkingBookSyncWriter.class);
    final Capture<SyncOperationLog> disparityMessageCapture = new Capture<SyncOperationLog>();
    final Capture<SyncOperationLog> finishedMessageCapture = new Capture<SyncOperationLog>();

    eventWriter.writeAggregation(EasyMock.anyObject(SyncAggregation.class));
    EasyMock.expectLastCall().times(2);


    eventWriter.writePlayEvent(EasyMock.anyObject(PlayedEvent.class));
    EasyMock.expectLastCall().times(5);

    eventWriter.writeTalkingBookCorruption(EasyMock.anyObject(TalkingBookCorruption.class));
    eventWriter.writeOperationLog(EasyMock.capture(disparityMessageCapture));
    eventWriter.writeOperationLog(EasyMock.capture(finishedMessageCapture));

    EasyMock.replay(eventWriter);

    FullSyncher syncher = new FullSyncher(0, .001, Lists.newArrayList(eventWriter));
    syncher.processData(new File(SyncRoot, "testSyncDirDisparities"));
    StatAggregator statAggregator = syncher.doConsistencyCheck();
    TestCase.assertEquals(1, statAggregator.perUpdateAggregations.size());

    EasyMock.verify(eventWriter);

    TestCase.assertEquals(SyncOperationLog.OPERATION_VALIDATION, disparityMessageCapture.getValue().getOperation());
    TestCase.assertEquals(SyncOperationLog.WARNING, disparityMessageCapture.getValue().getLogType());

    TestCase.assertEquals(SyncOperationLog.OPERATION_SYNC, finishedMessageCapture.getValue().getOperation());
    TestCase.assertEquals(SyncOperationLog.NORMAL, finishedMessageCapture.getValue().getLogType());

  }

  @Test
  public void testSyncherWithFlashData() throws Exception {

    final TalkingBookSyncWriter eventWriter = EasyMock.createMock(TalkingBookSyncWriter.class);
    final Capture<SyncOperationLog> disparityMessageCapture = new Capture<SyncOperationLog>();
    final Capture<SyncOperationLog> finishedMessageCapture = new Capture<SyncOperationLog>();

    eventWriter.writeAggregation(EasyMock.anyObject(SyncAggregation.class));
    EasyMock.expectLastCall().times(16);


    eventWriter.writePlayEvent(EasyMock.anyObject(PlayedEvent.class));
    EasyMock.expectLastCall().times(5);

    eventWriter.writeTalkingBookCorruption(EasyMock.anyObject(TalkingBookCorruption.class));
    eventWriter.writeOperationLog(EasyMock.capture(disparityMessageCapture));
    EasyMock.expectLastCall().times(11);

    eventWriter.writeOperationLog(EasyMock.capture(finishedMessageCapture));

    EasyMock.replay(eventWriter);

    FullSyncher syncher = new FullSyncher(0, .001, Lists.newArrayList(eventWriter));
    syncher.processData(new File(SyncRoot, "testSyncDirFlashData"));
    StatAggregator statAggregator = syncher.doConsistencyCheck();
    TestCase.assertEquals(1, statAggregator.perUpdateAggregations.size());

    EasyMock.verify(eventWriter);

    TestCase.assertEquals(SyncOperationLog.OPERATION_VALIDATION, disparityMessageCapture.getValue().getOperation());
    TestCase.assertEquals(SyncOperationLog.WARNING, disparityMessageCapture.getValue().getLogType());

    TestCase.assertEquals(SyncOperationLog.OPERATION_SYNC, finishedMessageCapture.getValue().getOperation());
    TestCase.assertEquals(SyncOperationLog.NORMAL, finishedMessageCapture.getValue().getLogType());

  }
}
