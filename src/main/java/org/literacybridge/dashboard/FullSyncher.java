package org.literacybridge.dashboard;

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.literacybridge.dashboard.aggregation.AggregationOf;
import org.literacybridge.dashboard.aggregation.Grouping;
import org.literacybridge.dashboard.aggregation.StatAggregator;
import org.literacybridge.dashboard.api.TalkingBookSyncWriter;
import org.literacybridge.stats.api.DirectoryCallbacks;
import org.literacybridge.stats.api.TalkingBookDataProcessor;
import org.literacybridge.stats.formats.syncDirectory.DirectoryProcessor;
import org.literacybridge.stats.model.DeploymentId;
import org.literacybridge.dashboard.model.syncOperations.SyncOperationLog;
import org.literacybridge.dashboard.processors.*;
import org.literacybridge.stats.DirectoryIterator;
import org.literacybridge.stats.model.DirectoryFormat;
import org.literacybridge.stats.processors.FilteringProcessor;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by willpugh on 2/10/14.
 */
public class FullSyncher {

  public static final int MIN_SECONDS_FOR_MIN_PLAY = 10;

  public final  double                            consistencyThreshold;
  public final  long                              updateId;
  public final  Collection<TalkingBookSyncWriter> eventWriters;
  private final AggregationProcessor              aggregationProcessor;
  private final DbPersistenceProcessor            dbPersistenceProcessor;
  private final List<TalkingBookDataProcessor>    processors;
  final         DirectoryProcessor                directoryProcessor;


  public FullSyncher(long updateId, double consistencyThreshold, Collection<TalkingBookSyncWriter> eventWriters) {
    this.consistencyThreshold = consistencyThreshold;
    this.eventWriters = eventWriters;

    dbPersistenceProcessor = new DbPersistenceProcessor(eventWriters);
    aggregationProcessor = new AggregationProcessor(MIN_SECONDS_FOR_MIN_PLAY);
    processors = ImmutableList.<TalkingBookDataProcessor>of(dbPersistenceProcessor, aggregationProcessor);
    directoryProcessor = new DirectoryProcessor(processors, DirectoryProcessor.CATEGORY_MAP);
    this.updateId = updateId;
  }

  public void processData(File syncRoot) throws Exception {
    processData(syncRoot, null, false);
  }

  public void processData(File syncRoot, DirectoryFormat format, boolean strict) throws Exception {

    doProcessData(directoryProcessor, syncRoot, format, strict);

    /*
    if (!syncRoot.isDirectory()) {
      throw new IllegalArgumentException("SyncRoot MUST be a directory.");
    }

    DirectoryIterator directoryIterator = new DirectoryIterator(syncRoot, format, strict);
    directoryIterator.process(directoryProcessor);
    */
    /*
    File[] possibleDeviceRoots = syncRoot.listFiles();
    for (File possibleDeviceRoot : possibleDeviceRoots) {
      if (possibleDeviceRoot.isDirectory()) {
        processDeviceDir(possibleDeviceRoot);
      }
    }
    */
  }

  static private void doProcessData(DirectoryCallbacks callbacks, File syncRoot, DirectoryFormat format, boolean strict)
      throws Exception {
    if (!syncRoot.isDirectory()) {
      throw new IllegalArgumentException("SyncRoot MUST be a directory.");
    }

    DirectoryIterator directoryIterator = new DirectoryIterator(syncRoot, format, strict);
    directoryIterator.process(callbacks);

  }

  public void processDeviceDir(String deviceName, File syncRoot) throws Exception {
    doProcessData(new FilteringProcessor(directoryProcessor, deviceName, null, null, null), syncRoot, null, false);
  }

  public void processUpdateDir(String deviceName, String updateName, File syncRoot) throws Exception {
    doProcessData(new FilteringProcessor(directoryProcessor, deviceName, updateName, null, null), syncRoot, null, false);
  }

  public void processVillageDir(String deviceName, String updateName, String village, File syncRoot) throws Exception {
    doProcessData(new FilteringProcessor(directoryProcessor, deviceName, updateName, village, null), syncRoot, null, false);
  }

  public void processTalkingBookDir(String deviceName, String updateName, String village, String talkingBook, File syncRoot) throws Exception {
    doProcessData(new FilteringProcessor(directoryProcessor, deviceName, updateName, village, talkingBook), syncRoot, null, false);
  }


  /*
  public void processDeviceDir(File deviceDir) throws IOException {
    if (!deviceDir.isDirectory()) {
      throw new IllegalArgumentException("laptop dir MUST be a directory.");
    }

    final File deviceContentDir = new File(deviceDir, DirectoryIterator.UPDATE_ROOT_V1);
    if (deviceContentDir.isDirectory()) {
      directoryProcessor.process(deviceDir.getName(), deviceContentDir);
    }
  }

  public void processUpdateDir(String deviceId, File updateDir) throws IOException {
    directoryProcessor.processUpdateDir(deviceId, updateDir);
  }

  public void processVillageDir(String deviceId, File villageDir, String contentUpdate) throws IOException {
    directoryProcessor.processVillageDir(deviceId, villageDir, contentUpdate);
  }

  public void processTalkingBookDir(String deviceId, File talkingBookDir, String contentUpdate, String villageName) throws IOException {
    directoryProcessor.processTalkingBookDir(deviceId, talkingBookDir, contentUpdate, villageName);
  }
  */

  public StatAggregator doConsistencyCheck() throws IOException {

    //If there was a flashdata then use that for the gold standard of stats, if not use the stats aggregator.
    StatAggregator  bestStatAggregator = (!aggregationProcessor.flashDataAggregator.isEmpty())
                                          ? aggregationProcessor.flashDataAggregator
                                          : aggregationProcessor.statsAggregator;

    //Do a consistency check of the last run
    ConsistencyChecker consistencyChecker = new ConsistencyChecker(bestStatAggregator,
                                                                   aggregationProcessor.logAggregator);
    int disparities = logAllDisparities(consistencyChecker);

    final SyncOperationLog syncOperationLog = createFinishedSyncingLog(disparities);
    for (TalkingBookSyncWriter  writer : eventWriters) {
      writer.writeOperationLog(syncOperationLog);
    }

    return bestStatAggregator;
  }

  public int logAllDisparities(ConsistencyChecker consistencyChecker) throws IOException  {
    int numDisparities = 0;
    numDisparities += logAllDisparitiesForGrouping(consistencyChecker, Grouping.contentId);
    //numDisparities += logAllDisparitiesForGrouping(consistencyChecker, Grouping.talkingBook);
    //numDisparities += logAllDisparitiesForGrouping(consistencyChecker, Grouping.village);

    return numDisparities;
  }

  public int logAllDisparitiesForGrouping(ConsistencyChecker consistencyChecker, Grouping grouping) throws IOException {
    int numDisparities = 0;
    numDisparities += logDisparities(consistencyChecker, grouping, AggregationOf.tenSecondPlays);
    numDisparities += logDisparities(consistencyChecker, grouping, AggregationOf.finishedPlays);
    numDisparities += logDisparities(consistencyChecker, grouping, AggregationOf.surveyTaken);
    numDisparities += logDisparities(consistencyChecker, grouping, AggregationOf.surveyUseless);
    numDisparities += logDisparities(consistencyChecker, grouping, AggregationOf.surveyApplied);

    return numDisparities;
  }

  public int logDisparities(ConsistencyChecker consistencyChecker, Grouping grouping, AggregationOf aggregationOf) throws IOException {

    int numDisparities = 0;
    Map<DeploymentId, Map<String, ConsistencyChecker.ConsistencyRecord>> disparities =
        consistencyChecker.findDisparities(grouping, aggregationOf, consistencyThreshold);

    for (DeploymentId deploymentId : disparities.keySet()) {
      for (String contentId : disparities.get(deploymentId).keySet()) {
        final ConsistencyChecker.ConsistencyRecord record = disparities.get(deploymentId).get(contentId);
        SyncOperationLog syncOperationLog = createDisparityLog(record, deploymentId, aggregationOf, grouping,
                                                               contentId);
        for (TalkingBookSyncWriter  writer : eventWriters) {
          writer.writeOperationLog(syncOperationLog);
        }
        numDisparities++;
      }
    }
    return numDisparities;
  }

  public SyncOperationLog createDisparityLog(ConsistencyChecker.ConsistencyRecord record,
                                             DeploymentId deploymentId, AggregationOf aggregationOf,
                                             Grouping grouping, String contentId) {

    SyncOperationLog syncOperationLog = new SyncOperationLog();
    syncOperationLog.setOperation(SyncOperationLog.OPERATION_VALIDATION);
    syncOperationLog.setMessage(
        "Error validating " + aggregationOf.name() + " for the grouping " + grouping.name() + " with threshold=" + consistencyThreshold);
    syncOperationLog.setDataBlob(
        "Validation failed for content update=" + deploymentId.id + "; " + grouping.name() + "=" + contentId +
            ".  Stats File records " + aggregationOf.name() + "=" + record.count1 +
            ".  Log Aggregation records " + aggregationOf.name() + "=" + record.count2);

    syncOperationLog.setLogType(SyncOperationLog.WARNING);
    syncOperationLog.setDateTime(DateTime.now());

    return syncOperationLog;
  }

  public SyncOperationLog createFinishedSyncingLog(int numDisparities) {
    SyncOperationLog syncOperationLog = new SyncOperationLog();
    syncOperationLog.setOperation(SyncOperationLog.OPERATION_SYNC);
    if (numDisparities == 0) {
      syncOperationLog.setMessage("Finished Sync successfully with no disparities between log files and stat files");
    } else {
      syncOperationLog.setMessage(
          "Finished Sync successfully, but there were " + numDisparities + " disparities between log files and stat files");
    }
    syncOperationLog.setLogType(SyncOperationLog.NORMAL);
    syncOperationLog.setDateTime(DateTime.now());

    return syncOperationLog;
  }

}
