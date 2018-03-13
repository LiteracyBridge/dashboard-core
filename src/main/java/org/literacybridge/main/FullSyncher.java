package org.literacybridge.main;

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.literacybridge.dashboard.aggregation.AggregationOf;
import org.literacybridge.dashboard.aggregation.Grouping;
import org.literacybridge.dashboard.aggregation.StatAggregator;
import org.literacybridge.dashboard.api.TalkingBookSyncWriter;
import org.literacybridge.dashboard.processes.ContentUsageUpdateProcess;
import org.literacybridge.stats.api.DirectoryCallbacks;
import org.literacybridge.stats.api.TalkingBookDataProcessor;
import org.literacybridge.stats.formats.syncDirectory.DirectoryProcessor;
import org.literacybridge.stats.model.DeploymentId;
import org.literacybridge.dashboard.dbTables.syncOperations.SyncOperationLog;
import org.literacybridge.dashboard.processors.*;
import org.literacybridge.stats.DirectoryIterator;
import org.literacybridge.stats.model.DirectoryFormat;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Created by willpugh on 2/10/14.
 */
public class FullSyncher {

  public static final int MIN_SECONDS_FOR_MIN_PLAY = 10;

  private final double                            consistencyThreshold;
    private final Collection<TalkingBookSyncWriter> statsWriters;
  private final Collection<TalkingBookDataProcessor> dataProcessors;
  private final DirectoryProcessor                directoryProcessor;
  private final ContentUsageUpdateProcess.UpdateUsageContext context;

    // TODO: This is for testing only. Fix the test.
    public FullSyncher(
        double consistencyThreshold,
        Collection<TalkingBookSyncWriter> statsWriters,
        ContentUsageUpdateProcess.UpdateUsageContext context)
    {
        this(
            consistencyThreshold,
            statsWriters,
            ImmutableList.<TalkingBookDataProcessor>of(
                new DbPersistenceProcessor(statsWriters),
                new AggregationProcessor(MIN_SECONDS_FOR_MIN_PLAY)),
            context);
    }


    public FullSyncher(
        double consistencyThreshold,
        Collection<TalkingBookSyncWriter> statsWriters,
        Collection<TalkingBookDataProcessor> dataProcessors,
        ContentUsageUpdateProcess.UpdateUsageContext context)
    {
        this.consistencyThreshold = consistencyThreshold;
        this.statsWriters = statsWriters;
        this.dataProcessors = dataProcessors;

        directoryProcessor = new DirectoryProcessor(dataProcessors, context);
        this.context = context;
    }

  public void processData(File syncRoot) throws Exception {
    processData(syncRoot, null, false);
  }

  public void processData(File syncRoot, DirectoryFormat format, boolean strict) throws Exception {
      if (!syncRoot.isDirectory()) {
          throw new IllegalArgumentException("SyncRoot MUST be a directory.");
      }

      DirectoryIterator directoryIterator = new DirectoryIterator(syncRoot, format, strict, context);
      directoryIterator.process(directoryProcessor);
  }

    public StatAggregator doConsistencyCheck() throws IOException {
      AggregationProcessor aggregationProcessor = null;
      for (TalkingBookDataProcessor p: dataProcessors) {
          if (p instanceof AggregationProcessor) {
              aggregationProcessor = (AggregationProcessor) p;
              break;
          }
      }

    //If there was a flashdata then use that for the gold standard of stats, if not use the stats aggregator.
    StatAggregator  bestStatAggregator = (!aggregationProcessor.flashDataAggregator.isEmpty())
                                          ? aggregationProcessor.flashDataAggregator
                                          : aggregationProcessor.statsAggregator;

    //Do a consistency check of the last run
    ConsistencyChecker consistencyChecker = new ConsistencyChecker(bestStatAggregator,
                                                                   aggregationProcessor.logAggregator);
    int disparities = logAllDisparities(consistencyChecker);

    final SyncOperationLog syncOperationLog = createFinishedSyncingLog(disparities);
    for (TalkingBookSyncWriter  writer : statsWriters) {
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
        SyncOperationLog syncOperationLog = createDisparityLog(record, deploymentId,
            aggregationOf, grouping,
                                                               contentId);
        for (TalkingBookSyncWriter  writer : statsWriters) {
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
