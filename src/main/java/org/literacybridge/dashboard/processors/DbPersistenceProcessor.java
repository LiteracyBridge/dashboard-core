package org.literacybridge.dashboard.processors;

import com.google.common.collect.Lists;
import org.literacybridge.dashboard.aggregation.AggregationOf;
import org.literacybridge.dashboard.aggregation.Aggregations;
import org.literacybridge.dashboard.aggregation.UpdateAggregations;
import org.literacybridge.dashboard.api.TalkingBookSyncWriter;
import org.literacybridge.dashboard.dbTables.contentUsage.ContentSyncUniqueId;
import org.literacybridge.dashboard.dbTables.contentUsage.SyncAggregation;
import org.literacybridge.dashboard.dbTables.syncOperations.TalkingBookCorruption;
import org.literacybridge.dashboard.dbTables.syncOperations.UniqueTalkingBookSync;
import org.literacybridge.stats.formats.flashData.FlashData;
import org.literacybridge.stats.formats.flashData.NORmsgStats;
import org.literacybridge.stats.formats.statsFile.StatsFile;
import org.literacybridge.stats.model.ProcessingContext;
import org.literacybridge.stats.model.SyncProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * @author willpugh
 */
public class DbPersistenceProcessor extends AbstractPersistenceProcessor {

  static protected final Logger logger = LoggerFactory.getLogger(DbPersistenceProcessor.class);
  private boolean                 flashDataProcessed = false;

    public DbPersistenceProcessor(TalkingBookSyncWriter writer) {
        this(Lists.<TalkingBookSyncWriter>newArrayList(writer));
    }
    public DbPersistenceProcessor(Collection<TalkingBookSyncWriter> writers) {
        super(writers);
    }


  @Override
  public void onTalkingBookEnd(ProcessingContext context) {

    final UpdateAggregations updateAggregations = logAggregations.aggregator.perUpdateAggregations.get(
        context.deploymentId);
    if (updateAggregations == null) {
      //No results
      return;
    }

    UniqueTalkingBookSync uniqueTalkingBookSync = new UniqueTalkingBookSync();
    uniqueTalkingBookSync.setContentUpdate(context.deploymentId.id);
    uniqueTalkingBookSync.setTalkingBook(context.talkingBookId);
    uniqueTalkingBookSync.setVillage(context.village);


    int corruptLines = 0;
    final Map<String, Aggregations> contentAggregations = updateAggregations.contentAggregations;

    for (String contentId : contentAggregations.keySet()) {
      final Aggregations contentAggregation = contentAggregations.get(contentId);
      corruptLines += contentAggregation.get(AggregationOf.corruptedLines);

      if (currentContext != null) {
        try {
          ContentSyncUniqueId contentSyncUniqueId = ContentSyncUniqueId.createFromContext(contentId, context, SyncAggregation.Source.LOG_EVENTS.value);
          SyncAggregation     syncAggregation = new SyncAggregation();

          syncAggregation.setContentSyncUniqueId(contentSyncUniqueId);
          syncAggregation.setContentPackage(currentContext.contentPackage);
          syncAggregation.setVillage(context.village);

          syncAggregation.setCountApplied(contentAggregation.get(AggregationOf.surveyApplied));
          syncAggregation.setCountUseless(contentAggregation.get(AggregationOf.surveyUseless));

          syncAggregation.setCountStarted(contentAggregation.get(AggregationOf.tenSecondPlays));
          syncAggregation.setCountQuarter(contentAggregation.get(AggregationOf.quarterPlays));
          syncAggregation.setCountHalf(contentAggregation.get(AggregationOf.halfPlays));
          syncAggregation.setCountThreeQuarters(contentAggregation.get(AggregationOf.threeQuartersPlays));
          syncAggregation.setCountCompleted(contentAggregation.get(AggregationOf.finishedPlays));
          syncAggregation.setTotalTimePlayed(contentAggregation.get(AggregationOf.totalTimePlayed));

          for (TalkingBookSyncWriter writer : writers) {
            writer.writeAggregation(syncAggregation, currentContext);
          }
        } catch (IOException ioe) {
          logger.error(ioe.getMessage() + ": cannot create aggregation from logs for " + context.toString());
        }
      }
    }
    currentContext = null;

    TalkingBookCorruption talkingBookCorruption = new TalkingBookCorruption();
    talkingBookCorruption.setUniqueTalkingBookSync(uniqueTalkingBookSync);
    talkingBookCorruption.setCorruptLines(corruptLines);

    try {
      for (TalkingBookSyncWriter writer : writers) {
        writer.writeTalkingBookCorruption(talkingBookCorruption);
      }
    } catch (IOException ioe) {
      logger.error(ioe.getMessage() + ": cannot create corruption aggregation from logs for " + context.toString());
    }

  }

  @Override
  public void processFlashData(SyncProcessingContext context, FlashData flashData) throws IOException {
    currentContext = context;
    logAggregations.processFlashData(context, flashData);

    for (NORmsgStats msgStats :  flashData.allStats()) {

      try {
        ContentSyncUniqueId contentSyncUniqueId = ContentSyncUniqueId.createFromContext(msgStats.getContentId(), context, SyncAggregation.Source.FLASH_DATA.value);
        SyncAggregation     syncAggregation = new SyncAggregation();

        syncAggregation.setContentSyncUniqueId(contentSyncUniqueId);
        syncAggregation.setContentPackage(context.contentPackage);
        syncAggregation.setVillage(context.village);

        syncAggregation.setCountApplied       (msgStats.getCountApplied());
        syncAggregation.setCountUseless       (msgStats.getCountUseless());

        syncAggregation.setCountStarted       (msgStats.getCountStarted());
        syncAggregation.setCountQuarter       (msgStats.getCountQuarter());
        syncAggregation.setCountHalf          (msgStats.getCountHalf());
        syncAggregation.setCountThreeQuarters (msgStats.getCountThreequarters());
        syncAggregation.setCountCompleted     (msgStats.getCountCompleted());
        syncAggregation.setTotalTimePlayed    (msgStats.getTotalSecondsPlayed());

        for (TalkingBookSyncWriter writer : writers) {
          writer.writeAggregation(syncAggregation, context);
        }
      } catch (IllegalArgumentException e) {
        logger.error(e.getMessage() + ": cannot create aggregation from flashdata for " + context.toString());
      }
    }

    flashDataProcessed = true;
  }

  @Override
  public void processStatsFile(SyncProcessingContext context, String contentId, StatsFile statsFile) {
    currentContext = context;
    //backupContentAggregations.processStatsFile(context, statsFile.messageId, statsFile);

      try {
        ContentSyncUniqueId contentSyncUniqueId = ContentSyncUniqueId.createFromContext(statsFile.messageId, context, SyncAggregation.Source.STAT_FILES.value);
        SyncAggregation     syncAggregation = new SyncAggregation();

        syncAggregation.setContentSyncUniqueId(contentSyncUniqueId);
        syncAggregation.setContentPackage(context.contentPackage);
        syncAggregation.setVillage(context.village);

        syncAggregation.setCountApplied       (statsFile.appliedCount);
        syncAggregation.setCountUseless       (statsFile.uselessCount);
        //TODO: we need to add total surveys taken
        syncAggregation.setCountStarted       (statsFile.openCount-statsFile.completionCount);
        syncAggregation.setCountQuarter       (0);
        syncAggregation.setCountHalf          (0);
        syncAggregation.setCountThreeQuarters (0);
        syncAggregation.setCountCompleted     (statsFile.completionCount);
        syncAggregation.setTotalTimePlayed    (0);

        for (TalkingBookSyncWriter writer : writers) {
          writer.writeAggregation(syncAggregation, context);
        }
      } catch (IllegalArgumentException e) {
        logger.error(e.getMessage() + ": cannot create aggregation from statsfile for " + context.toString());
      } catch (IOException e) {
          logger.error(e.getMessage() + ": TalkingBookSyncWriter IO Exception for " + context.toString());    	  
      }
  }


}
