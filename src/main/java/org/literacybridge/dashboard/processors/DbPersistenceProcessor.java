package org.literacybridge.dashboard.processors;

import com.google.common.collect.Lists;
import org.hibernate.id.IdentifierGenerationException;
import org.literacybridge.dashboard.aggregation.AggregationOf;
import org.literacybridge.dashboard.aggregation.Aggregations;
import org.literacybridge.dashboard.aggregation.UpdateAggregations;
import org.literacybridge.dashboard.api.EventWriter;
import org.literacybridge.dashboard.api.TalkingBookSyncWriter;
import org.literacybridge.dashboard.model.contentUsage.ContentSyncUniqueId;
import org.literacybridge.dashboard.model.contentUsage.SyncAggregation;
import org.literacybridge.dashboard.model.syncOperations.TalkingBookCorruption;
import org.literacybridge.dashboard.model.syncOperations.UniqueTalkingBookSync;
import org.literacybridge.stats.formats.flashData.FlashData;
import org.literacybridge.stats.formats.flashData.NORmsgStats;
import org.literacybridge.stats.formats.logFile.LogAction;
import org.literacybridge.stats.formats.logFile.LogLineContext;
import org.literacybridge.stats.formats.statsFile.StatsFile;
import org.literacybridge.stats.model.ProcessingContext;
import org.literacybridge.stats.model.SyncProcessingContext;
import org.literacybridge.stats.model.TbDataLine;
import org.literacybridge.stats.model.events.Event;
import org.literacybridge.stats.model.events.PlayedEvent;
import org.literacybridge.stats.model.events.RecordEvent;
import org.literacybridge.stats.model.events.SurveyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * @author willpugh
 */
public class DbPersistenceProcessor extends AbstractLogProcessor {

  static protected final Logger logger = LoggerFactory.getLogger(DbPersistenceProcessor.class);

  final Collection<TalkingBookSyncWriter> writers;

  String category          = "";
  String recordedContentId = null;

  //HACK(willpugh) - This is here as a hack.  We need to get the content package in onTalkingBookEnd,
  //however, this information is not available until you are in a sync context, so we need to
  //stash this for later.  The content package SHOULD be the same for all syncs, but we could
  //invent ways this would get tricked.
  SyncProcessingContext syncProcessingContext = null;
  LogLineContext surveyLogLineContext = null;
  String         surveyContentId      = null;

  //Track content aggregations through log files, in case there is no flashdata
  boolean                 flashDataProcessed        = false;
  LogAggregationProcessor backupContentAggregations = new LogAggregationProcessor(10);


  public DbPersistenceProcessor(TalkingBookSyncWriter writer) {
    this(Lists.newArrayList(writer));
  }

  public DbPersistenceProcessor(Collection<TalkingBookSyncWriter> writers) {
    this.writers = writers;
  }

  @Override
  public void processTbDataLine(TbDataLine tbDataLine) {
    for (TalkingBookSyncWriter writer : writers) {
      try {
        writer.writeTbDataLog(tbDataLine);
      } catch (IOException e) {
        logger.error(e.getLocalizedMessage(), e);
      }
    }
  }

  @Override
  public void onTalkingBookStart(ProcessingContext context) {
    flashDataProcessed = false;
    backupContentAggregations.clear();

  }

  @Override
  public void onTalkingBookEnd(ProcessingContext context) {

    final UpdateAggregations updateAggregations = backupContentAggregations.aggregator.perUpdateAggregations.get(
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

      if (syncProcessingContext != null) {
        try {
          ContentSyncUniqueId contentSyncUniqueId = ContentSyncUniqueId.createFromContext(contentId, context, SyncAggregation.LOG_EVENTS);
          SyncAggregation     syncAggregation = new SyncAggregation();

          syncAggregation.setContentSyncUniqueId(contentSyncUniqueId);
          syncAggregation.setContentPackage(syncProcessingContext.contentPackage);
          syncAggregation.setVillage(context.village);

          syncAggregation.setCountApplied       (contentAggregation.get(AggregationOf.surveyApplied));
          syncAggregation.setCountUseless       (contentAggregation.get(AggregationOf.surveyUseless));

          syncAggregation.setCountStarted       (contentAggregation.get(AggregationOf.tenSecondPlays));
          syncAggregation.setCountQuarter       (contentAggregation.get(AggregationOf.quarterPlays));
          syncAggregation.setCountHalf          (contentAggregation.get(AggregationOf.halfPlays));
          syncAggregation.setCountThreeQuarters (contentAggregation.get(AggregationOf.threeQuartersPlays));
          syncAggregation.setCountCompleted     (contentAggregation.get(AggregationOf.finishedPlays));
          syncAggregation.setTotalTimePlayed    (contentAggregation.get(AggregationOf.totalTimePlayed));

          for (TalkingBookSyncWriter writer : writers) {
            writer.writeAggregation(syncAggregation);
          }
        } catch (IOException ioe) {
          logger.error(ioe.getMessage() + ": cannot create aggregation from logs for " + context.toString());
        }
      }
      syncProcessingContext = null;
    }

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
  public void onPlay(LogLineContext context, String contentId, int volume, double voltage) {
    syncProcessingContext = context.context;
  }

  @Override
  public void onPlayed(LogLineContext context, String contentId, short secondsPlayed, short secondsSomething,
                       int volume, double voltage, boolean ended) {
    syncProcessingContext = context.context;
    backupContentAggregations.onPlayed(context, contentId, secondsPlayed, secondsSomething, volume, voltage, ended);

    if (context.logLineInfo == null) {
      backupContentAggregations.aggregator.add(context.context.deploymentId, AggregationOf.corruptedFiles,
                                               contentId, context.context.village, context.context.talkingBookId, 1);
      logger.trace(String.format("Corrupted log line info for file %s:%d", context.logFilePosition.loggingFileName(),
                                 context.logFilePosition.lineNumber));
      return;
    }

    PlayedEvent event = new PlayedEvent();
    Event.populateEvent(context, event);

    event.setContentId(contentId);
    event.setPercentDone((double) secondsPlayed / (double) secondsSomething);
    event.setTimePlayed(secondsPlayed);
    event.setTotalTime(secondsSomething);
    event.setVolume(volume);
    event.setFinished(ended);


    for (EventWriter writer : writers) {
      try {
        writer.writePlayEvent(event);
      } catch (IOException e) {
        logger.error(e.getLocalizedMessage(), e);
      }
    }
  }

  @Override
  public void onCategory(LogLineContext context, String categoryId) {
    syncProcessingContext = context.context;
    backupContentAggregations.onCategory(context, categoryId);

    category = categoryId;
  }

  @Override
  public void onRecord(LogLineContext context, String contentId, int unknownNumber) {
    syncProcessingContext = context.context;
    backupContentAggregations.onRecord(context, contentId, unknownNumber);

    recordedContentId = contentId;
  }

  @Override
  public void onRecorded(LogLineContext context, int secondsRecorded) {
    syncProcessingContext = context.context;
    backupContentAggregations.onRecorded(context, secondsRecorded);

    if (context.logLineInfo == null) {
      logger.trace(String.format("Corrupted log line info for file %s:%d", context.logFilePosition.loggingFileName(),
                                 context.logFilePosition.lineNumber));
      return;
    }

    if (recordedContentId != null) {
      RecordEvent event = new RecordEvent();
      Event.populateEvent(context, event);
      event.setContentId(recordedContentId);
      event.setSecondsRecorded(secondsRecorded);

      for (EventWriter writer : writers) {
        try {
          writer.writeRecordEvent(event);
        } catch (IOException e) {
          logger.error(e.getLocalizedMessage(), e);
        }
      }
    }
  }

  @Override
  public void onPause(LogLineContext context, String contentId) {
    syncProcessingContext = context.context;
    backupContentAggregations.onPause(context, contentId);

  }

  @Override
  public void onUnPause(LogLineContext context, String contentId) {
    syncProcessingContext = context.context;
    backupContentAggregations.onUnPause(context, contentId);
  }

  @Override
  public void onSurvey(LogLineContext context, String contentId) {
    syncProcessingContext = context.context;
    backupContentAggregations.onSurvey(context, contentId);

    //If the survey content ID is null, then there was another survey
    //that wasn't completed
    if (surveyContentId != null && surveyLogLineContext != null) {
      SurveyEvent event = new SurveyEvent();
      Event.populateEvent(surveyLogLineContext, event);
      event.setContentId(contentId);
      event.setIsUseful(null);

      if (context.logLineInfo != null) {
        for (EventWriter writer : writers) {
          try {
            writer.writeSurveyEvent(event);
          } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
          } catch (IdentifierGenerationException e) {
            logger.error(e.getLocalizedMessage(), e);
            System.out.println("-------> *** HIBERNATE ERROR *** <---------");
          }
        }
      } else {
        logger.trace(String.format("Corrupted log line info for file %s:%d", context.logFilePosition.loggingFileName(),
                                   context.logFilePosition.lineNumber));

      }
    }

    surveyContentId = contentId;
    surveyLogLineContext = context;

  }

  @Override
  public void onSurveyCompleted(LogLineContext context, String contentId, boolean useful) {
    syncProcessingContext = context.context;
    backupContentAggregations.onSurveyCompleted(context, contentId, useful);

    if (surveyContentId != null && surveyLogLineContext != null) {
      SurveyEvent event = new SurveyEvent();
      Event.populateEvent(surveyLogLineContext, event);
      event.setContentId(contentId);
      event.setIsUseful(useful);


      if (context.logLineInfo != null) {
        for (EventWriter writer : writers) {
          try {
            writer.writeSurveyEvent(event);
          } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
          }
        }
      } else {
        logger.trace(String.format("Corrupted log line info for file %s:%d", context.logFilePosition.loggingFileName(),
                                   context.logFilePosition.lineNumber));
      }
    }

    surveyContentId = null;
    surveyLogLineContext = null;
  }

  @Override
  public void onShuttingDown(LogLineContext context) {
    backupContentAggregations.onShuttingDown(context);
  }

  @Override
  public void onVoltageDrop(LogLineContext context, LogAction action, double voltageDropped, int time) {
    backupContentAggregations.onVoltageDrop(context, action, voltageDropped, time);
  }

  @Override
  public void onLogFileStart(String fileName) {
    backupContentAggregations.onLogFileStart(fileName);

    category = "";
    surveyContentId = null;
    surveyLogLineContext = null;
    recordedContentId = null;
  }

  @Override
  public void onLogFileEnd() {
    backupContentAggregations.onLogFileEnd();
  }

  @Override
  public void processFlashData(SyncProcessingContext context, FlashData flashData) throws IOException {
    syncProcessingContext = context;
    backupContentAggregations.processFlashData(context, flashData);

    for (NORmsgStats msgStats :  flashData.allStats()) {

      try {
        ContentSyncUniqueId contentSyncUniqueId = ContentSyncUniqueId.createFromContext(msgStats.getContentId(), context, SyncAggregation.FLASH_DATA);
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
          writer.writeAggregation(syncAggregation);
        }
      } catch (IllegalArgumentException e) {
        logger.error(e.getMessage() + ": cannot create aggregation from flashdata for " + context.toString());
      }
    }

    flashDataProcessed = true;
  }

  @Override
  public void processCorruptFlashData(SyncProcessingContext context, String flashDataPath, String errorMessage) {
    syncProcessingContext = context;
    backupContentAggregations.processCorruptFlashData(context, flashDataPath, errorMessage);
    super.processCorruptFlashData(context, flashDataPath, errorMessage);
  }
  @Override
  public void processStatsFile(SyncProcessingContext context, String contentId, StatsFile statsFile) {
    syncProcessingContext = context;
    //backupContentAggregations.processStatsFile(context, statsFile.messageId, statsFile);

      try {
        ContentSyncUniqueId contentSyncUniqueId = ContentSyncUniqueId.createFromContext(statsFile.messageId, context, SyncAggregation.STAT_FILES);
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
          writer.writeAggregation(syncAggregation);
        }
      } catch (IllegalArgumentException e) {
        logger.error(e.getMessage() + ": cannot create aggregation from statsfile for " + context.toString());
      } catch (IOException e) {
          logger.error(e.getMessage() + ": TalkingBookSyncWriter IO Exception for " + context.toString());    	  
      }
  }


}
