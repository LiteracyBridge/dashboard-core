package org.literacybridge.dashboard.processors;

import org.hibernate.id.IdentifierGenerationException;
import org.literacybridge.dashboard.aggregation.AggregationOf;
import org.literacybridge.dashboard.api.EventWriter;
import org.literacybridge.dashboard.api.TalkingBookSyncWriter;
import org.literacybridge.dashboard.dbTables.TbDataLine;
import org.literacybridge.dashboard.dbTables.events.Event;
import org.literacybridge.dashboard.dbTables.events.JumpEvent;
import org.literacybridge.dashboard.dbTables.events.PlayedEvent;
import org.literacybridge.dashboard.dbTables.events.RecordEvent;
import org.literacybridge.dashboard.dbTables.events.SurveyEvent;
import org.literacybridge.stats.formats.logFile.LogAction;
import org.literacybridge.stats.formats.logFile.LogLineContext;
import org.literacybridge.stats.model.ProcessingContext;
import org.literacybridge.stats.model.SyncProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * @author willpugh
 */
public class AbstractPersistenceProcessor extends AbstractLogProcessor {

  static protected final Logger logger = LoggerFactory.getLogger(AbstractPersistenceProcessor.class);

  protected final Collection<TalkingBookSyncWriter> writers;

  protected String category          = "";
  protected String recordedContentId = null;

  //HACK(willpugh) - This is here as a hack.  We need to get the content package in onTalkingBookEnd,
  //however, this information is not available until you are in a sync context, so we need to
  //stash this for later.  The content package SHOULD be the same for all syncs, but we could
  //invent ways this would get tricked.
  protected SyncProcessingContext currentContext = null;
  protected LogLineContext surveyLogLineContext = null;
  protected String         surveyContentId      = null;
  protected LogLineContext playLogLineContext = null;
  protected String         playContentId = null;

  //Track content aggregations through log files, in case there is no flashdata
  protected LogAggregationProcessor logAggregations    = new LogAggregationProcessor(10);

    public AbstractPersistenceProcessor(Collection<TalkingBookSyncWriter> writers) {
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
    logAggregations.clear();

  }

    @Override
    public void onSyncProcessingStart(SyncProcessingContext context) {

    }

    @Override
    public void onSyncProcessingEnd(SyncProcessingContext context) {

    }

  @Override
  public void onPlay(LogLineContext context, String contentId, int volume, double voltage) {
    currentContext = context.context;
    playContentId = contentId;
    playLogLineContext = context;
  }

  @Override
  public void onPlayed(LogLineContext context, String contentId, short secondsPlayed, short secondsSomething,
                       int volume, double voltage, boolean ended) {
    currentContext = context.context;
    logAggregations.onPlayed(context, contentId, secondsPlayed, secondsSomething, volume, voltage, ended);

    if (context.logLineInfo == null) {
      logAggregations.aggregator.add(context.context.deploymentId, AggregationOf.corruptedFiles,
                                               contentId, context.context.village, context.context.talkingBookId, 1);
      logger.trace(String.format("Corrupted log line info for file %s:%d", context.logFilePosition.loggingFileName(),
                                 context.logFilePosition.lineNumber));
      return;
    }

    PlayedEvent event = new PlayedEvent();
    Event.populateEvent(context, event);

    event.setContentId(contentId);
    event.setPercentDone((secondsSomething>0.0)?((double) secondsPlayed / (double) secondsSomething):0);
    event.setTimePlayed(secondsPlayed);
    event.setTotalTime(secondsSomething);
    event.setVolume(volume);
    event.setFinished(ended);


    for (EventWriter writer : writers) {
      try {
        writer.writePlayEvent(event, context);
      } catch (IOException e) {
        logger.error(e.getLocalizedMessage(), e);
      }
    }
    playContentId = null;
    playLogLineContext = null;
  }

  @Override
  public void onCategory(LogLineContext context, String categoryId) {
    currentContext = context.context;
    logAggregations.onCategory(context, categoryId);

    category = categoryId;
  }

  @Override
  public void onRecord(LogLineContext context, String contentId, int unknownNumber) {
    currentContext = context.context;
    logAggregations.onRecord(context, contentId, unknownNumber);

    recordedContentId = contentId;
  }

  @Override
  public void onRecorded(LogLineContext context, int secondsRecorded) {
    currentContext = context.context;
    logAggregations.onRecorded(context, secondsRecorded);

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
          writer.writeRecordEvent(event, context);
        } catch (IOException e) {
          logger.error(e.getLocalizedMessage(), e);
        }
      }
    }
  }

  @Override
  public void onPause(LogLineContext context, String contentId) {
    currentContext = context.context;
    logAggregations.onPause(context, contentId);

  }

  @Override
  public void onUnPause(LogLineContext context, String contentId) {
    currentContext = context.context;
    logAggregations.onUnPause(context, contentId);
  }

  @Override
  public void onSurvey(LogLineContext context, String contentId) {
    currentContext = context.context;
    logAggregations.onSurvey(context, contentId);

    // Note: the comment below almost certainly should say "is NOT null", because that's the
    // (only) way to have another survey that wasn't completed. (Didn't correct it because I
    // wanted to leave this as intact as possible, to give, ahem, context to the notes in the
    // next function.

    //If the survey content ID is null, then there was another survey
    //that wasn't completed
    if (surveyContentId != null && surveyLogLineContext != null) {
      SurveyEvent event = new SurveyEvent();
      Event.populateEvent(surveyLogLineContext, event);
      event.setContentId(surveyContentId);
      event.setIsUseful(null);

      // Note: changed the line below from 'context.logLineInfo', because this section of code is
      // concerned with a survey event that was not closed out, so it is the previous event that's
      // being logged.
      if (surveyLogLineContext.logLineInfo != null) {
        for (EventWriter writer : writers) {
          try {
            writer.writeSurveyEvent(event, context);
          } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
          } catch (IdentifierGenerationException e) {
            logger.error(e.getLocalizedMessage(), e);
            System.out.println("-------> *** IMPORTER GENERATED NULL PK *** <---------");
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
    currentContext = context.context;
    logAggregations.onSurveyCompleted(context, contentId, useful);

    // Note: It is a mystery why this logic is so similar to that above. It is a mystery why we
    // mix-and-match contentId from this call and context from the onSurvey call. It shouldn't
    // matter much; it is valid to have an onSurvey without onSurveyCompleted by simply going
    // to Home on the TB. And it shouldn't be possible to have an onSurveyCompleted without
    // a matching onSurvey. It particularly shouldn't be possible to have an onSurvey, then a
    // played event, then onSurveyCompleted. Shouldn't. Given how buggy the TB code is, and how
    // messy (and usually corrupt) the logs are, it seems it should be safer to use the values
    // from this call. But given how brittle everything is, I'm leaning to keeping what has been
    // running ("working" is another question) rather than changing anything. And leaving this as
    // it is at least requires that there be an opening onSurvey, which isn't bad.

    if (surveyContentId != null && surveyLogLineContext != null) {
      SurveyEvent event = new SurveyEvent();
      Event.populateEvent(surveyLogLineContext, event);
      event.setContentId(contentId);
      event.setIsUseful(useful);


      if (context.logLineInfo != null) {
        for (EventWriter writer : writers) {
          try {
            writer.writeSurveyEvent(event, context);
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
    public void onJumpTime(LogLineContext logLineContext, int timeFrom, int timeTo) {
        logAggregations.onJumpTime(logLineContext, timeFrom, timeTo);

        if (playLogLineContext != null && playContentId != null) {
            JumpEvent jumpEvent = new JumpEvent();
            Event.populateEvent(playLogLineContext, jumpEvent);
            jumpEvent.setContentId(playContentId);
            jumpEvent.setSeconds(timeFrom, timeTo);

            for (EventWriter writer : writers) {
                try {
                    writer.writeJumpEvent(jumpEvent, logLineContext);
                } catch (IOException e) {
                    logger.error(e.getLocalizedMessage(), e);
                }
            }
        }
    }

    @Override
  public void onShuttingDown(LogLineContext context) {
    logAggregations.onShuttingDown(context);
  }

  @Override
  public void onVoltageDrop(LogLineContext context, LogAction action, double voltageDropped, int time) {
    logAggregations.onVoltageDrop(context, action, voltageDropped, time);
  }

  @Override
  public void onLogFileStart(String fileName) {
    logAggregations.onLogFileStart(fileName);

    category = "";
    surveyContentId = null;
    surveyLogLineContext = null;
    recordedContentId = null;
    playContentId = null;
    playLogLineContext = null;

  }

  @Override
  public void onLogFileEnd() {
    logAggregations.onLogFileEnd();
  }

  @Override
  public void processCorruptFlashData(SyncProcessingContext context, String flashDataPath, String errorMessage) {
    currentContext = context;
    logAggregations.processCorruptFlashData(context, flashDataPath, errorMessage);
    super.processCorruptFlashData(context, flashDataPath, errorMessage);
  }


}
