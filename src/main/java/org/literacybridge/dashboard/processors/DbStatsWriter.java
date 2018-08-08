package org.literacybridge.dashboard.processors;

import org.literacybridge.dashboard.api.EventWriter;
import org.literacybridge.dashboard.api.SyncAggregationWriter;
import org.literacybridge.dashboard.api.SyncOperationLogWriter;
import org.literacybridge.dashboard.api.TalkingBookSyncWriter;
import org.literacybridge.dashboard.dbTables.contentUsage.SyncAggregation;
import org.literacybridge.dashboard.dbTables.events.FasterEvent;
import org.literacybridge.dashboard.dbTables.events.JumpEvent;
import org.literacybridge.dashboard.dbTables.events.SlowerEvent;
import org.literacybridge.dashboard.dbTables.syncOperations.SyncOperationLog;
import org.literacybridge.dashboard.dbTables.syncOperations.TalkingBookCorruption;
import org.literacybridge.dashboard.dbTables.TbDataLine;
import org.literacybridge.dashboard.dbTables.events.PlayedEvent;
import org.literacybridge.dashboard.dbTables.events.RecordEvent;
import org.literacybridge.dashboard.dbTables.events.SurveyEvent;
import org.literacybridge.stats.formats.logFile.LogLineContext;
import org.literacybridge.stats.model.SyncProcessingContext;

import java.io.IOException;

/**
 * @author willpugh
 */
public class DbStatsWriter implements TalkingBookSyncWriter {

  private final EventWriter            eventWriter;
  private final SyncAggregationWriter  syncAggregationWriter;
  private final SyncOperationLogWriter syncOperationLogWriter;

  public DbStatsWriter(EventWriter eventWriter, SyncAggregationWriter syncAggregationWriter,
                      SyncOperationLogWriter syncOperationLogWriter) {
    this.eventWriter = eventWriter;
    this.syncAggregationWriter = syncAggregationWriter;
    this.syncOperationLogWriter = syncOperationLogWriter;
  }

  @Override
  public void writePlayEvent(PlayedEvent playEvent,
      LogLineContext context) throws IOException {
    try {
      eventWriter.writePlayEvent(playEvent, context);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void writeRecordEvent(RecordEvent recordEvent,
      LogLineContext context) throws IOException {
    eventWriter.writeRecordEvent(recordEvent, context);
  }

  @Override
  public void writeSurveyEvent(SurveyEvent surveyEvent,
      LogLineContext context) throws IOException {
    eventWriter.writeSurveyEvent(surveyEvent, context);
  }

    @Override
    public void writeJumpEvent(JumpEvent jumpEvent, LogLineContext context) throws IOException {
        // We don't record these in the database.
    }

  @Override
  public void writeFasterEvent(FasterEvent fasterEvent, LogLineContext context) throws IOException {
    // We don't record these in the database.
  }

  @Override
  public void writeSlowerEvent(SlowerEvent slowerEvent, LogLineContext context) throws IOException {
    // We don't record these in the database.
  }

  @Override
  public void writeAggregation(SyncAggregation aggregation,
      SyncProcessingContext context) throws IOException {
    syncAggregationWriter.writeAggregation(aggregation, context);
  }

  @Override
  public void writeOperationLog(SyncOperationLog operationLog) throws IOException  {
    syncOperationLogWriter.writeOperationLog(operationLog);
  }

  @Override
  public void writeTalkingBookCorruption(TalkingBookCorruption talkingBookCorruption) throws IOException {
    syncOperationLogWriter.writeTalkingBookCorruption(talkingBookCorruption);
  }

  @Override
  public void writeTbDataLog(TbDataLine tbDataLine) throws IOException {
    syncOperationLogWriter.writeTbDataLog(tbDataLine);
  }
}
