package org.literacybridge.dashboard.processors;

import org.literacybridge.stats.model.events.PlayedEvent;
import org.literacybridge.stats.model.events.RecordEvent;
import org.literacybridge.stats.model.events.SurveyEvent;
import org.literacybridge.dashboard.api.EventWriter;
import org.literacybridge.dashboard.api.SyncAggregationWriter;
import org.literacybridge.dashboard.api.SyncOperationLogWriter;
import org.literacybridge.dashboard.model.contentUsage.SyncAggregation;
import org.literacybridge.dashboard.model.syncOperations.SyncOperationLog;
import org.literacybridge.dashboard.model.syncOperations.TalkingBookCorruption;
import org.literacybridge.dashboard.api.TalkingBookSyncWriter;

import java.io.IOException;

/**
 * @author willpugh
 */
public class DbSyncWriter implements TalkingBookSyncWriter {

  protected final EventWriter            eventWriter;
  protected final SyncAggregationWriter  syncAggregationWriter;
  protected final SyncOperationLogWriter syncOperationLogWriter;

  public DbSyncWriter(EventWriter eventWriter, SyncAggregationWriter syncAggregationWriter,
                      SyncOperationLogWriter syncOperationLogWriter) {
    this.eventWriter = eventWriter;
    this.syncAggregationWriter = syncAggregationWriter;
    this.syncOperationLogWriter = syncOperationLogWriter;
  }

  @Override
  public void writePlayEvent(PlayedEvent playEvent) throws IOException {
    eventWriter.writePlayEvent(playEvent);
  }

  @Override
  public void writeRecordEvent(RecordEvent recordEvent) throws IOException {
    eventWriter.writeRecordEvent(recordEvent);
  }

  @Override
  public void writeSurveyEvent(SurveyEvent surveyEvent) throws IOException {
    eventWriter.writeSurveyEvent(surveyEvent);
  }

  @Override
  public void writeAggregation(SyncAggregation aggregation) throws IOException {
    syncAggregationWriter.writeAggregation(aggregation);
  }

  @Override
  public void writeOperationLog(SyncOperationLog operationLog) throws IOException  {
    syncOperationLogWriter.writeOperationLog(operationLog);
  }

  @Override
  public void writeTalkingBookCorruption(TalkingBookCorruption talkingBookCorruption) throws IOException {
    syncOperationLogWriter.writeTalkingBookCorruption(talkingBookCorruption);
  }
}
