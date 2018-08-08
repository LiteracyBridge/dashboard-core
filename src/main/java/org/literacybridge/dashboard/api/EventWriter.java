package org.literacybridge.dashboard.api;


import org.literacybridge.dashboard.dbTables.events.FasterEvent;
import org.literacybridge.dashboard.dbTables.events.JumpEvent;
import org.literacybridge.dashboard.dbTables.events.PlayedEvent;
import org.literacybridge.dashboard.dbTables.events.RecordEvent;
import org.literacybridge.dashboard.dbTables.events.SlowerEvent;
import org.literacybridge.dashboard.dbTables.events.SurveyEvent;
import org.literacybridge.stats.formats.logFile.LogLineContext;

import java.io.IOException;

/**
 * Created by willpugh on 2/7/14.
 */
public interface EventWriter {

  void writePlayEvent(PlayedEvent playEvent,
      LogLineContext context) throws IOException;

  void writeRecordEvent(RecordEvent recordEvent,
      LogLineContext context) throws IOException;

  void writeSurveyEvent(SurveyEvent surveyEvent,
      LogLineContext context) throws IOException;

  void writeJumpEvent(JumpEvent jumpEvent, LogLineContext context) throws IOException;
  void writeFasterEvent(FasterEvent fasterEvent, LogLineContext context) throws IOException;
  void writeSlowerEvent(SlowerEvent slowerEvent, LogLineContext context) throws IOException;
}
