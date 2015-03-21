package org.literacybridge.dashboard.api;


import org.literacybridge.stats.model.events.PlayedEvent;
import org.literacybridge.stats.model.events.RecordEvent;
import org.literacybridge.stats.model.events.SurveyEvent;
import org.literacybridge.stats.model.events.TBData;

import java.io.IOException;

/**
 * Created by willpugh on 2/7/14.
 */
public interface EventWriter {

  void writePlayEvent(PlayedEvent playEvent) throws IOException;

  void writeRecordEvent(RecordEvent recordEvent) throws IOException;

  void writeSurveyEvent(SurveyEvent surveyEvent) throws IOException;

}
