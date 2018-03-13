package org.literacybridge.dashboard.processors;

import org.literacybridge.stats.api.TalkingBookDataProcessor;
import org.literacybridge.stats.formats.flashData.FlashData;
import org.literacybridge.stats.formats.logFile.LogAction;
import org.literacybridge.stats.formats.logFile.LogLineContext;
import org.literacybridge.stats.formats.statsFile.StatsFile;
import org.literacybridge.stats.model.ProcessingContext;
import org.literacybridge.stats.model.SyncProcessingContext;
import org.literacybridge.dashboard.dbTables.TbDataLine;

import java.io.IOException;

/**
 * Created by willpugh on 2/7/14.
 */
abstract public class AbstractLogProcessor implements TalkingBookDataProcessor {

  @Override
  public void onTalkingBookStart(ProcessingContext context) {

  }

  @Override
  public void onTalkingBookEnd(ProcessingContext context) {

  }

  @Override
  public void onSyncProcessingStart(SyncProcessingContext context) {

  }

  @Override
  public void onSyncProcessingEnd(SyncProcessingContext context) {

   }

  @Override
  public void onPlay(LogLineContext context, String contentId, int volume, double voltage) {

  }

  @Override
  public void onPlayed(LogLineContext context, String contentId, short secondsPlayed, short secondsSomething,
                       int volume, double voltage, boolean ended) {

  }

  @Override
  public void onCategory(LogLineContext context, String categoryId) {

  }

  @Override
  public void onRecord(LogLineContext context, String contentId, int unknownNumber) {

  }

  @Override
  public void onRecorded(LogLineContext context, int secondsRecorded) {

  }

  @Override
  public void onPause(LogLineContext context, String contentId) {

  }

  @Override
  public void onUnPause(LogLineContext context, String contentId) {

  }

  @Override
  public void onSurvey(LogLineContext context, String contentId) {

  }

  @Override
  public void onSurveyCompleted(LogLineContext context, String contentId, boolean useful) {

  }

    @Override
    public void onJumpTime(LogLineContext logLineContext, int timeFrom, int timeTo) {

    }

    @Override
  public void onShuttingDown(LogLineContext context) {

  }

  @Override
  public void onVoltageDrop(LogLineContext context, LogAction action, double voltageDropped, int time) {

  }

  @Override
  public void onLogFileStart(String fileName) {

  }

  @Override
  public void onLogFileEnd() {

  }

  @Override
  public void processFlashData(SyncProcessingContext context, FlashData flashData) throws IOException {

  }

  @Override
  public void processCorruptFlashData(SyncProcessingContext context, String flashDataPath, String errorMessage) {

  }

  @Override
  public void processStatsFile(SyncProcessingContext context, String contentId, StatsFile statsFile) {

  }

  @Override
  public void markStatsFileAsCorrupted(SyncProcessingContext context, String contentId, String errorMessage) {

  }

  @Override
  public void processTbDataLine(TbDataLine tbDataLine) {

  }
}
