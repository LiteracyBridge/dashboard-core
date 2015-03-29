package org.literacybridge.dashboard.api;

import org.literacybridge.dashboard.model.syncOperations.SyncOperationLog;
import org.literacybridge.dashboard.model.syncOperations.TalkingBookCorruption;
import org.literacybridge.stats.model.TbDataLine;

import java.io.IOException;

/**
 */
public interface SyncOperationLogWriter {
  void writeOperationLog(SyncOperationLog operationLog) throws IOException;

  void writeTalkingBookCorruption(TalkingBookCorruption talkingBookCorruption) throws IOException;

  void writeTbDataLog(TbDataLine tbDataLine) throws IOException;
}
