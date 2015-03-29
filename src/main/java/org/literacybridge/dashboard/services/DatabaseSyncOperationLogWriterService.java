package org.literacybridge.dashboard.services;

import org.hibernate.SessionFactory;
import org.literacybridge.dashboard.api.SyncOperationLogWriter;
import org.literacybridge.dashboard.model.syncOperations.SyncOperationLog;
import org.literacybridge.dashboard.model.syncOperations.TalkingBookCorruption;
import org.literacybridge.stats.model.TbDataLine;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * Created by willpugh on 2/12/14.
 */
@Repository("syncOperationLogWriter")
public class DatabaseSyncOperationLogWriterService implements SyncOperationLogWriter {

  @Resource
  private SessionFactory sessionFactory;

  @Override
  @Transactional(propagation = Propagation.REQUIRED, readOnly = false)
  public void writeOperationLog(SyncOperationLog operationLog) throws IOException {
    sessionFactory.getCurrentSession().save(operationLog);
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRED, readOnly = false)
  public void writeTalkingBookCorruption(TalkingBookCorruption talkingBookCorruption) throws IOException {
    sessionFactory.getCurrentSession().saveOrUpdate(talkingBookCorruption);
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRED, readOnly = false)
  public void writeTbDataLog(TbDataLine tbDataLine) throws IOException {
    sessionFactory.getCurrentSession().saveOrUpdate(tbDataLine);
  }
}
