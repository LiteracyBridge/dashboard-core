package org.literacybridge.dashboard.services;

import org.hibernate.SessionFactory;
import org.literacybridge.dashboard.dbTables.syncOperations.TalkingBookCorruption;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * @author willpugh
 */
public class TalkingBookCorruptionWriterService {

  @Resource
  private SessionFactory sessionFactory;

  @Transactional(propagation = Propagation.REQUIRED, readOnly = false)
  public void write(TalkingBookCorruption corruption) throws IOException {
    sessionFactory.getCurrentSession().saveOrUpdate(corruption);
  }

}
