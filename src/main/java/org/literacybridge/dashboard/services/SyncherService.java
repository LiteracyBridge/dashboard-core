package org.literacybridge.dashboard.services;

import org.hibernate.SessionFactory;
import org.literacybridge.dashboard.api.TalkingBookSyncWriter;
import org.literacybridge.dashboard.processors.DbSyncWriter;
import org.literacybridge.dashboard.api.EventWriter;
import org.literacybridge.dashboard.api.SyncAggregationWriter;
import org.literacybridge.dashboard.api.SyncOperationLogWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;

/**
 * @author willpugh
 */
@Repository("syncherService")
public class SyncherService {
  @Resource
  private SessionFactory sessionFactory;

  @Autowired
  private SyncOperationLogWriter syncOperationLogWriter;

  @Autowired
  private EventWriter databaseEventWriter;

  @Autowired
  private SyncAggregationWriter syncAggregationWriter;

  public TalkingBookSyncWriter createSyncWriter() {
    return new DbSyncWriter(databaseEventWriter, syncAggregationWriter, syncOperationLogWriter);
  }


}
