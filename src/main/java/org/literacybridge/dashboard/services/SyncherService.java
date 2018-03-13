package org.literacybridge.dashboard.services;

import org.hibernate.SessionFactory;
import org.literacybridge.dashboard.api.TalkingBookSyncWriter;
import org.literacybridge.dashboard.processors.DbStatsWriter;
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

  // autowired to an instance of DatabaseSyncOperationLogWriterService
  @Autowired
  private SyncOperationLogWriter syncOperationLogWriter;

  // autowired to an instance of DatabaseEventWriterService
  @Autowired
  private EventWriter databaseEventWriter;

  // autowired to an instance of SyncAggregationWriterService
  @Autowired
  private SyncAggregationWriter syncAggregationWriter;

  public TalkingBookSyncWriter createSyncWriter() {
    return new DbStatsWriter(databaseEventWriter, syncAggregationWriter, syncOperationLogWriter);
  }


}
