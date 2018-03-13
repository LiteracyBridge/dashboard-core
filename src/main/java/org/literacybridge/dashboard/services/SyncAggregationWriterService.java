package org.literacybridge.dashboard.services;

import org.hibernate.SessionFactory;
import org.literacybridge.dashboard.dbTables.contentUsage.SyncAggregation;
import org.literacybridge.dashboard.api.SyncAggregationWriter;
import org.literacybridge.stats.model.SyncProcessingContext;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * @author willpugh
 */
@Repository("syncAggregationWriter")
public class SyncAggregationWriterService implements SyncAggregationWriter {

  @Resource
  private SessionFactory sessionFactory;

  @Override
  @Transactional(propagation = Propagation.REQUIRED, readOnly = false)
  public void writeAggregation(SyncAggregation aggregation,
      SyncProcessingContext context) throws IOException {
    sessionFactory.getCurrentSession().saveOrUpdate(aggregation);
  }

}
