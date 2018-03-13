package org.literacybridge.dashboard.api;

import org.literacybridge.dashboard.dbTables.contentUsage.SyncAggregation;
import org.literacybridge.stats.model.SyncProcessingContext;

import java.io.IOException;

/**
 * @author willpugh
 */
public interface SyncAggregationWriter {

  public void writeAggregation(SyncAggregation aggregation,
      SyncProcessingContext context) throws IOException;

}
