package org.literacybridge.dashboard.api;

import org.literacybridge.dashboard.model.contentUsage.SyncAggregation;

import java.io.IOException;

/**
 * @author willpugh
 */
public interface SyncAggregationWriter {

  public void writeAggregation(SyncAggregation aggregation) throws IOException;

}
