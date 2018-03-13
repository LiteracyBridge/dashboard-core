package org.literacybridge.dashboard.api;

import org.literacybridge.dashboard.dbTables.events.PlayedEvent;
import org.literacybridge.stats.formats.logFile.LogLineContext;

import java.io.IOException;

/**
 * @author willpugh
 *
 * This is an interface that merely aggreagates three otherwise othogonal interfaces. Callers of ANY
 * of those interfaces can simply call THIS one. While strictly unnecessary, it may reduce some
 * boilerplate code.
 */
public interface TalkingBookSyncWriter extends EventWriter, SyncAggregationWriter, SyncOperationLogWriter { }
