package org.literacybridge.dashboard.processors;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.literacybridge.dashboard.api.TalkingBookSyncWriter;
import org.literacybridge.dashboard.dbTables.TbDataLine;
import org.literacybridge.dashboard.dbTables.contentUsage.SyncAggregation;
import org.literacybridge.dashboard.dbTables.events.Event;
import org.literacybridge.dashboard.dbTables.events.EventUniqueId;
import org.literacybridge.dashboard.dbTables.events.FasterEvent;
import org.literacybridge.dashboard.dbTables.events.JumpEvent;
import org.literacybridge.dashboard.dbTables.events.PlayedEvent;
import org.literacybridge.dashboard.dbTables.events.RecordEvent;
import org.literacybridge.dashboard.dbTables.events.SlowerEvent;
import org.literacybridge.dashboard.dbTables.events.SurveyEvent;
import org.literacybridge.dashboard.dbTables.syncOperations.SyncOperationLog;
import org.literacybridge.dashboard.dbTables.syncOperations.TalkingBookCorruption;
import org.literacybridge.dashboard.processes.ContentUsageUpdateProcess;
import org.literacybridge.stats.formats.logFile.LogLineContext;
import org.literacybridge.stats.model.SyncProcessingContext;

import java.io.IOException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

/**
 * @author bill, based on DbSyncWriter
 */
public class FlatStatsWriter implements TalkingBookSyncWriter {
    private static final String TBEVENTS_LOG = "tbevents.kvp";
    private static final String PLAYED_STATS_LOG = "playstatistics.kvp";
    
    private static final Set<String> openedStreams = new HashSet<>();
    private final ContentUsageUpdateProcess.UpdateUsageContext processContext;

    public FlatStatsWriter(ContentUsageUpdateProcess.UpdateUsageContext processContext) {
        this.processContext = processContext;
    }

    private PrintStream getStream(String fileName) throws IOException {
        PrintStream ps = processContext.getGlobalFileOutputStream(fileName);
        openedStreams.add(fileName);
        return ps;
    }

    private void appendEventCommonFields(LogWriter lw, SyncProcessingContext context, Event event) {
        EventUniqueId uid = event.getIdFields();
        lw.append(
            "project",context.project,
            "deployment",context.deploymentId.toString(),
            "contentpackage",event.getPackageId(),
            "community",event.getVillage(),
            "recipientid",context.recipientId,
            "talkingbookid",event.getIdFields().getTalkingBookId(),

            "wakecycle",uid.getCycle(),
            "powercycle",uid.getPeriod(),
            "dayinpowercycle",uid.getDayInPeriod(),
            "timeinday",uid.getTimeInDay()
            );
    }

    @Override
    public void writePlayEvent(PlayedEvent event,
        LogLineContext logLineContext) throws IOException
    {
        SyncProcessingContext context = logLineContext.context;
        PrintStream ps = getStream(TBEVENTS_LOG);
        LogWriter lw = new LogWriter(ps, logLineContext.context.syncTime.toDateTime(DateTimeZone.UTC), "played");
        appendEventCommonFields(lw, context, event);
        lw.append(
            "contentid", event.getContentId(),

            "played",event.getTimePlayed(),
            "length",event.getTotalTime(),
            "pctcompleted",100.0*event.getPercentDone(),
            "isfinished",event.isFinished()?1:0 // as integer, so we can sum()
        );
        lw.write();
    }

    @Override
    public void writeRecordEvent(RecordEvent event,
        LogLineContext logLineContext) throws IOException
    {
        SyncProcessingContext context = logLineContext.context;
        PrintStream ps = getStream(TBEVENTS_LOG);
        LogWriter lw = new LogWriter(ps, logLineContext.context.syncTime.toDateTime(DateTimeZone.UTC), "record");
        appendEventCommonFields(lw, context, event);
        lw.append(
            "contentid", event.getContentId()
        );
        lw.write();
    }

    @Override
    public void writeSurveyEvent(SurveyEvent event,
        LogLineContext logLineContext) throws IOException
    {
        SyncProcessingContext context = logLineContext.context;
        boolean isAbandoned = event.getIsUseful() == null;
        PrintStream ps = getStream(TBEVENTS_LOG);
        LogWriter lw = new LogWriter(ps, logLineContext.context.syncTime.toDateTime(DateTimeZone.UTC),
            isAbandoned?"survey_abandoned":"survey");
        appendEventCommonFields(lw, context, event);
        lw.append(
            "contentid", event.getContentId());
        if (!isAbandoned) {
            lw.append(
                "isuseful", event.getIsUseful() ? 1 : 0 // as integer, so we can sum()
            );
        }
        lw.write();
    }

    @Override
    public void writeJumpEvent(JumpEvent event, LogLineContext logLineContext) throws IOException {
        SyncProcessingContext context = logLineContext.context;
        PrintStream ps = getStream(TBEVENTS_LOG);
        LogWriter lw = new LogWriter(ps, logLineContext.context.syncTime.toDateTime(DateTimeZone.UTC), "jump_time");
        appendEventCommonFields(lw, context, event);
        lw.append(
            "contentid", event.getContentId(),

            "secondsfrom",event.getSecondsFrom(),
            "secondsto",event.getSecondsTo()
        );
        lw.write();
    }

    @Override
    public void writeFasterEvent(FasterEvent event, LogLineContext logLineContext) throws IOException {
        SyncProcessingContext context = logLineContext.context;
        PrintStream ps = getStream(TBEVENTS_LOG);
        LogWriter lw = new LogWriter(ps, logLineContext.context.syncTime.toDateTime(DateTimeZone.UTC), "faster");
        appendEventCommonFields(lw, context, event);
        lw.write();
    }

    @Override
    public void writeSlowerEvent(SlowerEvent event, LogLineContext logLineContext) throws IOException {
        SyncProcessingContext context = logLineContext.context;
        PrintStream ps = getStream(TBEVENTS_LOG);
        LogWriter lw = new LogWriter(ps, logLineContext.context.syncTime.toDateTime(DateTimeZone.UTC), "slower");
        appendEventCommonFields(lw, context, event);
        lw.write();
    }

    @Override
    public void writeAggregation(SyncAggregation aggregation,
        SyncProcessingContext context) throws IOException
    {
        PrintStream ps = getStream(PLAYED_STATS_LOG);
        LogWriter lw = new LogWriter(ps, context.syncTime.toDateTime(DateTimeZone.UTC), "aggregation",
            "project",context.project,
            "deployment",context.deploymentId.toString(),
            "contentpackage",aggregation.getContentPackage(),
            "community",aggregation.getVillage(),
            "recipientid",context.recipientId,
            "talkingbookid",aggregation.getContentSyncUniqueId().getTalkingBook(),
            "contentid", aggregation.getContentSyncUniqueId().getContentId(),
            "started",aggregation.getCountStarted(),
            "quarter",aggregation.getCountQuarter(),
            "half",aggregation.getCountHalf(),
            "threequarters",aggregation.getCountThreeQuarters(),
            "completed",aggregation.getCountCompleted(),
            "played_seconds",aggregation.getTotalTimePlayed(),
            "survey_taken",aggregation.getCountSurveyTaken(),
            "survey_applied",aggregation.getCountApplied(),
            "survey_useless",aggregation.getCountUseless(),
            "tbcdid",context.deviceSyncedFrom,
            "stats_timestamp",context.syncTime);
        if (context.deploymentTime != null) {
            lw.append("deployment_timestamp",context.deploymentTime);
        }
        if (context.deploymentUuid != null) {
            lw.append("deployment_uuid", context.deploymentUuid);
        }

        // This number has no known utility. Keeping the calculation for historical reasons.
//        double effCompletions = 0.3 *  aggregation.getCountQuarter() +
//                                0.6 *  aggregation.getCountHalf() +
//                                0.83 * aggregation.getCountThreeQuarters() +
//                                       aggregation.getCountCompleted();
//        lw.append("effective_completions",effCompletions);
        lw.write();
    }

    @Override
    public void writeOperationLog(SyncOperationLog operationLog) {
        // This is not valid stats data
    }

    @Override
    public void writeTalkingBookCorruption(TalkingBookCorruption talkingBookCorruption) {
        // This is not valid stats data
    }

    @Override
    public void writeTbDataLog(TbDataLine tbDataLine) {
        // We already create a better "tb data log"
    }

    /**
     * Helper to write key:value pairs to a log file.
     */
    private static class LogWriter {
        PrintStream ps;
        StringBuilder sb;

        /**
         * Initialize the log writer with an output stream, timestamp, and function code
         * @param ps The print stream to receive the log.
         * @param timestamp of when the log data was created, generally when the data is collected
         *                  from the Talking Book
         * @param function being logged
         * @param data zero or more pairs of String:Object
         */
        LogWriter(PrintStream ps, DateTime timestamp, String function, Object... data) {
            this.ps = ps;
            this.sb = new StringBuilder(timestamp.toString()).append(',').append(function);
            append(data);
        }
        public LogWriter log(DateTime timestamp, String function, Object... data) {
            this.sb = new StringBuilder(timestamp.toString()).append(',').append(function);
            append(data);
            return this;
        }
        public LogWriter append(Object... data) {
            if (data.length % 2 != 0) throw new IllegalArgumentException("data must be in pairs");
            for (int ix=0; ix<data.length; ix+=2) {
                if (!(data[ix] instanceof String)) { throw new IllegalArgumentException("data must be 'name,value' pairs, where name is a string"); }
            }
            for (int ix=0; ix<data.length; ix+=2) {
                if (data[ix+1] != null) {
                    sb.append(',').append(data[ix]).append(':');
                    if (data[ix+1] instanceof String) {
                        sb.append('"').append(data[ix + 1]).append('"');
                    } else {
                        sb.append(data[ix+1].toString());
                    }
                }
            }
            return this;
        }
        public void write() {
            ps.println(sb.toString());
            sb = null;
        }
    }

}
