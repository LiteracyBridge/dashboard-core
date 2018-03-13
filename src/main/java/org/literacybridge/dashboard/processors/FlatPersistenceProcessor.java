package org.literacybridge.dashboard.processors;

import com.google.common.collect.Lists;
import org.literacybridge.dashboard.aggregation.Aggregations;
import org.literacybridge.dashboard.aggregation.UpdateAggregations;
import org.literacybridge.dashboard.api.TalkingBookSyncWriter;
import org.literacybridge.dashboard.dbTables.contentUsage.ContentSyncUniqueId;
import org.literacybridge.dashboard.dbTables.contentUsage.SyncAggregation;
import org.literacybridge.stats.formats.flashData.FlashData;
import org.literacybridge.stats.formats.flashData.NORmsgStats;
import org.literacybridge.stats.formats.logFile.LogLineContext;
import org.literacybridge.stats.formats.statsFile.StatsFile;
import org.literacybridge.stats.model.SyncProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.literacybridge.dashboard.aggregation.AggregationOf.finishedPlays;
import static org.literacybridge.dashboard.aggregation.AggregationOf.halfPlays;
import static org.literacybridge.dashboard.aggregation.AggregationOf.quarterPlays;
import static org.literacybridge.dashboard.aggregation.AggregationOf.surveyApplied;
import static org.literacybridge.dashboard.aggregation.AggregationOf.surveyTaken;
import static org.literacybridge.dashboard.aggregation.AggregationOf.surveyUseless;
import static org.literacybridge.dashboard.aggregation.AggregationOf.tenSecondPlays;
import static org.literacybridge.dashboard.aggregation.AggregationOf.threeQuartersPlays;
import static org.literacybridge.dashboard.aggregation.AggregationOf.totalTimePlayed;

/**
 * @author bill
 */
public class FlatPersistenceProcessor extends AbstractPersistenceProcessor {
    static protected final Logger logger = LoggerFactory.getLogger(FlatPersistenceProcessor.class);

    private Map<String, SyncAggregation> mergedStats = new HashMap<>();
    private int valuesUpdated = 0, valuesKept = 0;

    public FlatPersistenceProcessor(TalkingBookSyncWriter writer) {
        this(Lists.<TalkingBookSyncWriter>newArrayList(writer));
    }

    public FlatPersistenceProcessor(Collection<TalkingBookSyncWriter> writers)
    {
        super(writers);
    }

    @Override
    public void onSyncProcessingStart(SyncProcessingContext context) {
        logAggregations.clear();
        mergedStats.clear();
        valuesKept = valuesUpdated = 0;
    }

    /**
     * Lazy allocate a SyncAggregation object for a given content id.
     * @param contentId for which the SyncAggregation is desired.
     * @param context Context in which the content id was seen
     * @return new or previously existing SyncAggregation for the content id.
     */
    private SyncAggregation getAggregationForContent(String contentId,
        SyncProcessingContext context)
    {
        ContentSyncUniqueId contentSyncUniqueId = ContentSyncUniqueId.createFromContext(contentId,
            context,
            SyncAggregation.Source.MERGED.value);
        SyncAggregation stats = mergedStats.get(contentId);
        if (stats == null) {
            stats = new SyncAggregation();
            stats.setContentSyncUniqueId(contentSyncUniqueId);
            stats.setContentPackage(context.contentPackage);
            stats.setVillage(context.village);
            mergedStats.put(contentId, stats);
        }

        return stats;
    }

    private int maxer(int newValue, int prevValue) {
        if (newValue > prevValue) {
            valuesUpdated++;
            return newValue;
        }
        valuesKept++;
        return prevValue;
    }

    @Override
    public void onSyncProcessingEnd(SyncProcessingContext context) {
        // Find all of the content mentioned. 

        final UpdateAggregations updateAggregations = logAggregations.aggregator.perUpdateAggregations
            .get(context.deploymentId);
        if (updateAggregations != null) {
            final Map<String, Aggregations> contentAggregations = updateAggregations.contentAggregations;
            for (String contentId : contentAggregations.keySet()) {
                final Aggregations ca = contentAggregations.get(contentId);
                // If there aren't any interesting stats for this content, skip it.
                if (ca.get(surveyTaken)==0 && ca.get(surveyApplied)==0 &&
                    ca.get(surveyUseless)==0 && ca.get(tenSecondPlays)==0 &&
                    ca.get(quarterPlays)==0 && ca.get(halfPlays)==0 &&
                    ca.get(threeQuartersPlays)==0 && ca.get(finishedPlays)==0 &&
                    ca.get(totalTimePlayed)==0 )
                    continue;
                
                SyncAggregation stats = getAggregationForContent(contentId, context);

                stats.setCountSurveyTaken(maxer(ca.get(surveyTaken),
                    stats.getCountSurveyTaken()));
                stats.setCountApplied(maxer(ca.get(surveyApplied),
                    stats.getCountApplied()));
                stats.setCountUseless(maxer(ca.get(surveyUseless),
                    stats.getCountUseless()));

                stats.setCountStarted(maxer(ca.get(tenSecondPlays),
                    stats.getCountStarted()));
                stats.setCountQuarter(maxer(ca.get(quarterPlays),
                    stats.getCountQuarter()));
                stats.setCountHalf(maxer(ca.get(halfPlays),
                    stats.getCountHalf()));
                stats.setCountThreeQuarters(maxer(ca.get(threeQuartersPlays),
                    stats.getCountThreeQuarters()));
                stats.setCountCompleted(maxer(ca.get(finishedPlays),
                    stats.getCountCompleted()));
                stats.setTotalTimePlayed(maxer(ca.get(totalTimePlayed),
                    stats.getTotalTimePlayed()));
            }
        }
        
//        List<SyncAggregation> filtered = new ArrayList<>();
//        for (SyncAggregation sa : mergedStats.values()) {
//            if (sa.getCountStarted()>0 || sa.getCountQuarter()>0 || sa.getCountHalf()>0 ||
//                sa.getCountThreeQuarters()>0 || sa.getCountCompleted()>0 || sa.getTotalTimePlayed()>0 ||
//                sa.getCountSurveyTaken()>0 || sa.getCountApplied()>0 || sa.getCountUseless()>0) {
//                filtered.add(sa);
//            }
//        }

        for (SyncAggregation sa : mergedStats.values()) {
            for (TalkingBookSyncWriter writer : writers) {
                try {
                    writer.writeAggregation(sa, context);
                } catch (IOException e) {
                    logger.error(e.getLocalizedMessage(), e);
                }
            }
        }

        mergedStats.clear();
        logAggregations.clear();
        valuesKept = valuesUpdated = 0;
    }

    @Override
    public void processFlashData(SyncProcessingContext context, FlashData flashData)
        throws IOException
    {

        for (NORmsgStats msgStats : flashData.allStats()) {
            String contentId = msgStats.getContentId();
            if (contentId == null) continue;

            SyncAggregation stats = getAggregationForContent(contentId, context);

            stats.setCountApplied(maxer(msgStats.getCountApplied(), stats.getCountApplied()));
            stats.setCountUseless(maxer(msgStats.getCountUseless(), stats.getCountUseless()));
            stats.setCountStarted(maxer(msgStats.getCountStarted(), stats.getCountStarted()));
            stats.setCountQuarter(maxer(msgStats.getCountQuarter(), stats.getCountQuarter()));
            stats.setCountHalf(maxer(msgStats.getCountHalf(), stats.getCountHalf()));
            stats.setCountThreeQuarters(maxer(msgStats.getCountThreequarters(),
                stats.getCountThreeQuarters()));
            stats.setCountCompleted(maxer(msgStats.getCountCompleted(), stats.getCountCompleted()));
            stats.setTotalTimePlayed(maxer(msgStats.getTotalSecondsPlayed(),
                stats.getTotalTimePlayed()));
        }
    }

    @Override
    public void processStatsFile(SyncProcessingContext context,
        String contentId,
        StatsFile statsFile)
    {
        SyncAggregation stats = getAggregationForContent(contentId, context);

        stats.setCountSurveyTaken(maxer(statsFile.surveyCount, stats.getCountSurveyTaken()));
        stats.setCountApplied(maxer(statsFile.appliedCount, stats.getCountApplied()));
        stats.setCountUseless(maxer(statsFile.uselessCount, stats.getCountUseless()));
        int started = statsFile.openCount - statsFile.completionCount;
        stats.setCountStarted(maxer(started, stats.getCountStarted()));
        stats.setCountCompleted(maxer(statsFile.completionCount, stats.getCountCompleted()));
    }

    @Override
    public void onPlay(LogLineContext context, String contentId, int volume, double voltage) {
        super.onPlay(context, contentId, volume, voltage);
    }

    @Override
    public void onPlayed(LogLineContext context,
        String contentId,
        short secondsPlayed,
        short secondsSomething,
        int volume,
        double voltage,
        boolean ended)
    {
        super.onPlayed(context, contentId, secondsPlayed, secondsSomething, volume, voltage, ended);
    }

}
