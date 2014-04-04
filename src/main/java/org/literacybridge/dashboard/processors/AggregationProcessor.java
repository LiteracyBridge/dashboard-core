package org.literacybridge.dashboard.processors;

import org.literacybridge.dashboard.aggregation.AggregationOf;
import org.literacybridge.dashboard.aggregation.StatAggregator;
import org.literacybridge.stats.model.SyncProcessingContext;
import org.literacybridge.stats.formats.flashData.FlashData;
import org.literacybridge.stats.formats.flashData.NORmsgStats;
import org.literacybridge.stats.formats.logFile.LogLineContext;
import org.literacybridge.stats.formats.statsFile.StatsFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author willpugh
 */
public class AggregationProcessor extends AbstractLogProcessor {

  static protected final Logger logger = LoggerFactory.getLogger(AggregationProcessor.class);


  final public StatAggregator flashDataAggregator = new StatAggregator();
  final public StatAggregator logAggregator = new StatAggregator();
  final public StatAggregator statsAggregator = new StatAggregator();

  final int minPlayedToBeCounted;

  public AggregationProcessor(int minPlayedToBeCounted) {
    this.minPlayedToBeCounted = minPlayedToBeCounted;
  }

  @Override
  public void onPlayed(LogLineContext context, String contentId, short secondsPlayed, short secondsSomething,
                       int volume, double voltage, boolean ended) {

    //There are a number of aggregations, and unfortunately different stats from both stats files (which log 10 second plays)
    //and FlashData files, which have it set up in quarter play, half play, three quarters play, etc.

    if (ended) {
      logAggregator.add(context.context.deploymentId, AggregationOf.tenSecondPlays, contentId, context.context.village,
                     context.context.talkingBookId, 1);
      logAggregator.add(context.context.deploymentId, AggregationOf.finishedPlays, contentId, context.context.village,
                     context.context.talkingBookId, 1);
    } else if (secondsPlayed >= minPlayedToBeCounted) {
      logAggregator.add(context.context.deploymentId, AggregationOf.tenSecondPlays, contentId, context.context.village,
                     context.context.talkingBookId, 1);
    }

    //Now do FlashData style aggregations for quarter, half and three-quarters plays.
    double fractionPlayed = ((double)secondsPlayed)/((double)secondsSomething);
    if (fractionPlayed >= .25) {
      logAggregator.add(context.context.deploymentId, AggregationOf.quarterPlays, contentId, context.context.village,
                     context.context.talkingBookId, 1);
    }

    if (fractionPlayed >= .50) {
      logAggregator.add(context.context.deploymentId, AggregationOf.halfPlays, contentId, context.context.village,
                     context.context.talkingBookId, 1);
    }

    if (fractionPlayed >= .75) {
      logAggregator.add(context.context.deploymentId, AggregationOf.threeQuartersPlays, contentId,
                     context.context.village,
                     context.context.talkingBookId, 1);
    }

    logAggregator.add(context.context.deploymentId, AggregationOf.totalTimePlayed, contentId, context.context.village,
                   context.context.talkingBookId, secondsPlayed);


  }

  @Override
  public void onSurvey(LogLineContext context, String contentId) {
    logAggregator.add(context.context.deploymentId, AggregationOf.surveyTaken, contentId, context.context.village,
                   context.context.talkingBookId, 1);
  }

  @Override
  public void onSurveyCompleted(LogLineContext context, String contentId, boolean useful) {

    if (useful) {
      logAggregator.add(context.context.deploymentId, AggregationOf.surveyApplied, contentId, context.context.village,
                     context.context.talkingBookId, 1);
    } else {
      logAggregator.add(context.context.deploymentId, AggregationOf.surveyUseless, contentId, context.context.village,
                     context.context.talkingBookId, 1);
    }
  }

  @Override
  public void processStatsFile(SyncProcessingContext context, String contentId, StatsFile statsFile) {
    statsAggregator.add(context.deploymentId, AggregationOf.tenSecondPlays, contentId, context.village,
                   context.talkingBookId, statsFile.openCount);
    statsAggregator.add(context.deploymentId, AggregationOf.finishedPlays, contentId, context.village,
                   context.talkingBookId, statsFile.completionCount);
    statsAggregator.add(context.deploymentId, AggregationOf.surveyTaken, contentId, context.village,
                   context.talkingBookId, statsFile.surveyCount);

    statsAggregator.add(context.deploymentId, AggregationOf.surveyApplied, contentId, context.village,
                   context.talkingBookId, statsFile.appliedCount);
    statsAggregator.add(context.deploymentId, AggregationOf.surveyUseless, contentId, context.village,
                   context.talkingBookId, statsFile.uselessCount);
  }

  @Override
  public void markStatsFileAsCorrupted(SyncProcessingContext context, String contentId, String errorMessage) {
    statsAggregator.add(context.deploymentId, AggregationOf.corruptedFiles, contentId, context.village,
                   context.talkingBookId, 1);
  }

  @Override
  public void processFlashData(SyncProcessingContext context, FlashData flashData) {

    for (NORmsgStats stats : flashData.allStats()) {

      if (stats.getContentId() == null) {
        logger.error("Invalid stats entry with null contentId.  Context=" + context + " stats indexMsg=" + stats.getIndexMsg());
        continue;
      }

      flashDataAggregator.add(context.deploymentId, AggregationOf.startedPlays, stats.getContentId(), context.village,
                              context.talkingBookId, stats.getCountStarted());
      flashDataAggregator.add(context.deploymentId, AggregationOf.quarterPlays, stats.getContentId(), context.village,
                              context.talkingBookId, stats.getCountQuarter());
      flashDataAggregator.add(context.deploymentId, AggregationOf.halfPlays, stats.getContentId(), context.village,
                              context.talkingBookId, stats.getCountHalf());
      flashDataAggregator.add(context.deploymentId, AggregationOf.threeQuartersPlays, stats.getContentId(), context.village,
                              context.talkingBookId, stats.getCountThreequarters());
      flashDataAggregator.add(context.deploymentId, AggregationOf.finishedPlays, stats.getContentId(), context.village,
                              context.talkingBookId, stats.getCountCompleted());
      flashDataAggregator.add(context.deploymentId, AggregationOf.surveyApplied, stats.getContentId(), context.village,
                              context.talkingBookId, stats.getCountApplied());
      flashDataAggregator.add(context.deploymentId, AggregationOf.surveyUseless, stats.getContentId(), context.village,
                              context.talkingBookId, stats.getCountUseless());
      flashDataAggregator.add(context.deploymentId, AggregationOf.totalTimePlayed, stats.getContentId(), context.village,
                              context.talkingBookId, stats.getTotalSecondsPlayed());
    }
  }


}
