package org.literacybridge.dashboard.processors;

import org.literacybridge.dashboard.aggregation.AggregationOf;
import org.literacybridge.dashboard.aggregation.StatAggregator;
import org.literacybridge.stats.formats.logFile.LogLineContext;

/**
 * Creates aggregations similiar to the ones found in the flashdata.bin files from the logs.
 *
 * This aggregates content + survey information.
 *
 * For the content, the main metric is a histogram of how long the content was played for.  It is broken into:
 *    <ul>
 *      <li>Played for more than 10 seconds</li>
 *      <li>Played for more than 1/4 of the total length</li>
 *      <li>Played for more than 1/2 of the total length</li>
 *      <li>Played for more than 3/4 of the total length</li>
 *      <li>Played to completion</li>
 *    </ul>
 *
 *    It is important to note that this is a histogram, NOT an aggregation.  So a content play that goes for 1/2 of the
 *    length, will NOT register as a 1/4 play in addition to a 1/2 play.
 */
public class LogAggregationProcessor extends AbstractLogProcessor {

  final public StatAggregator aggregator = new StatAggregator();

  final int minPlayedToBeCounted;

  public LogAggregationProcessor(int minPlayedToBeCounted) {
    this.minPlayedToBeCounted = minPlayedToBeCounted;
  }

  public void clear() {
    aggregator.clear();
  }

  @Override
  public void onPlayed(LogLineContext context, String contentId, short secondsPlayed, short secondsSomething,
                       int volume, double voltage, boolean ended) {

    //There are a number of aggregations, and unfortunately different stats from both stats files (which log 10 second plays)
    //and FlashData files, which have it set up in quarter play, half play, three quarters play, etc.

    double fractionPlayed = ((double)secondsPlayed)/((double)secondsSomething);
    if (ended) {
      aggregator.add(context.context.deploymentId, AggregationOf.finishedPlays, contentId, context.context.village,
                     context.context.talkingBookId, 1);
    } else if (fractionPlayed >= .75) {
      aggregator.add(context.context.deploymentId, AggregationOf.threeQuartersPlays, contentId, context.context.village,
                     context.context.talkingBookId, 1);
    } else if (fractionPlayed >= .50) {
      aggregator.add(context.context.deploymentId, AggregationOf.halfPlays, contentId, context.context.village,
                     context.context.talkingBookId, 1);
    } else if (fractionPlayed >= .25) {
      aggregator.add(context.context.deploymentId, AggregationOf.quarterPlays, contentId, context.context.village,
                     context.context.talkingBookId, 1);
    } else if (secondsPlayed >= minPlayedToBeCounted) {
      aggregator.add(context.context.deploymentId, AggregationOf.tenSecondPlays, contentId, context.context.village,
                     context.context.talkingBookId, 1);
    }

    if (secondsPlayed >= minPlayedToBeCounted) {
      aggregator.add(context.context.deploymentId, AggregationOf.totalTimePlayed, contentId, context.context.village,
                     context.context.talkingBookId, secondsPlayed);
    }
  }

  @Override
  public void onSurvey(LogLineContext context, String contentId) {
    aggregator.add(context.context.deploymentId, AggregationOf.surveyTaken, contentId, context.context.village,
                   context.context.talkingBookId, 1);
  }

  @Override
  public void onSurveyCompleted(LogLineContext context, String contentId, boolean useful) {

    if (useful) {
      aggregator.add(context.context.deploymentId, AggregationOf.surveyApplied, contentId, context.context.village,
                     context.context.talkingBookId, 1);
    } else {
      aggregator.add(context.context.deploymentId, AggregationOf.surveyUseless, contentId, context.context.village,
                     context.context.talkingBookId, 1);
    }
  }

}
