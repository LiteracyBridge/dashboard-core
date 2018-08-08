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

  // Play times of less than this are considered to be just flipping through the messages, not an
  // actual playback.
  static final int minSecondsPlayedToConsiderPlaying = 10;
  // Playing within this much of the end is considered finishing the message.
  static final int maxSecondsRemainingToConsiderFinished = 2;

  public LogAggregationProcessor() {
  }

  public void clear() {
    aggregator.clear();
  }

  @Override
  public void onPlayed(LogLineContext context, String contentId, short secondsPlayed, short secondsSomething,
                       int volume, double voltage, boolean ended) {

    //There are a number of aggregations, and unfortunately different stats from both stats files (which log 10 second plays)
    //and FlashData files, which have it set up in quarter play, half play, three quarters play, etc.

    // FlashData stats are FIRST match of:
    // finished: position > total_length - 2000 (milliseconds)
    // 3/4:      position > .75 * total_length
    // 1/2:      position > total_length / 2
    // 1/4:      position > total_length / 4
    // started:  position >= 10 seconds
    // else no stat recorded
    //
    // applied:  count of # times user indicated they would apply a message
    // useless:  count of # times user indicated a message is useless
    // totalSeconds: cumulative # seconds message was played (in each rotation), to a max of 65,535 seconds
    //                 (per TB, so up to 18 hours per tb (per rotation) is recorded).

    // Stats files are ALL of:
    // finished: position all bytes in file
    // started:  position > 10 seconds

    // Log stats record the elapsed time playing less any time spent paused. 
    // Log stats log '-Ended' if playback ended by itself (ie, ran out of bytes to play).

    // Log stats do NOT account for skipping forward or backward, nor do they account for playing faster
    // or slower.

    // Not only are the stats apples, oranges, and peaches, but none of them are reliable,
    // so it is not even possible to prefer one over another as "more accurate".

    double fractionPlayed = ((double)secondsPlayed)/((double)secondsSomething);
    boolean nearlyFinished = (secondsSomething - secondsPlayed) <= maxSecondsRemainingToConsiderFinished;
    if (ended || nearlyFinished) {
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
    } else if (secondsPlayed >= minSecondsPlayedToConsiderPlaying) {
      aggregator.add(context.context.deploymentId, AggregationOf.tenSecondPlays, contentId, context.context.village,
                     context.context.talkingBookId, 1);
    }

    if (secondsPlayed >= minSecondsPlayedToConsiderPlaying) {
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
