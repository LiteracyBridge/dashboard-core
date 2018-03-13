package org.literacybridge.stats.formats.logFile;

import com.google.common.collect.Lists;
import org.joda.time.LocalTime;
import org.literacybridge.stats.api.TalkingBookDataProcessor;
import org.literacybridge.stats.model.SyncProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author willpugh
 */
public class LogFileParser {

  /**
   * Matches a line that we are able to process.  Will try to be fairly forgiving in terms of what it accepts.
   * For example, the line:
   * 2r0096c008p023d18h18m53s401/314/314V:PLAY TB000248_372AB558 @VOL=03 @Volt=314
   * <p/>
   * Will be captured in three parts (brackets used to denote captures):
   * <p/>
   * [2r0096c008p023d18h18m53s401/314/314V]:[PLAY] [TB000248_372AB558 @VOL=03 @Volt=314]
   */
  public static final Pattern LOG_LINE_PATTERN = Pattern.compile("([^:]*):(\\w+):*\\s*(.*)");
  /**
   * This matches the beginning of log lines that look like :
   * 2r0096c008p023d18h18m49s357/314/314V
   * <p/>
   * and capture each piece of data.  For example the brackets will denote the captured text. . .
   * <p/>
   * [2]r[0096]c[008]p[023]d[18]h[18]m[49]s[357]/[314]/[314]V
   * <p/>
   * Capture groups are:
   * <ol>
   * <li>Household rotation</li>
   * <li>What power cycle the device is in (how many times its been turned on and off)</li>
   * <li>What period the device is in (count of continuous time blocks, which break when power is lost, usually during battery change)</li>
   * <li>Day in period</li>
   * <li>Hours of Day in period</li>
   * <li>Minutes of Hour in period</li>
   * <li>Seconds of Minute in period</li>
   * <li>Highest Transient Voltage Detected</li>
   * <li>Steady State Voltage</li>
   * <li>Lowest Transient Voltage Detected</li>
   * </ol>
   */
  public static final Pattern LOG_LINE_START_PATTERN = Pattern.compile(
    "(0|(\\d+)r)(\\d+)c(\\d+)p\\D*(\\d+)d(\\d+)h(\\d+)m(\\d+)s(\\d+)/(\\d+)/(\\d+)V");
  /**
   * This matches the newer log line format that begins with a 0p
   */
  public static final Pattern NEW_LINE_PATTERN = Pattern.compile("\\d+p(.*)");
  /**
   * Matches the rest of the PLAY event, after the parts are pulled out from LOG_LINE_PATTERN +  LOG_LINE_START_PATTERN
   * <p/>
   * This will turn something that looks like:
   * 00046a_9_4E7A864E @VOL=03 @Volt=250
   * <p/>
   * And capture each important piece (brackets used to denote captures):
   * [00046a_9_4E7A864E] @VOL=[03] @Volt=[250]
   */
  public static final Pattern REST_OF_PLAY = Pattern.compile("(\\S+)\\s+@VOL=(\\d+)\\s+@Volt=(\\S+)\\s*");
  /**
   * Matches the rest of the PLAYED event, after the parts are pulled out from LOG_LINE_PATTERN +  LOG_LINE_START_PATTERN
   * <p/>
   * This will turn something that looks like:
   * 00046a_9_F70FDD0B 0002/0001sec @VOL=01 @Volt=202-Ended
   * <p/>
   * And capture each important piece (brackets used to denote captures):
   * [00046a_9_F70FDD0B] [0002]/[0001]sec @VOL=[01] @Volt=[202-Ended]
   */
  public static final Pattern REST_OF_PLAYED = Pattern.compile(
    "(\\S+)\\s+(\\d+)/(\\d+)sec\\s+@VOL=(\\d+)\\s+@Volt=(\\S+)\\s*");
  /**
   * Matches the rest of the RECORD event, after the parts are pulled out from LOG_LINE_PATTERN +  LOG_LINE_START_PATTERN
   * <p/>
   * This will turn something that looks like:
   * 00046a_9_7F67F127 -> 9
   * 00046a_9_7F67F127 -> $1-0
   * <p/>
   * And capture each important piece (brackets used to denote captures):
   * [00046a_9_7F67F127] -> [9]
   * [00046a_9_7F67F127] -> [$1-0]
   *
   * Test in the debugger like this: Pattern.compile("(\\S+)\\s+->\\s+(\\$?\\d+(?:-\\d)*)\\s*").matcher(args).matches()
   */
  public static final Pattern REST_OF_RECORD = Pattern.compile("(\\S+)\\s+->\\s+(\\$?\\d+(?:-\\d)*)\\s*");
  /**
   * Matches the rest of the TIME RECORD event, after the parts are pulled out from LOG_LINE_PATTERN +  LOG_LINE_START_PATTERN
   * <p/>
   * This will turn something that looks like:
   * RECORDED (secs): 0004
   * <p/>
   * And capture each important piece (brackets used to denote captures):
   * RECORDED (secs): [0004]
   */
  public static final Pattern REST_OF_RECORDED = Pattern.compile("RECORDED\\s+\\(secs\\):\\s*(\\d+)\\s*");

    /**
     * matches the rest of the JUMP_TIME event. Matches
     * JUMP_TIME:0132->0125
     * and extracts the two sequence points.
     */
    public static final Pattern REST_OF_JUMP_TIME = Pattern.compile("JUMP_TIME:(\\d+)->(\\d+)");
  /**
   * Matches the pattern for a VOLTAGE DROP.  This can happen during any of the other patterns
   * <p/>
   * This will turn something that looks like:
   * VOLTAGE DROP: 0.02v in 0003 sec
   * <p/>
   * And capture each important piece (brackets used to denote captures):
   * VOLTAGE DROP: [0.02]v in [0003] sec
   */
  private static final Pattern VOLTAGE_DROP = Pattern.compile("VOLTAGE DROP:\\s*([0-9.]+)v\\s*in\\s*(\\d+)\\s+sec");
  static protected final Logger logger = LoggerFactory.getLogger(LogFileParser.class);
  private final Collection<TalkingBookDataProcessor> eventProcessors;
  private final SyncProcessingContext context;

  //Last piece of content played
  private String contentLastPlayed = "";
    private String currentRawLine;
    private String currentAction;
    private String currentParams;

    public LogFileParser(TalkingBookDataProcessor eventProcessors, SyncProcessingContext context) {
    this.eventProcessors = Lists.newArrayList(eventProcessors);
    this.context = context;
  }

  public LogFileParser(
      Collection<TalkingBookDataProcessor> eventProcessors,
      SyncProcessingContext context) {
    this.eventProcessors = eventProcessors;
    this.context = context;
  }

  private static boolean checkForMatch(String action, String args, LogFilePosition filePosition,
                                       Matcher matcher) {
    if (!matcher.matches()) {

      //If this is not a feedback message, mark as being an error
      if (!"Feedback".equalsIgnoreCase(args)) {
        final String errorString = String.format("%s : %d - Cannot match arguments in %s action. Args=%s",
          filePosition.loggingFileName(), filePosition.lineNumber, action, args);
        logger.trace(errorString);
      }

      return false;
    }
    return true;
  }

  private void clearParseState() {
    contentLastPlayed = "";
  }

  public String getContentLastPlayed() {
    return contentLastPlayed;
  }

  public int parse(final String fileName, final InputStream is) throws IOException {
    int numErrors = 0;
    int lineNumber = 1;
    final BufferedReader br = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));

    clearParseState();
    for (TalkingBookDataProcessor eventProcessor : eventProcessors) {
      eventProcessor.onLogFileStart(fileName);
    }

    try {
        while ((currentRawLine = br.readLine()) != null) {

        final Matcher fullLineMatcher = LOG_LINE_PATTERN.matcher(currentRawLine);
        if (fullLineMatcher.matches()) {

          final String preludeString = fullLineMatcher.group(1);
          currentAction = fullLineMatcher.group(2);
          currentParams = fullLineMatcher.group(3);

          if (!parseAction(fileName, lineNumber, preludeString, currentAction, currentParams,
                           currentRawLine)) {
              numErrors++;
          }
        }

        lineNumber++;
      }
    } finally {
      for (TalkingBookDataProcessor eventProcessor : eventProcessors) {
        eventProcessor.onLogFileEnd();
      }
    }
    return numErrors;
  }

  private LogLineContext parseLogLineContext(String fileName, int lineNumber, String line) {

    final LogFilePosition logFilePosition = new LogFilePosition(fileName, lineNumber);

    LogLineInfo logLineInfo = null;
    try {
      logLineInfo = parseLogLineInfo(line);
    } catch (NumberFormatException e) {
      final String errorString = String.format("%s : %d - Invalid number in log info. Line=%s, Error=%s", fileName,
        lineNumber, line, e.getMessage());
      logger.trace(errorString);
    }

    return new LogLineContext(logLineInfo, logFilePosition, context);
  }

  public LogLineInfo parseLogLineInfo(String line) throws NumberFormatException {

    //The line pattern seems to have changed to include a 0p at the beginning.
    //This catches that.
    Matcher checkOldLine = NEW_LINE_PATTERN.matcher(line);
    if (checkOldLine.matches()) {
      line = checkOldLine.group(1);
    }

    Matcher matcher = LOG_LINE_START_PATTERN.matcher(line);
    if (!matcher.matches()) {
      return null;
    }

    short rotation = 0;
    if (!"0".matches(matcher.group(1))) {
      rotation = Short.parseShort(matcher.group(2));
    }

    final short cycle = Short.parseShort(matcher.group(3));
    final short period = Short.parseShort(matcher.group(4));

    short dayOfPeriod = Short.parseShort(matcher.group(5));
    int hourOfPeriod = Integer.parseInt(matcher.group(6));
    int minuteOfPeriod = Integer.parseInt(matcher.group(7));
    int secondOfPeriod = Integer.parseInt(matcher.group(8));
    final double highestVoltage = Double.parseDouble(matcher.group(9)) / 100;
    final double steadyStateVoltage = Double.parseDouble(matcher.group(10)) / 100;
    final double lowestVoltage = Double.parseDouble(matcher.group(11)) / 100;

    //Somehow, there appear to be log messages with hour of day > 24 and minutes > 60

    minuteOfPeriod += secondOfPeriod / 60;
    secondOfPeriod = secondOfPeriod % 60;
    hourOfPeriod += minuteOfPeriod / 60;
    minuteOfPeriod = minuteOfPeriod % 60;
    dayOfPeriod += hourOfPeriod / 24;
    hourOfPeriod = hourOfPeriod % 24;

    final LocalTime periodTime = new LocalTime(hourOfPeriod, minuteOfPeriod, secondOfPeriod);

    return new LogLineInfo(rotation, cycle, period, dayOfPeriod, periodTime, highestVoltage, steadyStateVoltage,
      lowestVoltage);
  }

  private boolean parseAction(final String fileName, final int lineNumber,
                              final String preludeString, final String action,
                              final String actionParams, final String rawLine) {

    final LogLineContext logLineContext = parseLogLineContext(fileName, lineNumber, preludeString);
    final Matcher voltageMatcher = VOLTAGE_DROP.matcher(actionParams);
    final boolean isVoltageDrop = voltageMatcher.matches();
    final LogAction logAction = LogAction.lookup(action);

    if (logAction == null) {
        // There are thousands of these. The logs from the talking books are extremely noisy and
        // full of corruption, as well as log lines that legitimately do not match.
        logger.trace(String.format("Invalid action '%s'", action));
        // If we call everything that isn't a good action "corrupt", every file will be corrupt.
        return true;
    }
    boolean result = true;

    if (!isVoltageDrop) {

      switch (logAction) {

        case play:
          result = processPlay(logLineContext, actionParams);
          break;

      case playing:
          // Nothing to do for these.
          break;

      case played:
          result = processPlayed(logLineContext, actionParams);
          break;

        case category:
            result = processCategory(logLineContext, actionParams);
          break;

        case paused:
          for (TalkingBookDataProcessor eventProcessor : eventProcessors) {
            eventProcessor.onPause(logLineContext, contentLastPlayed);
          }
          break;

        case unpaused:
          for (TalkingBookDataProcessor eventProcessor : eventProcessors) {
            eventProcessor.onUnPause(logLineContext, contentLastPlayed);
          }
          break;

        case record:
            result = processRecord(logLineContext, actionParams);
          break;

        case time_recorded:
            result = processRecorded(logLineContext, actionParams);
          break;

        case survey:
            result = processSurvey(logLineContext, actionParams);
          break;

        case shuttingDown:
          for (TalkingBookDataProcessor eventProcessor : eventProcessors) {
            eventProcessor.onShuttingDown(logLineContext);
          }
          break;

      case jump_time:
          result = processJumpTime(logLineContext, actionParams);
          break;

        default:
          logger.error("Illegal action found " + logAction.actionName);
          result = false;
          break;
      }

    } else {
        try {
            final double voltsDropped = Double.parseDouble(voltageMatcher.group(1));
            final int time = Integer.parseInt(voltageMatcher.group(2));
            for (TalkingBookDataProcessor eventProcessor : eventProcessors) {
                eventProcessor.onVoltageDrop(logLineContext, logAction, voltsDropped, time);
            }
        } catch (NumberFormatException e) {
            result = false;
        }
      }
      return result;
  }

  protected boolean processPlay(LogLineContext logLineContext, String args) {
    final Matcher matcher = REST_OF_PLAY.matcher(args);
    if (!checkForMatch("Play", args, logLineContext.logFilePosition, matcher)) {
      return false;
    }


    final String contentId = matcher.group(1);
    contentLastPlayed = contentId;

    try {
      final int volume = Integer.parseInt(matcher.group(2));
      final double voltage = Double.parseDouble(matcher.group(3)) / 100;
      for (TalkingBookDataProcessor eventProcessor : eventProcessors) {
        eventProcessor.onPlay(logLineContext, contentId, volume, voltage);
      }
    } catch (NumberFormatException e) {
      final String errorString = String.format("%s : %d - Invalid number in Play action. Args=%s, Error=%s",
        logLineContext.logFilePosition.loggingFileName(),
        logLineContext.logFilePosition.lineNumber, args, e.getMessage());
      logger.trace(errorString);
      return false;
    }
      return true;
  }

  protected boolean processPlayed(LogLineContext logLineContext, String args) {
    final Matcher matcher = REST_OF_PLAYED.matcher(args);
    if (!checkForMatch("Played", args, logLineContext.logFilePosition, matcher)) {
      return false;
    }


    final String contentId = matcher.group(1);
    contentLastPlayed = contentId;

    try {
      final short timePlayed = Short.parseShort(matcher.group(2));
      final short timeSomething = Short.parseShort(matcher.group(3));
      final int volume = Integer.parseInt(matcher.group(4));
      final String voltageString = matcher.group(5);

      final String[] voltageParts = voltageString.split("-");
      final double voltage = Double.parseDouble(voltageParts[0]) / 100;
      final boolean isEnded = (voltageParts.length == 2) && (voltageParts[1].equalsIgnoreCase("ended"));

      for (TalkingBookDataProcessor eventProcessor : eventProcessors) {
        eventProcessor.onPlayed(logLineContext, contentId, timePlayed, timeSomething, volume, voltage, isEnded);
      }
    } catch (NumberFormatException e) {
      final String errorString = String.format("%s : %d - Invalid number in Played action. Args=%s, Error=%s",
        logLineContext.logFilePosition.loggingFileName(),
        logLineContext.logFilePosition.lineNumber, args, e.getMessage());
      logger.trace(errorString);
      return false;
    }
    return true;
  }

  protected boolean processCategory(final LogLineContext logLineContext, final String categoryId) {
    for (TalkingBookDataProcessor eventProcessor : eventProcessors) {
      eventProcessor.onCategory(logLineContext, categoryId);
    }
    return true;
  }

  protected boolean processRecord(LogLineContext logLineContext, String args) {

    // "Record" (vs "RECORD") records are not really log records, but rather are comments.
    if (currentAction.equals("Record")) {
        return true;
    }

    //There are several "record" messages that don't have args.  Not much we
    //can do with them.
    if (args.isEmpty()) {
      return false;
    }

    final Matcher matcher = REST_OF_RECORD.matcher(args);
    if (!checkForMatch("Record", args, logLineContext.logFilePosition, matcher)) {
      return false;
    }


    final String contentId = matcher.group(1);

    try {
      // This code parses the category as an integer, and then passes that value on. At best,
      // of course, that would give the top category. The value isn't actually used for anything,
      // so there's no real damage done here.
      // TODO: fix it anyway.
      int meaningless = 0;
      try {
        meaningless = Integer.parseInt(matcher.group(2));
      } catch (Exception ex) {
          // Ignore.
      }
      for (TalkingBookDataProcessor eventProcessor : eventProcessors) {
        eventProcessor.onRecord(logLineContext, contentId, meaningless);
      }
    } catch (NumberFormatException e) {
      final String errorString = String.format("%s : %d - Invalid number in Record action. Args=%s, Error=%s",
        logLineContext.logFilePosition.loggingFileName(),
        logLineContext.logFilePosition.lineNumber, args, e.getMessage());
      logger.trace(errorString);
      return false;
    }
    return true;
  }

  protected boolean processRecorded(LogLineContext logLineContext, String args) {
    final Matcher matcher = REST_OF_RECORDED.matcher(args);
    if (!checkForMatch("Recorded", args, logLineContext.logFilePosition, matcher)) {
      return false;
    }

    try {
      final int time = Integer.parseInt(matcher.group(1));
      for (TalkingBookDataProcessor eventProcessor : eventProcessors) {
        eventProcessor.onRecorded(logLineContext, time);
      }
    } catch (NumberFormatException e) {
      final String errorString = String.format("%s : %d - Invalid number in Record action. Args=%s, Error=%s",
        logLineContext.logFilePosition.loggingFileName(),
        logLineContext.logFilePosition.lineNumber, args, e.getMessage());
      logger.trace(errorString);
      return false;
    }
    return true;
  }

    protected boolean processJumpTime(LogLineContext logLineContext, String args) {
        final Matcher matcher = REST_OF_JUMP_TIME.matcher(args);
        if (!checkForMatch("Jump_time", args, logLineContext.logFilePosition, matcher)) {
            return false;
        }

        try {
            final int timeFrom = Integer.parseInt(matcher.group(1));
            final int timeTo = Integer.parseInt(matcher.group(2));
            for (TalkingBookDataProcessor eventProcessor : eventProcessors) {
                eventProcessor.onJumpTime(logLineContext, timeFrom, timeTo);
            }
        } catch (NumberFormatException e) {
            final String errorString = String.format("%s : %d - Invalid number in JUMP_TIME action. Args=%s, Error=%s",
                logLineContext.logFilePosition.loggingFileName(),
                logLineContext.logFilePosition.lineNumber, args, e.getMessage());
            logger.trace(errorString);
            return false;
        }
        return true;
    }

    protected boolean processSurvey(LogLineContext logLineContext, String args) {
    if (args == null) {
      final String errorString = String.format("%s : %d - No argument for Survey action.",
        logLineContext.logFilePosition.loggingFileName(),
        logLineContext.logFilePosition.lineNumber);
      logger.trace(errorString);
      return false;
    }

    if ("taken".equalsIgnoreCase(args)) {
      for (TalkingBookDataProcessor eventProcessor : eventProcessors) {
        eventProcessor.onSurvey(logLineContext, getContentLastPlayed());
      }
    } else if ("apply".equalsIgnoreCase(args)) {
      for (TalkingBookDataProcessor eventProcessor : eventProcessors) {
        eventProcessor.onSurveyCompleted(logLineContext, getContentLastPlayed(), true);
      }
    } else if ("useless".equalsIgnoreCase(args)) {
      for (TalkingBookDataProcessor eventProcessor : eventProcessors) {
        eventProcessor.onSurveyCompleted(logLineContext, getContentLastPlayed(), false);
      }
    } else {
      final String errorString = String.format("%s : %d - Invalid argument for Surveyaction. Args=%s",
        logLineContext.logFilePosition.loggingFileName(),
        logLineContext.logFilePosition.lineNumber, args);
      logger.trace(errorString);
      return false;
    }
    return true;
  }
}
