package org.literacybridge.stats.model;

import org.joda.time.LocalDateTime;
import org.literacybridge.stats.DirectoryIterator;

import java.util.Comparator;
import java.util.regex.Matcher;

/**
 */
public class SyncDirId /*implements Comparable<SyncDirId>*/ {

  public static final int SYNC_VERSION_1 = 1;
  public static final int SYNC_VERSION_2 = 2;
  public final LocalDateTime dateTime;
  public final String dirName;
  public final String uniquifier;
  public static final Comparator<SyncDirId> TIME_COMPARATOR = new Comparator<SyncDirId>() {
    @Override
    public int compare(SyncDirId o1, SyncDirId o2) {
      if (o1 == o2) {
        return 0;
      }
      if (o1.dateTime == null && o2.dateTime != null) {
        return -1;
      }

      if (o2.dateTime == null && o1.dateTime != null) {
        return 1;
      }

      int retVal = o1.dateTime.compareTo(o2.dateTime);
      if (retVal == 0) {
        retVal = o1.uniquifier.compareTo(o2.uniquifier);
      }
      return retVal;
    }
  };
  public final int version;


  protected SyncDirId(LocalDateTime dateTime, String dirName, String uniquifier, int version) {
    this.dateTime = dateTime;
    this.dirName = dirName;
    this.uniquifier = uniquifier;
    this.version = version;
  }

  public static SyncDirId parseSyncDir(DeploymentId deploymentId, String syncDirName) {

    SyncDirId retVal = null;
    Matcher matchv2 = DirectoryIterator.SYNC_TIME_PATTERN_V2.matcher(syncDirName);
    if (matchv2.matches()) {

      LocalDateTime dateTime = new LocalDateTime(Integer.parseInt(matchv2.group(1)),  // year
        Integer.parseInt(matchv2.group(2)),     // month
        Integer.parseInt(matchv2.group(3)),     // day
        Integer.parseInt(matchv2.group(4)),     // hour
        Integer.parseInt(matchv2.group(5)),     // minute
        Integer.parseInt(matchv2.group(6)));    // second
        // (7) is the tbcd id
      retVal = new SyncDirId(dateTime, syncDirName, matchv2.group(7).toUpperCase(), SYNC_VERSION_2);

    } else {
      LocalDateTime dateTime = parseV1SyncTime(syncDirName, deploymentId.year);

      //Check to see if we are in a weird "wrap around" case where the deployment update was in December (so name
      // has the previous year), but the sync happened in the new year.  In this case, fix up the year
      if (dateTime != null) {

        if ((dateTime.getMonthOfYear() == 1 || dateTime.getMonthOfYear() == 2) &&
          (deploymentId.update != 1 && deploymentId.update != 2)) {
          dateTime = dateTime.plusYears(1);
        } else if ((dateTime.getMonthOfYear() == 11 || dateTime.getMonthOfYear() == 12) &&
          (deploymentId.update == 1)) {
          dateTime = dateTime.minusYears(1);
        }
      }

      retVal = new SyncDirId(dateTime, syncDirName, "", SYNC_VERSION_1);
    }

    return retVal;
  }

  /**
   * Parses the old sync directory version, this had the format
   *
   * @param syncTime
   * @param baseYear
   * @return
   */
  static public LocalDateTime parseV1SyncTime(String syncTime, int baseYear) {
    Matcher match = DirectoryIterator.SYNC_TIME_PATTERN_V1.matcher(syncTime);
    if (!match.matches()) {
      return null;
    }

    return new LocalDateTime(baseYear,
      Integer.parseInt(match.group(1)),
      Integer.parseInt(match.group(2)),
      Integer.parseInt(match.group(3)),
      Integer.parseInt(match.group(4)),
      Integer.parseInt(match.group(5)));

  }

  /**
   * Adds a millisecond to the localdatetime.  This is to uniquify in some collections.
   *
   * @return
   */
  public SyncDirId addMilli() {
    return new SyncDirId(dateTime.plusMillis(1), dirName, uniquifier, version);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SyncDirId)) return false;

    SyncDirId syncDirId = (SyncDirId) o;

    if (dirName == null) {
        return syncDirId.dirName == null;
    }
    return dirName.equalsIgnoreCase(syncDirId.dirName);
  }

  @Override
  public int hashCode() {
    return dirName != null ? dirName.toLowerCase().hashCode() : 0;
  }

  /*
  @Override
  public int compareTo(SyncDirId o) {
    if (o == this) { return 0; }
    if (this.dateTime==null && o.dateTime!=null) {
      return -1;
    }

    if (o.dateTime == null && this.dateTime != null) {
      return 1;
    }

    int retVal = this.dateTime.compareTo(o.dateTime);
    if (retVal == 0) {
      retVal = this.uniquifier.compareTo(o.uniquifier);
    }
    return retVal;
  }
  */
  @Override
  public String toString() {
    return "SyncDirId{" +
      "dirName='" + dirName + '\'' +
      '}';
  }
}
