package org.literacybridge.dashboard.dbTables.contentUsage;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.Transient;

/**
 * @author willpugh
 */
@Entity(name="SyncAggregation")
public class SyncAggregation {
    public enum Source {
        MERGED(0),
        LOG_EVENTS(1),
        STAT_FILES(2),
        FLASH_DATA(3);

        public final int value;
        Source(int value) {this.value = value;}
    }

    @EmbeddedId
    ContentSyncUniqueId contentSyncUniqueId;

  @Column(nullable = false)  String  village;
  @Column(nullable = false)  String  contentPackage;

  //Data
  @Column(nullable = false)  int     countStarted;
  @Column(nullable = false)  int     countQuarter;
  @Column(nullable = false)  int     countHalf;
  @Column(nullable = false)  int     countThreeQuarters;
  @Column(nullable = false)  int     countCompleted;
  // "Transient" in the sense that the column doesn't exist in the database (TODO: add it)
  @Transient                 int     countSurveyTaken;
  @Column(nullable = false)  int     countApplied;
  @Column(nullable = false)  int     countUseless;
  @Column(nullable = false)  int     totalTimePlayed;

  //Metadata
  @Column(nullable = true )  double  disparity;


  public ContentSyncUniqueId getContentSyncUniqueId() {
    return contentSyncUniqueId;
  }

  public void setContentSyncUniqueId(ContentSyncUniqueId contentSyncUniqueId) {
    this.contentSyncUniqueId = contentSyncUniqueId;
  }

  public String getVillage() {
    return village;
  }

  public void setVillage(String village) {
    this.village = village.toUpperCase();
  }

  public String getContentPackage() {
    return contentPackage;
  }

  public void setContentPackage(String contentPackage) {
    this.contentPackage = contentPackage.toUpperCase();
  }

  public int getCountStarted() {
    return countStarted;
  }

  public void setCountStarted(int countStarted) {
    this.countStarted = countStarted;
  }

  public int getCountQuarter() {
    return countQuarter;
  }

  public void setCountQuarter(int countQuarter) {
    this.countQuarter = countQuarter;
  }

  public int getCountHalf() {
    return countHalf;
  }

  public void setCountHalf(int countHalf) {
    this.countHalf = countHalf;
  }

  public int getCountThreeQuarters() {
    return countThreeQuarters;
  }

  public void setCountThreeQuarters(int countThreeQuarters) {
    this.countThreeQuarters = countThreeQuarters;
  }

  public int getCountCompleted() {
    return countCompleted;
  }

  public void setCountCompleted(int countCompleted) {
    this.countCompleted = countCompleted;
  }

  public int getCountSurveyTaken() {
    return countSurveyTaken;
  }

  public void setCountSurveyTaken(int countSurveyTaken) {
    this.countSurveyTaken = countSurveyTaken;
  }

  public int getCountApplied() {
    return countApplied;
  }

  public void setCountApplied(int countApplied) {
    this.countApplied = countApplied;
  }

  public int getCountUseless() {
    return countUseless;
  }

  public void setCountUseless(int countUseless) {
    this.countUseless = countUseless;
  }

  public int getTotalTimePlayed() {
    return totalTimePlayed;
  }

  public void setTotalTimePlayed(int totalTimePlayed) {
    this.totalTimePlayed = totalTimePlayed;
  }

}
