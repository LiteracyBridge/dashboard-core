package org.literacybridge.dashboard.model.contentUsage;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * @author willpugh
 */
@Entity(name="SyncAggregation")
public class SyncAggregation {

  public static final int LOG_EVENTS = 1;
  public static final int STAT_FILES = 2;
  public static final int FLASH_DATA = 3;


  @EmbeddedId ContentSyncUniqueId contentSyncUniqueId;

  @Column(nullable = false)  String  village;
  @Column(nullable = false)  String  contentPackage;

  //Data
  @Column(nullable = false)  int     countStarted;
  @Column(nullable = false)  int     countQuarter;
  @Column(nullable = false)  int     countHalf;
  @Column(nullable = false)  int     countThreeQuarters;
  @Column(nullable = false)  int     countCompleted;
  @Column(nullable = false)  int     countApplied;
  @Column(nullable = false)  int     countUseless;
  @Column(nullable = false)  int     totalTimePlayed;

  //Metadata
  @Column(nullable = false)  int     dataSource;
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
    this.village = village;
  }

  public String getContentPackage() {
    return contentPackage;
  }

  public void setContentPackage(String contentPackage) {
    this.contentPackage = contentPackage;
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

  public int getDataSource() {
    return dataSource;
  }

  public void setDataSource(int dataSource) {
    this.dataSource = dataSource;
  }
}
