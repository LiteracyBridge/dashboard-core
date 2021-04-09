package org.literacybridge.stats.model;

import org.joda.time.LocalDateTime;

/**
 * THis is a general dbTables class that is useful for processing any format of file that has
 * been synced by a talking book.  It contains the context about the content the talking book
 * had on it as well as the time it was synced.
 *
 * @author willpugh
 */
public class SyncProcessingContext extends ProcessingContext {
  public final LocalDateTime syncTime; // stats time
  public final LocalDateTime deploymentTime;
  public final String deploymentUuid;
  public final String contentPackage;
  public final String project;

  public SyncProcessingContext(String syncString,
      String talkingBookId,
      String village,
      String contentPackage,
      String contentUpdate,
      String project,
      String deviceSyncedFrom,
      String recipientId,
      LocalDateTime deploymentTime,
      String deploymentUuid) {
    super(talkingBookId, village, contentUpdate, deviceSyncedFrom, recipientId);

    SyncDirId syncDirId = SyncDirId.parseSyncDir(deploymentId, syncString);
    this.syncTime = syncDirId.dateTime;
    this.contentPackage = contentPackage;
    this.project = project;
    this.deploymentTime = deploymentTime;
    this.deploymentUuid = deploymentUuid;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SyncProcessingContext)) return false;
    if (!super.equals(o)) return false;

    SyncProcessingContext that = (SyncProcessingContext) o;

    if (contentPackage != null ? !contentPackage.equals(that.contentPackage) : that.contentPackage != null)
      return false;
    if (syncTime != null ? !syncTime.equals(that.syncTime) : that.syncTime != null) return false;
    if (deploymentUuid != null ? !deploymentUuid.equals(that.deploymentUuid) : that.deploymentUuid != null) return false;
    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (syncTime != null ? syncTime.hashCode() : 0);
    result = 31 * result + (contentPackage != null ? contentPackage.hashCode() : 0);
    return result;
  }
}
