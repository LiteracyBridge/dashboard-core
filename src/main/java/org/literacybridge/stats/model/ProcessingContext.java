package org.literacybridge.stats.model;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Pattern;

/**
 * A General procssing context that contains all the update/content/device information,
 * but no more specific information about a particular "sync" that occurred (e.g. specific time, etc)
 *
 * @author willpugh
 */
public class ProcessingContext {
  public static final Pattern SYNC_TIME_PATTERN = Pattern.compile("(\\d+)m(\\d+)d(\\d+)h(\\d+)m(\\d+)s");
  public final DeploymentId deploymentId;
  public final String village;
  public final String recipientId;
  public final String talkingBookId;
  public final String deviceSyncedFrom;


  public ProcessingContext(String talkingBookId, String village, String contentUpdate, String deviceSyncedFrom, String recipientId) {
    this.talkingBookId = talkingBookId;
    this.village = village;
    this.recipientId = recipientId;
    this.deploymentId = DeploymentId.parseContentUpdate(contentUpdate);
    this.deviceSyncedFrom = deviceSyncedFrom;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SyncProcessingContext)) return false;

    SyncProcessingContext that = (SyncProcessingContext) o;

    if (deploymentId != null ? !deploymentId.equals(that.deploymentId) : that.deploymentId != null)
      return false;
    if (deviceSyncedFrom != null ? !deviceSyncedFrom.equals(that.deviceSyncedFrom) : that.deviceSyncedFrom != null)
      return false;
    if (talkingBookId != null ? !talkingBookId.equals(that.talkingBookId) : that.talkingBookId != null)
      return false;
    if (village != null ? !village.equals(that.village) : that.village != null) return false;
    if (recipientId != null ? !recipientId.equals(that.recipientId) : that.recipientId != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = deploymentId != null ? deploymentId.hashCode() : 0;
    result = 31 * result + (village != null ? village.hashCode() : 0);
    result = 31 * result + (talkingBookId != null ? talkingBookId.hashCode() : 0);
    result = 31 * result + (deviceSyncedFrom != null ? deviceSyncedFrom.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
      org.apache.commons.lang.builder.ToStringBuilder sb = new org.apache.commons.lang.builder.ToStringBuilder(
          this);
      sb.append("contentUpdateId", deploymentId)
          .append("village", village);
      if (StringUtils.isNotEmpty(recipientId))
          sb.append(recipientId);
      sb.append("talkingBookId", talkingBookId)
          .append("deviceSyncedFrom", deviceSyncedFrom);
      return sb.toString();
  }
}
