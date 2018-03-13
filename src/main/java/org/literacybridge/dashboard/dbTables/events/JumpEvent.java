package org.literacybridge.dashboard.dbTables.events;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * Defines an event caused by a user jumping forward or backward during playback.
 */

public class JumpEvent extends Event {

  private String contentId;
  private int secondsFrom;
  private int secondsTo;

  public String getContentId() {
    return contentId;
  }

  public void setContentId(String contentId) {
    this.contentId = contentId;
  }

    public int getSecondsFrom() {
        return secondsFrom;
    }
    public int getSecondsTo() {
        return secondsTo;
    }

  public void setSeconds(int secondsFrom, int secondsTo) {
      this.secondsFrom = secondsFrom;
      this.secondsTo = secondsTo;
  }
}
