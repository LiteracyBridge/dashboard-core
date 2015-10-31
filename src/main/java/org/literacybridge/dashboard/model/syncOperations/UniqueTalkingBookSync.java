package org.literacybridge.dashboard.model.syncOperations;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

/**
 * @author willpugh
 */
@Embeddable
public class UniqueTalkingBookSync implements Serializable {
  @Column(nullable = false) String  contentUpdate;
  @Column(nullable = false) String  talkingBook;
  @Column(nullable = false) String  village;


  public String getContentUpdate() {
    return contentUpdate;
  }

  public void setContentUpdate(String contentUpdate) {
    this.contentUpdate = contentUpdate.toUpperCase();
  }

  public String getTalkingBook() {
    return talkingBook;
  }

  public void setTalkingBook(String talkingBook) {
    this.talkingBook = talkingBook.toUpperCase();
  }

  public String getVillage() {
    return village;
  }

  public void setVillage(String village) {
    this.village = village.toUpperCase();
  }
}
