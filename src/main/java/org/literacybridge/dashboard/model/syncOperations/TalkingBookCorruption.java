package org.literacybridge.dashboard.model.syncOperations;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

/**
 * @author willpugh
 */
@Entity(name = "TalkingBookCorruption")
public class TalkingBookCorruption {

  @EmbeddedId UniqueTalkingBookSync uniqueTalkingBookSync;
  @Column int    corruptLines;

  public UniqueTalkingBookSync getUniqueTalkingBookSync() {
    return uniqueTalkingBookSync;
  }

  public void setUniqueTalkingBookSync(UniqueTalkingBookSync uniqueTalkingBookSync) {
    this.uniqueTalkingBookSync = uniqueTalkingBookSync;
  }

  public int getCorruptLines() {
    return corruptLines;
  }

  public void setCorruptLines(int corruptLines) {
    this.corruptLines = corruptLines;
  }
}
