package org.literacybridge.dashboard.model.contentUsage;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.literacybridge.stats.model.ProcessingContext;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

/**
 * @author willpugh
 */
@Embeddable
public class ContentSyncUniqueId implements Serializable {

  public static ContentSyncUniqueId createFromContext(String contentId, ProcessingContext context) {
    Preconditions.checkArgument(contentId != null,               "ContentId MUST not be null for a content aggregation");
    Preconditions.checkArgument(context.deploymentId != null, "ContentUpdateId MUST not be null for a content aggregation");
    Preconditions.checkArgument(context.talkingBookId != null,   "talkingBookId MUST not be null for a content aggregation");

    ContentSyncUniqueId uniqueId = new ContentSyncUniqueId();
    uniqueId.setContentId(contentId);
    uniqueId.setContentUpdate(context.deploymentId.id);
    uniqueId.setTalkingBook(context.talkingBookId);
    return uniqueId;
  }

  @Column(nullable = false) String  contentUpdate;
  @Column(nullable = false) String  talkingBook;
  @Column(nullable = false) String  contentId;

  public ContentSyncUniqueId() {
  }

  public ContentSyncUniqueId(String contentUpdate, String talkingBook, String contentId) {
    this.contentUpdate = contentUpdate;
    this.talkingBook = talkingBook;
    this.contentId = contentId;
  }

  public String getContentUpdate() {
    return contentUpdate;
  }

  public void setContentUpdate(String contentUpdate) {
    this.contentUpdate = contentUpdate;
  }

  public String getTalkingBook() {
    return talkingBook;
  }

  public void setTalkingBook(String talkingBook) {
    this.talkingBook = talkingBook;
  }

  public String getContentId() {
    return contentId;
  }

  public void setContentId(String contentId) {
    this.contentId = contentId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ContentSyncUniqueId)) return false;

    ContentSyncUniqueId that = (ContentSyncUniqueId) o;

    if (contentId != null ? !contentId.equals(that.contentId) : that.contentId != null) return false;
    if (contentUpdate != null ? !contentUpdate.equals(that.contentUpdate) : that.contentUpdate != null) return false;
    if (talkingBook != null ? !talkingBook.equals(that.talkingBook) : that.talkingBook != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = contentUpdate != null ? contentUpdate.hashCode() : 0;
    result = 31 * result + (talkingBook != null ? talkingBook.hashCode() : 0);
    result = 31 * result + (contentId != null ? contentId.hashCode() : 0);
    return result;
  }

  @Override public String toString() {
    return new ToStringBuilder(this)
        .append("contentUpdate", contentUpdate)
        .append("talkingBook", talkingBook)
        .append("contentId", contentId)
        .toString();
  }
}
