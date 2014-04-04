package org.literacybridge.dashboard.model.syncOperations;

import org.codehaus.jackson.map.ObjectMapper;
import org.literacybridge.stats.model.validation.ValidationError;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import java.io.IOException;

/**
 */
@Entity(name = "UpdateValidationError")
public class UpdateValidationError {

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Id
  @GeneratedValue
  private Long id;

  @Column(nullable = false)
  private Long updateId;

  @Column(nullable = false, columnDefinition = "TEXT")
  private String errorMessage;

  @Column(nullable = false)
  private int errorId;

  @Column(nullable = false, columnDefinition = "TEXT")
  private String originalJson;

  public UpdateValidationError() {
  }

  public UpdateValidationError(long updateId, ValidationError validationError) throws IOException {
    this.updateId = updateId;
    this.errorMessage = validationError.errorMessage;
    this.errorId = errorId;
    this.originalJson = OBJECT_MAPPER.writeValueAsString(validationError);
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getUpdateId() {
    return updateId;
  }

  public void setUpdateId(Long updateId) {
    this.updateId = updateId;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public int getErrorId() {
    return errorId;
  }

  public void setErrorId(int errorId) {
    this.errorId = errorId;
  }

  public String getOriginalJson() {
    return originalJson;
  }

  public void setOriginalJson(String originalJson) {
    this.originalJson = originalJson;
  }
}
