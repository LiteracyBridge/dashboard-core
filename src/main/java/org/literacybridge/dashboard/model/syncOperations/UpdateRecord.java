package org.literacybridge.dashboard.model.syncOperations;

import javax.persistence.*;
import java.util.Date;

/**
 * This is a record of a content update occuring.  It will
 */
@Entity(name = "UpdateRecord")
@Table(name = "updaterecord", uniqueConstraints={
    @UniqueConstraint(columnNames = {"s3Id", "deletedTime"})
})
public class UpdateRecord {
  @Id
  @GeneratedValue
  private Long id;

  @Column(nullable = false)
  Date startTime;

  @Column(nullable = true, columnDefinition="timestamp default '1974-1-1'")
  Date deletedTime;

  @Column(nullable = false, unique=true)
  String externalId;

  @Column(nullable = true)
  String s3Id;

  @Column(nullable = true, columnDefinition = "TEXT")
  String message;

  @Column(nullable = false)
  UpdateProcessingState state;

  @Column(nullable = true)
  String updateName;

  @Column(nullable = true)
  String deviceName;

  @Column(nullable = false)
  String sha256;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Date getStartTime() {
    return startTime;
  }

  public void setStartTime(Date startTime) {
    this.startTime = startTime;
  }

  public Date getDeletedTime() {
    return deletedTime;
  }

  public void setDeletedTime(Date deletedTime) {
    this.deletedTime = deletedTime;
  }

  public String getExternalId() {
    return externalId;
  }

  public void setExternalId(String externalId) {
    this.externalId = externalId;
  }

  public String getS3Id() {
    return s3Id;
  }

  public void setS3Id(String s3Id) {
    this.s3Id = s3Id;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public UpdateProcessingState getState() {
    return state;
  }

  public void setState(UpdateProcessingState state) {
    this.state = state;
  }

  public String getUpdateName() {
    return updateName;
  }

  public void setUpdateName(String updateName) {
    this.updateName = updateName;
  }

  public String getDeviceName() {
    return deviceName;
  }

  public void setDeviceName(String deviceName) {
    this.deviceName = deviceName;
  }

  public String getSha256() {
    return sha256;
  }

  public void setSha256(String sha256) {
    this.sha256 = sha256;
  }
}
