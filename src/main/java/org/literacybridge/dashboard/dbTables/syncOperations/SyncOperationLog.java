package org.literacybridge.dashboard.dbTables.syncOperations;

import org.joda.time.DateTime;

import javax.persistence.*;

/**
 * Created by willpugh on 2/10/14.
 */
@Entity(name = "SyncOperationLog")
public class SyncOperationLog {

  public static final String NORMAL = "Normal";
  public static final String WARNING = "Warning";
  public static final String ERROR = "Error";
  public static final String CRITICAL = "Critical";

  public static final String OPERATION_VALIDATION = "Validation";
  public static final String OPERATION_SYNC = "Sync";

  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  int id;

  @Column(nullable = false) long updateId;
  @Column DateTime dateTime;
  @Column String operation;
  @Column String message;
  @Column String logType;
  @Column String urlToRetry;
  @Column String dataBlob;
  @Column DateTime closed;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public long getUpdateId() {
    return updateId;
  }

  public void setUpdateId(long updateId) {
    this.updateId = updateId;
  }

  public DateTime getDateTime() {
    return dateTime;
  }

  public void setDateTime(DateTime dateTime) {
    this.dateTime = dateTime;
  }

  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getLogType() {
    return logType;
  }

  public void setLogType(String logType) {
    this.logType = logType;
  }

  public String getUrlToRetry() {
    return urlToRetry;
  }

  public void setUrlToRetry(String urlToRetry) {
    this.urlToRetry = urlToRetry;
  }

  public String getDataBlob() {
    return dataBlob;
  }

  public void setDataBlob(String dataBlob) {
    this.dataBlob = dataBlob;
  }

  public DateTime getClosed() {
    return closed;
  }

  public void setClosed(DateTime closed) {
    this.closed = closed;
  }
}
