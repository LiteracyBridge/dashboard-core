package org.literacybridge.dashboard.model.syncOperations;

/**
 */
public enum UpdateProcessingState {

  /**
   * Means the downloaded file is written to disk, but no permanent storage is used yet.
   */
  initialized,

  /**
   * Uploaded file has been validated and uploaded to S3
   */
  accepted,

  /**
   * Uploaded file has been exploded and processed to put in the DB
   */
  uploadedToDb,

  /**
   * Exploded file has been aggregated with other files for this content update in S3
   */
  aggegated,

  /**
   * Update is finished.  Nothing left to do.
   */
  done,

  /**
   * Update is finished, but only because there was a failure.
   */
  failed;

}
