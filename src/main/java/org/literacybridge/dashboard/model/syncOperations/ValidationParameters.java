package org.literacybridge.dashboard.model.syncOperations;

import org.literacybridge.stats.model.DirectoryFormat;

/**
 */
public class ValidationParameters {

  /**
   * Directory format to force ths processor to use. If the directory uploaded has a proper manifest,
   * this should be null.
   */
  DirectoryFormat format;

  /**
   * Enforce strict validation.  This will mean the validation processor will stick to the letter of the format,
   * and fail for any strays
   */
  boolean strict;

  /**
   * If this is true, than processing will proceed, despite validation errors.
   */
  boolean force;


  public DirectoryFormat getFormat() {
    return format;
  }

  public void setFormat(DirectoryFormat format) {
    this.format = format;
  }

  public boolean isStrict() {
    return strict;
  }

  public void setStrict(boolean strict) {
    this.strict = strict;
  }

  public boolean isForce() {
    return force;
  }

  public void setForce(boolean force) {
    this.force = force;
  }
}
