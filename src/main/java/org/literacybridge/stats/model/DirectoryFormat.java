package org.literacybridge.stats.model;

/**
 */
public enum DirectoryFormat {
  Sync(1),      // The old (2010?) style stats data
  Archive(2);   // The newer (2013?, 2014?) style stats data

  public final int version;

  private DirectoryFormat(int version) {
    this.version = version;
  }

  public static DirectoryFormat fromVersion(int version) {
    switch (version) {
      case 1:
        return Sync;
      case 2:
        return Archive;
      default:
        throw new IllegalArgumentException("No directory format corresponds to " + version);
    }
  }
}
