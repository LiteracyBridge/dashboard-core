package org.literacybridge.stats.formats.logFile;

/**
 * @author willpugh
 */
public class LogFilePosition {

  final public String fileName;
  final public int lineNumber;

  public LogFilePosition(String fileName, int lineNumber) {
    this.fileName = fileName;
    this.lineNumber = lineNumber;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof LogFilePosition)) return false;

    LogFilePosition that = (LogFilePosition) o;

    if (lineNumber != that.lineNumber) return false;
    if (fileName != null ? !fileName.equals(that.fileName) : that.fileName != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = fileName != null ? fileName.hashCode() : 0;
    result = 31 * result + lineNumber;
    return result;
  }

  public String loggingFileName() {
      String [] parts = fileName.split("/");
      StringBuilder sb = new StringBuilder();
      for (int ix=parts.length-9; ix<parts.length; ix++) {
          if (sb.length()>0) { sb.append('/'); }
          sb.append(parts[ix]);
      }
      return sb.toString();
  }
}
