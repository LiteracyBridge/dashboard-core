package org.literacybridge.stats.model;

import java.io.Serializable;

/**
 * Created by wpugh on 3/29/15.
 */
public class TbDataLIneId implements Serializable {
  public String project = "";
  public String updateDateTime;
  public String outSn;

  public String getProject() {
    return project;
  }

  public void setProject(String project) {
    this.project = project;
  }

  public String getUpdateDateTime() {
    return updateDateTime;
  }

  public void setUpdateDateTime(String updateDateTime) {
    this.updateDateTime = updateDateTime;
  }

  public String getOutSn() {
    return outSn;
  }

  public void setOutSn(String outSn) {
    this.outSn = outSn;
  }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TbDataLIneId that = (TbDataLIneId) o;

        if (project != null ? !project.equals(that.project) : that.project != null)
            return false;
        if (updateDateTime != null ?
            !updateDateTime.equals(that.updateDateTime) :
            that.updateDateTime != null)
            return false;
        return outSn != null ? outSn.equals(that.outSn) : that.outSn == null;
    }

    @Override
    public int hashCode() {
        int result = project != null ? project.hashCode() : 0;
        result = 31 * result + (updateDateTime != null ? updateDateTime.hashCode() : 0);
        result = 31 * result + (outSn != null ? outSn.hashCode() : 0);
        return result;
    }
}
