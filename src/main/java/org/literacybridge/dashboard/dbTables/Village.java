package org.literacybridge.dashboard.dbTables;

import javax.persistence.*;

/**
 */
@Entity(name="Village")
public class Village {
  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  private long id;

  @Column(nullable = false)
  private String name;

  @Column(nullable = false)
  private double lat;

  @Column(nullable = false)
  private double lon;

  @Column(nullable = false, columnDefinition = "TEXT")
  private String notes;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public double getLat() {
    return lat;
  }

  public void setLat(double lat) {
    this.lat = lat;
  }

  public double getLon() {
    return lon;
  }

  public void setLon(double lon) {
    this.lon = lon;
  }

  public String getNotes() {
    return notes;
  }

  public void setNotes(String notes) {
    this.notes = notes;
  }
}
