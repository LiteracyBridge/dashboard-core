package org.literacybridge.dashboard.services;

import org.hibernate.SessionFactory;
import org.literacybridge.dashboard.dbTables.events.FasterEvent;
import org.literacybridge.dashboard.dbTables.events.JumpEvent;
import org.literacybridge.dashboard.dbTables.events.PlayedEvent;
import org.literacybridge.dashboard.dbTables.events.RecordEvent;
import org.literacybridge.dashboard.dbTables.events.SlowerEvent;
import org.literacybridge.dashboard.dbTables.events.SurveyEvent;
import org.literacybridge.dashboard.api.EventWriter;
import org.literacybridge.stats.formats.logFile.LogLineContext;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * Created by willpugh on 2/12/14.
 */
@Repository("databaseEventWriter")
public class DatabaseEventWriterService implements EventWriter {

  @Resource
  private SessionFactory sessionFactory;


  @Override
  @Transactional(propagation = Propagation.REQUIRED, readOnly = false)
  public void writePlayEvent(PlayedEvent playEvent,
      LogLineContext context) throws IOException {
    sessionFactory.getCurrentSession().saveOrUpdate(playEvent);
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRED, readOnly = false)
  public void writeRecordEvent(RecordEvent recordEvent,
      LogLineContext context) throws IOException {
    sessionFactory.getCurrentSession().saveOrUpdate(recordEvent);
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRED, readOnly = false)
  public void writeSurveyEvent(SurveyEvent surveyEvent,
      LogLineContext context) throws IOException {
    sessionFactory.getCurrentSession().saveOrUpdate(surveyEvent);
  }

    @Override
    public void writeJumpEvent(JumpEvent jumpEvent, LogLineContext context) throws IOException {
        // Not written to the database
    }

  @Override
  public void writeFasterEvent(FasterEvent fasterEvent, LogLineContext context) throws IOException {
    // Not written to the database
  }

  @Override
  public void writeSlowerEvent(SlowerEvent slowerEvent, LogLineContext context) throws IOException {
    // Not written to the database
  }
}
