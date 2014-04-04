package org.literacybridge.dashboard.services;

import org.hibernate.SessionFactory;
import org.literacybridge.stats.model.events.PlayedEvent;
import org.literacybridge.stats.model.events.RecordEvent;
import org.literacybridge.stats.model.events.SurveyEvent;
import org.literacybridge.dashboard.api.EventWriter;
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
  public void writePlayEvent(PlayedEvent playEvent) throws IOException {
    sessionFactory.getCurrentSession().saveOrUpdate(playEvent);
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRED, readOnly = false)
  public void writeRecordEvent(RecordEvent recordEvent) throws IOException {
    sessionFactory.getCurrentSession().saveOrUpdate(recordEvent);
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRED, readOnly = false)
  public void writeSurveyEvent(SurveyEvent surveyEvent) throws IOException {
    sessionFactory.getCurrentSession().saveOrUpdate(surveyEvent);
  }
}
