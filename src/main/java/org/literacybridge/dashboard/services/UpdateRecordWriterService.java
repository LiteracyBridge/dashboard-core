package org.literacybridge.dashboard.services;

import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.literacybridge.dashboard.model.syncOperations.UsageUpdateRecord;
import org.literacybridge.dashboard.model.syncOperations.UpdateValidationError;
import org.literacybridge.stats.model.validation.ValidationError;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

/**
 */
@Repository(value = "updateRecordWriter")
public class UpdateRecordWriterService {
  @Resource
  private SessionFactory sessionFactory;

  @Autowired
  private UpdateValidationErrorWriter updateValidationErrorWriter;

  @Transactional(propagation = Propagation.REQUIRED, readOnly = false)
  public void write(UsageUpdateRecord updateRecord) throws IOException {
    sessionFactory.getCurrentSession().saveOrUpdate(updateRecord);
  }

  @Transactional(propagation = Propagation.REQUIRED, readOnly = false)
  public void writeWithErrors(UsageUpdateRecord updateRecord, List<ValidationError> errors) throws IOException {
    write(updateRecord);
    sessionFactory.getCurrentSession().flush();

    for (ValidationError error : errors) {
      updateValidationErrorWriter.write(new UpdateValidationError(updateRecord.getId(), error));
    }
  }


  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public UsageUpdateRecord findById(long i) {
    return (UsageUpdateRecord) sessionFactory.getCurrentSession().get(UsageUpdateRecord.class, i);
  }

  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public UsageUpdateRecord findByS3Id(String s3Id) {
    final String hqlResult = "from UpdateRecord ur where ur.deletedTime != '1974-1-1' AND ur.s3Id=:s3Id";
    final Query  hqlQuery = sessionFactory.getCurrentSession().createQuery(hqlResult).setString("s3Id", s3Id);
    return (UsageUpdateRecord) hqlQuery.uniqueResult();
  }

  /*
  public List<UpdateRecord> findByStatus(UpdateProcessingState state) {
    final String query = " from UpdateRecord as r where r.state = ?";
    sessionFactory.getCurrentSession();

    )
  }
  */

}
