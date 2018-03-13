package org.literacybridge.dashboard.services;

import org.hibernate.SessionFactory;
import org.literacybridge.dashboard.dbTables.syncOperations.UpdateValidationError;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.io.IOException;

/**
 */
@Repository(value = "updateValidationErrorWriter")
public class UpdateValidationErrorWriter {

  @Resource
  private SessionFactory sessionFactory;

  @Transactional(propagation = Propagation.REQUIRED, readOnly = false)
  public void write(UpdateValidationError updateValidationError) throws IOException {
    sessionFactory.getCurrentSession().saveOrUpdate(updateValidationError);
  }


}
