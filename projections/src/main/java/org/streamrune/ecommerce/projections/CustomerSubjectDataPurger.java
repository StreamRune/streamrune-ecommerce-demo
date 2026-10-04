package org.streamrune.ecommerce.projections;

import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubjectId;

/**
 * GDPR Article 17 read-model purger for the customer read model.
 *
 * <p>Crypto-shredding (deleting the subject's encryption key) only makes the {@code @Encrypted}
 * event fields undecryptable; the {@code customers_view} row built by {@link CustomerProjection}
 * still holds the plaintext PII it captured before the forget. {@link ForgetSubjectService} invokes
 * this purger after the key is shredded so the row is actually removed.
 *
 * <p>The customer id is the subject id (see {@code @Encrypted(subjectId = "customerId")} on {@code
 * CustomerEvent}), so a purge is a single delete keyed by id. {@link ProjectionRepository#delete}
 * silently succeeds when the id is absent, making {@link #purge(SubjectId)} idempotent as the SPI
 * requires.
 */
public final class CustomerSubjectDataPurger implements SubjectDataPurger {

  private final ProjectionRepository repository;

  public CustomerSubjectDataPurger(ProjectionRepository repository) {
    this.repository = repository;
  }

  @Override
  public String name() {
    return "customers_view";
  }

  @Override
  public void purge(SubjectId subjectId) {
    // Projection name "customers" maps to the customers_view table; the customer id is the row id.
    repository.delete(ProjectionName.of("customers"), subjectId.value());
  }
}
