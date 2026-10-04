package org.streamrune.ecommerce.projections;

import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.ecommerce.domain.customer.*;
import org.streamrune.ecommerce.queries.dto.CustomerView;

public class CustomerProjection extends BaseProjection {

  public CustomerProjection(ProjectionRepository repository) {
    super(repository, "customers");
  }

  @Override
  public void process(List<EventEnvelope> events) {
    for (var envelope : events) {
      if (envelope.event() instanceof CustomerEvent evt) {
        switch (evt) {
          case CustomerEvent.CustomerRegistered e ->
              save(
                  e.customerId(),
                  new CustomerView(
                      e.customerId(),
                      e.name(),
                      e.email(),
                      e.address(),
                      e.phone(),
                      CustomerStatus.ACTIVE,
                      envelope.metadata().timestamp(),
                      envelope.metadata().timestamp()));
          case CustomerEvent.ProfileUpdated e ->
              findById(e.customerId(), CustomerView.class)
                  .ifPresent(
                      existing ->
                          save(
                              e.customerId(),
                              new CustomerView(
                                  existing.customerId(),
                                  e.name(),
                                  e.email(),
                                  e.address(),
                                  e.phone(),
                                  existing.status(),
                                  existing.createdAt(),
                                  envelope.metadata().timestamp())));
          case CustomerEvent.DataExportRequested e -> {
            /* no read-model update */
          }
          case CustomerEvent.CustomerForgotten e ->
              // GDPR Article 17 erasure: drop the read-model row entirely rather than leave a
              // redacted shell. The SubjectDataPurger run by ForgetSubjectService deletes the same
              // row, so projection and purger agree on the terminal state ("row gone") regardless
              // of which observes the forget first — delete is idempotent on a missing id. The
              // inherited delete() helper, like save(), writes inside the batch's transaction;
              // the repository field would run outside it and miss a row this batch inserted.
              delete(e.customerId());
        }
      }
    }
  }

  public CustomerView get(String customerId) {
    return findById(customerId, CustomerView.class).orElse(null);
  }

  public List<CustomerView> listAll() {
    return repository.findAll(projectionName(), CustomerView.class);
  }
}
