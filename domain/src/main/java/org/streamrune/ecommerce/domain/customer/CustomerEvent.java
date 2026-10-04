package org.streamrune.ecommerce.domain.customer;

import org.streamrune.core.DomainEvent;
import org.streamrune.core.crypto.Encrypted;

public sealed interface CustomerEvent extends DomainEvent {
  record CustomerRegistered(
      String customerId,
      @Encrypted(subjectId = "customerId") String name,
      @Encrypted(subjectId = "customerId") String email,
      @Encrypted(subjectId = "customerId") String address,
      @Encrypted(subjectId = "customerId") String phone)
      implements CustomerEvent {}

  record ProfileUpdated(
      String customerId,
      @Encrypted(subjectId = "customerId") String name,
      @Encrypted(subjectId = "customerId") String email,
      @Encrypted(subjectId = "customerId") String address,
      @Encrypted(subjectId = "customerId") String phone)
      implements CustomerEvent {}

  record DataExportRequested(String customerId) implements CustomerEvent {}

  record CustomerForgotten(String customerId) implements CustomerEvent {}
}
