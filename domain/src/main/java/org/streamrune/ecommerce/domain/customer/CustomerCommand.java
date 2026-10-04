package org.streamrune.ecommerce.domain.customer;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import org.streamrune.core.Command;
import org.streamrune.core.crypto.Encrypted;

/**
 * The customer's commands. The personal data carries {@code @Encrypted} here as well as on the
 * events: a command that fails on infrastructure is stored in the command dead-letter queue for a
 * later replay, and that stored copy is the command record. Encrypted under the customer's key, it
 * becomes unreadable when the customer is forgotten, together with the events.
 */
public sealed interface CustomerCommand extends Command {
  record RegisterCustomer(
      @NotBlank String customerId,
      @Encrypted(subjectId = "customerId") @NotBlank String name,
      @Encrypted(subjectId = "customerId") @Email @NotBlank String email,
      @Encrypted(subjectId = "customerId") String address,
      @Encrypted(subjectId = "customerId") String phone)
      implements CustomerCommand {}

  record UpdateProfile(
      String customerId,
      @Encrypted(subjectId = "customerId") String name,
      @Encrypted(subjectId = "customerId") String email,
      @Encrypted(subjectId = "customerId") String address,
      @Encrypted(subjectId = "customerId") String phone)
      implements CustomerCommand {}

  record RequestDataExport(String customerId) implements CustomerCommand {}

  record ForgetCustomer(String customerId) implements CustomerCommand {}
}
