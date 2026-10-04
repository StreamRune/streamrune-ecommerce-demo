package org.streamrune.ecommerce.commands.customer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.customer.*;
import org.streamrune.test.DeciderFixture;

class CustomerDeciderTest {

  private final DeciderFixture<CustomerCommand, CustomerState, CustomerEvent> fixture =
      DeciderFixture.of(new CustomerDecider());

  private CustomerEvent.CustomerRegistered registered() {
    return new CustomerEvent.CustomerRegistered(
        "c-1", "Alice", "alice@example.com", "123 Main St", "+1234567890");
  }

  @Test
  void registerCustomer() {
    fixture
        .given()
        .when(
            new CustomerCommand.RegisterCustomer(
                "c-1", "Alice", "alice@example.com", "123 Main St", "+1234567890"))
        .expectEvents(registered())
        .expectState(
            s -> {
              assertThat(s.status()).isEqualTo(CustomerStatus.ACTIVE);
              assertThat(s.name()).isEqualTo("Alice");
            });
  }

  @Test
  void updateProfile() {
    fixture
        .given(registered())
        .when(new CustomerCommand.UpdateProfile("c-1", "Alice Smith", null, null, null))
        .expectState(s -> assertThat(s.name()).isEqualTo("Alice Smith"));
  }

  @Test
  void updateProfile_forgotten_throws() {
    fixture
        .given(registered(), new CustomerEvent.CustomerForgotten("c-1"))
        .when(new CustomerCommand.UpdateProfile("c-1", "Alice", null, null, null))
        .expectException(DomainException.class);
  }

  @Test
  void requestDataExport() {
    fixture
        .given(registered())
        .when(new CustomerCommand.RequestDataExport("c-1"))
        .expectEvents(new CustomerEvent.DataExportRequested("c-1"));
  }

  @Test
  void forgetCustomer() {
    fixture
        .given(registered())
        .when(new CustomerCommand.ForgetCustomer("c-1"))
        .expectEvents(new CustomerEvent.CustomerForgotten("c-1"))
        .expectState(s -> assertThat(s.status()).isEqualTo(CustomerStatus.FORGOTTEN));
  }

  @Test
  void forgetCustomer_alreadyForgotten_recordsNothingAndSucceeds() {
    // A repeated forget must not be refused: the controller shreds the key after this command, and
    // a request that failed after the event was recorded is finished by sending it again.
    fixture
        .given(registered(), new CustomerEvent.CustomerForgotten("c-1"))
        .when(new CustomerCommand.ForgetCustomer("c-1"))
        .expectNoEvents()
        .expectState(s -> assertThat(s.status()).isEqualTo(CustomerStatus.FORGOTTEN));
  }

  @Test
  void forgetCustomer_neverRegistered_throws() {
    // Forgetting an id no customer registered would tombstone it in the key store, so the customer
    // could never register under it; the decider refuses before the controller reaches the shred.
    fixture
        .given()
        .when(new CustomerCommand.ForgetCustomer("c-unknown"))
        .expectException(DomainException.class);
  }
}
