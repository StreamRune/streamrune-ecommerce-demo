package org.streamrune.ecommerce.commands.customer;

import java.util.List;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.customer.*;

public class CustomerDecider implements Decider<CustomerCommand, CustomerState, CustomerEvent> {

  @Override
  public CustomerState initialState() {
    return new CustomerState();
  }

  @Override
  public List<CustomerEvent> decide(CustomerCommand cmd, CustomerState state) {
    return switch (cmd) {
      case CustomerCommand.RegisterCustomer c -> {
        // A customer id is registered once. Without this check a second RegisterCustomer would
        // replace the first customer's profile. A forgotten customer's id is not refused here but
        // one step later, by the key store: it never issues a key for an erased subject again, so
        // the event's encrypted fields cannot be written and the caller gets 410 Gone.
        if (state.customerId() != null && state.status() != CustomerStatus.FORGOTTEN)
          throw new DomainException("Customer already registered: " + c.customerId());
        yield List.of(
            new CustomerEvent.CustomerRegistered(
                c.customerId(), c.name(), c.email(), c.address(), c.phone()));
      }

      case CustomerCommand.UpdateProfile c -> {
        if (state.status() == CustomerStatus.FORGOTTEN)
          throw new DomainException("Cannot update a forgotten customer");
        yield List.of(
            new CustomerEvent.ProfileUpdated(
                c.customerId(),
                c.name() != null ? c.name() : state.name(),
                c.email() != null ? c.email() : state.email(),
                c.address() != null ? c.address() : state.address(),
                c.phone() != null ? c.phone() : state.phone()));
      }

      case CustomerCommand.RequestDataExport c ->
          List.of(new CustomerEvent.DataExportRequested(c.customerId()));

      case CustomerCommand.ForgetCustomer c -> {
        // An id no customer registered is refused: the forget endpoint shreds the key after this
        // command, and a shred records a permanent tombstone, so a customer could never register
        // under that id afterwards.
        if (state.customerId() == null) throw new DomainException("Customer not found");
        // Already forgotten: record nothing and succeed. The endpoint shreds the key after this
        // command, so a request that failed after the event was recorded is finished by sending
        // it again; refusing the repeat would leave the key in place for good.
        if (state.status() == CustomerStatus.FORGOTTEN) yield List.of();
        yield List.of(new CustomerEvent.CustomerForgotten(c.customerId()));
      }
    };
  }

  @Override
  public CustomerState evolve(CustomerState state, CustomerEvent evt) {
    return switch (evt) {
      case CustomerEvent.CustomerRegistered e ->
          new CustomerState(
              e.customerId(), e.name(), e.email(), e.address(), e.phone(), CustomerStatus.ACTIVE);
      case CustomerEvent.ProfileUpdated e ->
          new CustomerState(
              state.customerId(), e.name(), e.email(), e.address(), e.phone(), state.status());
      case CustomerEvent.DataExportRequested e -> state;
      case CustomerEvent.CustomerForgotten e ->
          new CustomerState(state.customerId(), null, null, null, null, CustomerStatus.FORGOTTEN);
    };
  }
}
