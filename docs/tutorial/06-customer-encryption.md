# Chapter 6 — Customer Aggregate & Encryption

> **What you'll learn:**
> - Why PII is uniquely dangerous in event-sourced systems — and how field-level encryption addresses it
> - How to annotate event and command fields with `@Encrypted` to protect personal data at rest
> - How to implement and test the `CustomerDecider` with `DeciderFixture`
> - How `CryptoShreddingModule` wires into Jackson so encryption is transparent to your domain code
> - How `PostgresCryptoEngine` manages per-subject AES-256-GCM keys in the database
> - How to verify that stored events are encrypted while the API returns plaintext

---

## What We're Building and Why

Events are immutable. That is one of the core guarantees of event sourcing, and it is enormously useful — it gives you a perfect audit trail, enables time-travel debugging, and makes projections easy to rebuild. But immutability creates a problem the moment personal data enters the picture.

In a traditional relational system, GDPR compliance is relatively mechanical: you find the row that holds the customer's name and email address, and you delete or blank it. In an event-sourced system there is no single row to update. The customer's name might appear in `CustomerRegistered`, `ProfileUpdated`, and any number of downstream projections. You cannot delete an event — the entire point of the log is that it never changes.

The standard solution is **crypto-shredding**: instead of deleting the data, you delete the key. If every PII field is encrypted with a key that is unique to the data subject (the customer), then deleting that key renders all their personal data permanently unreadable across every event and every projection at once. The events remain intact and the stream is still replayable, but decrypted values come back as `[REDACTED]`.

StreamRune implements this pattern through three collaborating pieces:

- **`@Encrypted`** — a record-component annotation that marks a field as PII and names the component that holds the subject identifier.
- **`CryptoShreddingModule`** — a Jackson module that intercepts serialisation and deserialisation, transparently encrypting marked fields on the way in and decrypting them on the way out.
- **`CryptoEngine`** — the strategy interface for key management and the actual AES-GCM cipher. `PostgresCryptoEngine` is the provided implementation that stores per-subject keys in an `encryption_keys` table.

Your domain code — `CustomerDecider`, `CustomerEvent`, `CustomerState` — works entirely with plaintext strings. Encryption is an infrastructure concern wired in at the serialisation layer. The domain stays pure.

---

## Domain Model

The Customer aggregate needs four commands, four events, a status enum, and a state record. Create these four files in the `domain` module: `CustomerStatus`, `CustomerCommand`, `CustomerEvent`, and `CustomerState`.

### CustomerStatus

Create `domain/src/main/java/org/streamrune/ecommerce/domain/customer/CustomerStatus.java`:

```java
package org.streamrune.ecommerce.domain.customer;

public enum CustomerStatus {
  ACTIVE,
  FORGOTTEN
}
```

`FORGOTTEN` is the state a customer enters after a `ForgetCustomer` command succeeds. It records that the customer asked to be erased: the customer cannot be updated, and the forget endpoint goes on to delete the encryption key so the data reads as redacted (chapter 7).

### CustomerCommand

Create `domain/src/main/java/org/streamrune/ecommerce/domain/customer/CustomerCommand.java`:

```java
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
```

`address` and `phone` are optional — the validation annotations on the others enforce the minimum required data for registration. Role-based access control comes in chapter 9.

The personal data carries `@Encrypted` on the commands as well as on the events (the next section explains what the annotation does). A command is normally never stored: the bus hands it to the decider and keeps only the events. The exception is a command that fails on infrastructure — a database or network error, not a business rejection. The command bus records it in its dead-letter queue (chapter 14) so it can be replayed later, and that record is the command itself. Without the annotation, a failed `RegisterCustomer` would leave the customer's name, email, address and phone in plaintext in the `dead_letter_queue` table, where deleting the customer's key (chapter 7) cannot reach them. With it, the stored copy is encrypted under the same per-customer key as the events and is erased together with them.

### CustomerEvent

The events carry the same personal data, encrypted the same way; this section explains what the annotation does. Create `domain/src/main/java/org/streamrune/ecommerce/domain/customer/CustomerEvent.java`:

```java
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
```

The `@Encrypted(subjectId = "customerId")` annotation does two things at runtime. During serialisation, `CryptoShreddingModule` reads the value of the `customerId` component, uses it to look up (or lazily create) an AES-256 key in `encryption_keys`, encrypts the annotated field's value with that key, and writes Base64-encoded ciphertext to the JSON payload. During deserialisation, it performs the reverse: Base64-decode, then decrypt. If the key has been deleted, it writes `[REDACTED]` instead of throwing.

Note that `customerId` itself is not annotated — it is the subject identifier, not the personal data. It must remain in plaintext so the engine knows which key to use.

`DataExportRequested` and `CustomerForgotten` carry no PII — they are intentional audit markers.

### CustomerState

Create `domain/src/main/java/org/streamrune/ecommerce/domain/customer/CustomerState.java`:

```java
package org.streamrune.ecommerce.domain.customer;

import org.streamrune.core.AggregateState;
import org.streamrune.core.types.AggregateType;

public record CustomerState(
    String customerId,
    String name,
    String email,
    String address,
    String phone,
    CustomerStatus status)
    implements AggregateState {
  /**
   * The aggregate type the customer decider is registered under: streams are customer:<customerId>.
   */
  public static final AggregateType TYPE = AggregateType.of("customer");

  public CustomerState() {
    this(null, null, null, null, null, CustomerStatus.ACTIVE);
  }
}
```

State is always held in plaintext in memory — it is only the serialised form (the event in the database) that is encrypted. When the `CommandBus` rebuilds state by replaying events, Jackson decrypts each field before handing the event to `evolve`.

---

## Step by Step

### Step 1 — Create the test

Create `commands/src/test/java/org/streamrune/ecommerce/commands/customer/CustomerDeciderTest.java`:

```java
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
  void registerCustomer_existingCustomer_throws() {
    fixture
        .given(registered())
        .when(
            new CustomerCommand.RegisterCustomer(
                "c-1", "Mallory", "mallory@example.com", "1 Other St", "+1999999999"))
        .expectFailedWith(DomainException.class, "Customer already registered: c-1");
  }

  /**
   * The decider does not refuse a forgotten customer's id. The key store does, one step later: it
   * never issues a key for an erased subject again, so the event's encrypted fields cannot be
   * written and the caller gets 410 Gone (GdprForgetE2EIT in each app).
   */
  @Test
  void registerCustomer_forgottenCustomer_isLeftToTheKeyStore() {
    fixture
        .given(registered(), new CustomerEvent.CustomerForgotten("c-1"))
        .when(
            new CustomerCommand.RegisterCustomer(
                "c-1", "Alice", "alice@example.com", "123 Main St", "+1234567890"))
        .expectEvents(registered());
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

  @Test
  void updateProfile_neverRegistered_throws() {
    // Personal data stored under an id no customer registered could never be erased: the decider
    // refuses a forget for such an id, so it refuses the write that would need one too.
    fixture
        .given()
        .when(
            new CustomerCommand.UpdateProfile(
                "c-unknown", "Mallory", "mallory@example.com", "1 Other St", null))
        .expectFailedWith(DomainException.class, "Customer not found");
  }

  @Test
  void requestDataExport_neverRegistered_throws() {
    fixture
        .given()
        .when(new CustomerCommand.RequestDataExport("c-unknown"))
        .expectFailedWith(DomainException.class, "Customer not found");
  }

  @Test
  void requestDataExport_forgotten_throws() {
    // Nothing is left to export, and the stream of an erased customer takes no further event.
    fixture
        .given(registered(), new CustomerEvent.CustomerForgotten("c-1"))
        .when(new CustomerCommand.RequestDataExport("c-1"))
        .expectFailedWith(DomainException.class, "Cannot export the data of a forgotten customer");
  }
}
```

Run it — the tests will fail with a compilation error because `CustomerDecider` does not exist yet:

```bash
./gradlew :commands:test --tests "*.CustomerDeciderTest"
```

That is the red phase.

### Step 2 — Implement CustomerDecider

Create `commands/src/main/java/org/streamrune/ecommerce/commands/customer/CustomerDecider.java`:

```java
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
        // An id no customer registered is refused, as ForgetCustomer refuses it below: personal
        // data stored under such an id could never be erased.
        if (state.customerId() == null) throw new DomainException("Customer not found");
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

      case CustomerCommand.RequestDataExport c -> {
        if (state.customerId() == null) throw new DomainException("Customer not found");
        // Nothing is left to export, and the stream of an erased customer takes no further event.
        if (state.status() == CustomerStatus.FORGOTTEN)
          throw new DomainException("Cannot export the data of a forgotten customer");
        yield List.of(new CustomerEvent.DataExportRequested(c.customerId()));
      }

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
```

A few things worth noting:

- `RegisterCustomer` refuses an id that already has a customer, for the reason `CreateProduct` does in chapter 2: the state the command bus loaded is the only place where "this customer exists" is known, and without the check a second registration would append another `CustomerRegistered` and `evolve` would replace the first customer's name, email, address and phone. The refusal is a `DomainException`, so the caller gets `400 Customer already registered: c-1`. The check leaves out a customer who is `FORGOTTEN`. That id is refused too, but by the key store rather than by the decider: once chapter 7 has deleted the subject's key, no new key is ever issued for the id, the event's `@Encrypted` fields cannot be written, and the `SubjectForgottenException` reaches the caller as `410 Gone` (the handler from chapter 1).
- `UpdateProfile` merges the incoming fields with the current state. A `null` field in the command means "leave this field unchanged." This is a common pattern for partial-update commands in event-sourced systems — the decider resolves the merge before emitting the event, so the event always carries the full resulting value rather than a sparse patch.
- `UpdateProfile` guards against an invalid transition by checking the current status. A forgotten customer is permanently redacted; attempting to update them is a domain error.
- `ForgetCustomer` refuses an id no customer registered: chapter 7 deletes the subject's key right after this command, and the key store then refuses a key for that id for good, so forgetting an unknown id would block it before its customer ever registers. For a customer who is already `FORGOTTEN` it returns no events instead of throwing. Nothing is recorded twice, and chapter 7 relies on that: a forget request that failed after the event was recorded is finished by sending it again, and the repeat must reach the key deletion.
- `evolve` for `CustomerForgotten` explicitly nulls out all PII fields in the in-memory state. The persisted event will return `[REDACTED]` values once the key is deleted (chapter 7), so the state should reflect the same reality.

Run the tests:

```bash
./gradlew :commands:test --tests "*.CustomerDeciderTest"
```

All twelve tests should pass. Green phase.

### Step 3 — Confirm the crypto dependencies in `domain/build.gradle.kts`

`CustomerEvent` and `CustomerCommand` import `@Encrypted` from `streamrune-core` (package `org.streamrune.core.crypto`). The dependencies were already added in Chapter 1 (Step 3). The file should look like this:

```kotlin
dependencies {
    val sr = libs.versions.streamrune.get()
    implementation("org.streamrune:streamrune-core:$sr")
    implementation("org.streamrune:streamrune-crypto-api:$sr")
    implementation(libs.jackson.databind)
    implementation(libs.jakarta.validation.api)
}
```

The `@Encrypted` annotation and the `CryptoEngine` interface both live in `streamrune-core` (`org.streamrune.core.crypto`), so that is what the domain module actually needs to compile the annotation. The separate `streamrune-crypto-api` module provides the `CryptoShreddingModule` (the Jackson module the event store uses internally to encrypt and decrypt marked fields) and `CachedCryptoEngine`. Neither has a runtime dependency on PostgreSQL — keeping the domain annotation separate from the infrastructure implementation.

### Step 3b — Create `CustomerView`

The customer query controller (wired later in this chapter) returns `CustomerView`. Create it now.

Create `queries/src/main/java/org/streamrune/ecommerce/queries/dto/CustomerView.java`:

```java
package org.streamrune.ecommerce.queries.dto;

import java.time.Instant;
import org.streamrune.ecommerce.domain.customer.CustomerStatus;

public record CustomerView(
    String customerId,
    String name,
    String email,
    String address,
    String phone,
    CustomerStatus status,
    Instant createdAt,
    Instant updatedAt) {}
```

### Step 4 — Declare the crypto engine bean

This is the step that actually turns encryption on. Open `spring-app/src/main/java/org/streamrune/ecommerce/spring/config/StreamRuneConfig.java` and add a `cryptoEngine` bean (it needs the import of `org.streamrune.crypto.postgres.PostgresCryptoEngine`; `javax.sql.DataSource` is imported since chapter 5):

```java
@Bean
public PostgresCryptoEngine cryptoEngine(DataSource ds) {
  return PostgresCryptoEngine.builder().dataSource(ds).build();
}
```

`PostgresCryptoEngine` stores per-subject AES-256 keys in the `encryption_keys` table. You do not create it: now that a crypto engine bean exists, the event store factory applies the crypto migration series bundled in `streamrune-postgres-crypto` (`db/crypto-migration/`, recorded in `flyway_schema_history_crypto`) on the next start, next to the event-store series from chapter 1, and `encryption_keys`, `forgotten_subjects` and `erased_key_generations` appear. StreamRune's Spring auto-configuration discovers this bean via `ObjectProvider<CryptoEngine>` alongside the `EventTypeRegistry` and `EventUpcaster` beans from chapters 2 and 4, and builds a single `PostgresEventStore` with encryption active. Every event with `@Encrypted` components is then encrypted before it is written to `event_stream.payload` and decrypted when it is read back — transparently to your domain code.

A crypto engine also brings in the framework's GDPR erasure service, `ForgetSubjectService`, which chapter 7 puts to work: its `forget` deletes a subject's key. The service refuses to start without a purger for the aggregate snapshots (a snapshot keeps the aggregate's state in the clear, chapter 8), unless the application states that it has never stored one. This application has not — its command bus takes no snapshots before chapter 8 — so declare the service with `withoutSnapshotPurge()` for now. Without this bean the auto-configured service stops the application at startup with `The ForgetSubjectService StreamRune configures has no snapshot purger`. Chapter 7 replaces it with the version that runs the purgers, a snapshot purger among them, before chapter 8 turns snapshots on:

```java
@Bean
public ForgetSubjectService forgetSubjectService(PostgresCryptoEngine cryptoEngine) {
  return ForgetSubjectService.builder().cryptoEngine(cryptoEngine).withoutSnapshotPurge().build();
}
```

It needs the import of `org.streamrune.runtime.gdpr.ForgetSubjectService`.

### Step 5 — Register Customer events in the EventTypeRegistry

Still in `StreamRuneConfig.java`, add the four Customer event types to the `SimpleEventTypeRegistry` inside the `eventTypeRegistry` bean (alongside the four product events from chapter 2):

```java
.registerEvent(
    "CustomerRegistered",
    org.streamrune.ecommerce.domain.customer.CustomerEvent.CustomerRegistered.class)
.registerEvent(
    "ProfileUpdated",
    org.streamrune.ecommerce.domain.customer.CustomerEvent.ProfileUpdated.class)
.registerEvent(
    "DataExportRequested",
    org.streamrune.ecommerce.domain.customer.CustomerEvent.DataExportRequested.class)
.registerEvent(
    "CustomerForgotten",
    org.streamrune.ecommerce.domain.customer.CustomerEvent.CustomerForgotten.class)
```

The string names (`"CustomerRegistered"`, etc.) are the wire-format type names stored in the `event_type` column. Choose them deliberately — they are part of your persisted data contract.

### Step 6 — Register the CustomerDecider in the CommandBus

In the `commandBus` bean in `StreamRuneConfig.java`, register `CustomerDecider` with its aggregate type and id extractor:

```java
.register(
    CustomerState.TYPE,
    CustomerCommand.class,
    cmd ->
        AggregateId.of(
            switch (cmd) {
              case CustomerCommand.RegisterCustomer c -> c.customerId();
              case CustomerCommand.UpdateProfile c -> c.customerId();
              case CustomerCommand.RequestDataExport c -> c.customerId();
              case CustomerCommand.ForgetCustomer c -> c.customerId();
            }),
    new CustomerDecider())
```

Every `CustomerCommand` carries a `customerId`. The extractor maps any command subtype to that ID; together with `CustomerState.TYPE` it names the stream, `customer:c-1`, so the `CommandBus` knows which event stream to load before calling `decide`.

### Step 7 — Add the REST controllers

Create `spring-app/src/main/java/org/streamrune/ecommerce/spring/controller/CustomerCommandController.java`:

```java
package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.runtime.VirtualThreadCommandBus;

@RestController
@RequestMapping("/api/customers")
public class CustomerCommandController {

  private final VirtualThreadCommandBus commandBus;

  public CustomerCommandController(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  @PostMapping
  public ResponseEntity<Void> registerCustomer(@Valid @RequestBody RegisterRequest req) {
    commandBus.execute(
        new CustomerCommand.RegisterCustomer(
            req.customerId(), req.name(), req.email(), req.address(), req.phone()));
    return ResponseEntity.ok().build();
  }

  @PutMapping("/{id}")
  public ResponseEntity<Void> updateProfile(
      @PathVariable String id, @RequestBody UpdateProfileRequest req) {
    commandBus.execute(
        new CustomerCommand.UpdateProfile(id, req.name(), req.email(), req.address(), req.phone()));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/{id}/export-data")
  public ResponseEntity<Void> exportData(@PathVariable String id) {
    commandBus.execute(new CustomerCommand.RequestDataExport(id));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/{id}/forget")
  public ResponseEntity<Void> forgetCustomer(@PathVariable String id) {
    commandBus.execute(new CustomerCommand.ForgetCustomer(id));
    return ResponseEntity.ok().build();
  }

  public record RegisterRequest(
      @NotBlank String customerId,
      @NotBlank String name,
      @Email @NotBlank String email,
      String address,
      String phone) {}

  public record UpdateProfileRequest(String name, String email, String address, String phone) {}
}
```

> **Note:** For this chapter the `/forget` endpoint only records the `CustomerForgotten` event via the command bus. In chapter 7 we complete it: the controller will also inject `ForgetSubjectService` and call `forget(...)` after emitting the command, which deletes the subject's encryption key (crypto-shred) and runs the read-model purgers. If you diff against the real demo now, that is the difference you will see.

Create `spring-app/src/main/java/org/streamrune/ecommerce/spring/controller/CustomerQueryController.java`:

```java
package org.streamrune.ecommerce.spring.controller;

import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.projections.CustomerProjection;
import org.streamrune.ecommerce.queries.dto.CustomerView;

@RestController
@RequestMapping("/api/customers")
public class CustomerQueryController {

  private final CustomerProjection customerProjection;

  public CustomerQueryController(CustomerProjection customerProjection) {
    this.customerProjection = customerProjection;
  }

  @GetMapping("/{id}")
  public ResponseEntity<CustomerView> getCustomer(@PathVariable String id) {
    CustomerView customer = customerProjection.get(id);
    return customer != null ? ResponseEntity.ok(customer) : ResponseEntity.notFound().build();
  }

  @GetMapping
  public List<CustomerView> listCustomers() {
    return customerProjection.listAll();
  }
}
```

`CustomerProjection` follows the same pattern as `ProductProjection` from chapter 5. Create `projections/src/main/java/org/streamrune/ecommerce/projections/CustomerProjection.java`:

```java
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
```

Because `CryptoShreddingModule` decrypts `@Encrypted` fields before they reach the projection handler, `CustomerView` always holds plaintext values during normal operation. When `CustomerForgotten` arrives, the projection deletes the read-model row entirely rather than leaving a redacted shell — a subsequent `GET /api/customers/{id}` then returns `404 Not Found`. Chapter 7 wires the GDPR forget flow that also deletes the encryption key and runs a `SubjectDataPurger`; that purger deletes the same row, so projection and purger agree on the terminal state ("row gone").

### Step 8 — Register `CustomerProjection` in `MultiProjectionRunner`

`CustomerProjection` becomes a bean of `StreamRuneConfig`, next to `productProjection`, and the `MultiProjectionRunner` you wired in Chapter 5 gets a registration for it. Declare the bean:

```java
@Bean
public CustomerProjection customerProjection(ProjectionRepository repo) {
  return new CustomerProjection(repo);
}
```

and give `projectionRunner` the projection as a parameter and a `"customers"` registration:

```java
@Bean(destroyMethod = "close")
public MultiProjectionRunner projectionRunner(
    EventStore eventStore,
    OffsetStore offsetStore,
    JdbcProjectionRepository projectionRepository,
    StreamRuneProperties properties,
    ProductProjection productProjection,
    CustomerProjection customerProjection,
    CacheInvalidator cacheInvalidator) {
  var runner =
      MultiProjectionRunner.builder()
          .eventStore(eventStore)
          .offsetStore(offsetStore)
          .subscriptionConfig(projectionPolling(properties))
          // Every TRANSACTIONAL_LOCAL registration below commits its read-model write and its
          // checkpoint in ONE transaction.
          .atomicProcessor(projectionRepository)
          .register(
              "products",
              new CacheAwareProjection(productProjection, cacheInvalidator),
              ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
          .register(
              "customers",
              new CacheAwareProjection(customerProjection, cacheInvalidator),
              ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
          .build();
  runner.start();
  return runner;
}
```

Without the registration `CustomerProjection` would be a dangling bean — Spring would create it but no projection runner would ever feed it events. With this change, `MultiProjectionRunner` assigns customers its own virtual thread and its own offset row in `PostgresOffsetStore`, independent of the products projection. `CustomerProjection` writes through the same `projectionRepository` bean as products, so it is `TRANSACTIONAL_LOCAL` too: the forget handler's `delete(customerId)` and the rest of the batch commit with the checkpoint, or not at all.

The whole class at the end of this chapter:

<details>
<summary><code>StreamRuneConfig.java</code> at the end of Chapter 6</summary>

```java
package org.streamrune.ecommerce.spring.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.time.Duration;
import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.*;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import org.streamrune.ecommerce.commands.customer.CustomerDecider;
import org.streamrune.ecommerce.commands.product.ProductDecider;
import org.streamrune.ecommerce.commands.upcaster.ProductCreatedUpcaster;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.projections.*;
import org.streamrune.ecommerce.queries.query.ListProducts;
import org.streamrune.postgres.*;
import org.streamrune.runtime.*;
import org.streamrune.runtime.gdpr.ForgetSubjectService;
import org.streamrune.spring.StreamRuneProperties;

@Configuration(proxyBeanMethods = false)
public class StreamRuneConfig {

  // Spring Boot 4.0 auto-configures Jackson 3.x (tools.jackson.databind.ObjectMapper).
  // StreamRune framework still uses Jackson 2.x — provide the legacy ObjectMapper explicitly.
  @Bean
  public ObjectMapper legacyObjectMapper() {
    return JsonMapper.builder().build();
  }

  /**
   * Per-subject AES-256 crypto engine backing the {@code @Encrypted} fields on {@code
   * CustomerEvent}. Wired into the event store below so PII is encrypted at rest in {@code
   * event_stream.payload}; deleting a subject's key (the forget flow) crypto-shreds that subject.
   * Its {@code encryption_keys}, {@code forgotten_subjects} and {@code erased_key_generations}
   * tables come from the framework's crypto migration series, which the event store factory applies
   * at startup next to its own ({@code streamrune.event-store.schema.auto-initialize}).
   */
  @Bean
  public PostgresCryptoEngine cryptoEngine(DataSource ds) {
    return PostgresCryptoEngine.builder().dataSource(ds).build();
  }

  /**
   * Exposes the event registry for the auto-configured {@code postgresEventStoreFactory} to pick up
   * via {@code ObjectProvider<EventTypeRegistry>}.
   */
  @Bean
  public EventTypeRegistry eventTypeRegistry() {
    return SimpleEventTypeRegistry.builder()
        // Product events
        .registerEvent(
            "ProductCreated",
            org.streamrune.ecommerce.domain.product.ProductEvent.ProductCreated.class)
        .registerEvent(
            "PriceUpdated", org.streamrune.ecommerce.domain.product.ProductEvent.PriceUpdated.class)
        .registerEvent(
            "StockAdjusted",
            org.streamrune.ecommerce.domain.product.ProductEvent.StockAdjusted.class)
        .registerEvent(
            "ProductDiscontinued",
            org.streamrune.ecommerce.domain.product.ProductEvent.ProductDiscontinued.class)
        // Customer events
        .registerEvent(
            "CustomerRegistered",
            org.streamrune.ecommerce.domain.customer.CustomerEvent.CustomerRegistered.class)
        .registerEvent(
            "ProfileUpdated",
            org.streamrune.ecommerce.domain.customer.CustomerEvent.ProfileUpdated.class)
        .registerEvent(
            "DataExportRequested",
            org.streamrune.ecommerce.domain.customer.CustomerEvent.DataExportRequested.class)
        .registerEvent(
            "CustomerForgotten",
            org.streamrune.ecommerce.domain.customer.CustomerEvent.CustomerForgotten.class)
        .build();
  }

  /**
   * Exposes the product-created upcaster so the auto-configured {@code postgresEventStoreFactory}
   * can collect it via {@code ObjectProvider<EventUpcaster>}.
   */
  @Bean
  public EventUpcaster productCreatedUpcaster() {
    return new ProductCreatedUpcaster();
  }

  // Declared as the CONCRETE type: it is both the ProjectionRepository every projection writes
  // through and the AtomicBatchProcessor the runner commits with. Startup verifies the pairing:
  // every TRANSACTIONAL_LOCAL projection below must write to THIS object, and each one does.
  @Bean
  public JdbcProjectionRepository projectionRepository(DataSource ds) {
    return new JdbcProjectionRepository(ds);
  }

  // Interceptors
  @Bean
  public BeanValidationInterceptor validationInterceptor() {
    var factory = jakarta.validation.Validation.buildDefaultValidatorFactory();
    return new BeanValidationInterceptor(factory.getValidator());
  }

  @Bean
  public VirtualThreadCommandBus commandBus(
      EventStore store, BeanValidationInterceptor validation) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .interceptors(validation)
        .register(
            ProductState.TYPE,
            ProductCommand.class,
            cmd ->
                AggregateId.of(
                    switch (cmd) {
                      case ProductCommand.CreateProduct c -> c.productId();
                      case ProductCommand.UpdatePrice c -> c.productId();
                      case ProductCommand.AdjustStock c -> c.productId();
                      case ProductCommand.DiscontinueProduct c -> c.productId();
                    }),
            new ProductDecider())
        .register(
            CustomerState.TYPE,
            CustomerCommand.class,
            cmd ->
                AggregateId.of(
                    switch (cmd) {
                      case CustomerCommand.RegisterCustomer c -> c.customerId();
                      case CustomerCommand.UpdateProfile c -> c.customerId();
                      case CustomerCommand.RequestDataExport c -> c.customerId();
                      case CustomerCommand.ForgetCustomer c -> c.customerId();
                    }),
            new CustomerDecider())
        .build();
  }

  // Projections
  @Bean
  public ProductProjection productProjection(ProjectionRepository repo) {
    return new ProductProjection(repo);
  }

  @Bean
  public CustomerProjection customerProjection(ProjectionRepository repo) {
    return new CustomerProjection(repo);
  }

  /**
   * The GDPR erasure service: {@code forget(subjectId, requester)} deletes the subject's key, which
   * crypto-shreds the {@code @Encrypted} fields of every event that holds the customer's data. The
   * builder refuses a service without a snapshot purger unless {@code withoutSnapshotPurge()}
   * states that no aggregate snapshot exists, and none does: the command bus takes no snapshots.
   */
  @Bean
  public ForgetSubjectService forgetSubjectService(PostgresCryptoEngine cryptoEngine) {
    return ForgetSubjectService.builder().cryptoEngine(cryptoEngine).withoutSnapshotPurge().build();
  }

  @Bean
  public OffsetStore offsetStore(DataSource ds) {
    return new PostgresOffsetStore(ds);
  }

  /**
   * How often each projection reads the event store for new events: {@code
   * streamrune.polling-interval-ms} plus up to {@code streamrune.polling-jitter-ms} of jitter, five
   * seconds and one second unless configured. They are the two properties the framework's own
   * projection runner reads in the Quarkus and Micronaut apps; this app builds its runner itself,
   * so it hands them over itself. The integration tests set a shorter interval.
   */
  private static SubscriptionConfig projectionPolling(StreamRuneProperties properties) {
    return new SubscriptionConfig(
        true,
        Duration.ofMillis(properties.pollingIntervalMs()),
        Duration.ofMillis(properties.pollingJitterMs()));
  }

  @Bean(destroyMethod = "close")
  public MultiProjectionRunner projectionRunner(
      EventStore eventStore,
      OffsetStore offsetStore,
      JdbcProjectionRepository projectionRepository,
      StreamRuneProperties properties,
      ProductProjection productProjection,
      CustomerProjection customerProjection,
      CacheInvalidator cacheInvalidator) {
    var runner =
        MultiProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(projectionPolling(properties))
            // Every TRANSACTIONAL_LOCAL registration below commits its read-model write and its
            // checkpoint in ONE transaction.
            .atomicProcessor(projectionRepository)
            .register(
                "products",
                new CacheAwareProjection(productProjection, cacheInvalidator),
                ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
            .register(
                "customers",
                new CacheAwareProjection(customerProjection, cacheInvalidator),
                ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
            .build();
    runner.start();
    return runner;
  }

  // Query Bus with caching
  @Bean
  public CachingQueryBus cachingQueryBus(ProductProjection productProjection) {
    var simpleQueryBus = new SimpleQueryBus();
    var cachingBus = CachingQueryBus.builder().delegate(simpleQueryBus).build();
    cachingBus.register(ListProducts.class, (ListProducts q) -> productProjection.listAll());
    return cachingBus;
  }

  @Bean
  public CacheInvalidator cacheInvalidator(CachingQueryBus cachingBus) {
    return cachingBus.cacheInvalidator();
  }
}
```

</details>

### Step 9 — Run and verify

Start the application:

```bash
./gradlew :spring-app:bootRun
```

Register a customer:

```bash
curl -X POST http://localhost:8080/api/customers \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"c-1","name":"Alice","email":"alice@example.com","address":"123 Main St","phone":"+1234567890"}'
```

You should receive `200 OK`. Now inspect the raw event in the database to see the encrypted payload:

```bash
docker compose exec postgres psql -U postgres -d streamrune_ecommerce \
  -c "SELECT aggregate_type, aggregate_id, event_type, payload FROM event_stream WHERE aggregate_type = 'customer' AND aggregate_id = 'c-1';"
```

The `payload` column will look something like this — the PII fields are Base64-encoded ciphertext, not plaintext:

```
 aggregate_type | aggregate_id | event_type         | payload
----------------+--------------+--------------------+---------------------------------------------------------------------------------------
 customer       | c-1          | CustomerRegistered | {"customerId":"c-1","name":"3q2+7wABC...","email":"...","address":"...","phone":"..."}
```

The `customerId` is in plaintext because it is not annotated with `@Encrypted`. Every other field is opaque ciphertext.

Now query through the API, where `CryptoShreddingModule` decrypts on the way out:

```bash
curl http://localhost:8080/api/customers/c-1
```

The response returns the customer with all fields decrypted:

```json
{
  "customerId": "c-1",
  "name": "Alice",
  "email": "alice@example.com",
  "address": "123 Main St",
  "phone": "+1234567890",
  "status": "ACTIVE",
  "createdAt": "2026-06-19T10:15:30Z",
  "updatedAt": "2026-06-19T10:15:30Z"
}
```

`CustomerView` is an 8-field record, so the response also carries `createdAt` and `updatedAt` (set from the event metadata timestamp the projection recorded).

> **Heads up for chapter 9: these routes are open here.** As written in this chapter, anyone who can reach the API can register, change and read any customer: no request carries an identity yet. Chapter 9 switches the identity on and makes every customer route ask one rule — an authenticated `ADMIN`, or the customer themself — and the list return only what the caller may see. From then on the two calls above need the caller's headers, `-H 'X-User-Id: c-1' -H 'X-User-Role: CUSTOMER'`, and answer `403` without them.

You can also confirm that a per-subject key was created:

```bash
docker compose exec postgres psql -U postgres -d streamrune_ecommerce \
  -c "SELECT subject_id, length(key_bytes) AS key_bytes FROM encryption_keys;"
```

You should see one row with a 32-byte key (AES-256). `subject_id` will *not* read `c-1` — `PostgresCryptoEngine` stores the SHA-256 hex hash of the subject id, never the raw value, so PII-shaped ids (emails, usernames) never persist verbatim in this table.

---

## What We Learned

- **PII in event-sourced systems** cannot be deleted by updating a row. The standard solution is crypto-shredding: encrypt PII with a per-subject key, then delete the key when the subject exercises their right to erasure.
- **`@Encrypted(subjectId = "customerId")`** annotates a record component as PII. The `subjectId` attribute names the sibling component that holds the data subject identifier that selects the encryption key. Annotate the commands that carry personal data too: a command that fails on infrastructure is stored in the dead-letter queue, and only its own annotations keep that copy encrypted.
- **`CryptoShreddingModule`** is a Jackson `SimpleModule` that intercepts serialisation and deserialisation for any record that carries `@Encrypted` components. Domain code never calls `encrypt` or `decrypt` directly.
- **`PostgresCryptoEngine`** stores per-subject AES-256-GCM keys in an `encryption_keys` table. Keys are created lazily on the first write for a new subject and loaded on every subsequent read. Crypto-shredding is performed by `CryptoEngine.deleteKey` (chapter 7), which deletes the key row and records the tombstone and the destroyed key generation in one transaction — never delete rows from `encryption_keys` by hand: the next write would mint a fresh key, and the old events would fail to decrypt instead of reading as `[REDACTED]`.
- **Per-subject keys** mean that deleting one customer's key does not affect any other customer. Each `customerId` has its own independent key row.
- **Decider tests are unaffected by encryption.** `DeciderFixture` works with plaintext values because it exercises `decide` and `evolve` directly, without going through Jackson serialisation. This is the correct level of abstraction — encryption is infrastructure, not domain logic.

---

## Next Up

We can encrypt data, but what about the right to be forgotten? Next: GDPR compliance — deleting the encryption key with `ForgetCustomer`, verifying that replayed events return `[REDACTED]`, and understanding what happens to projections when a customer is forgotten.
