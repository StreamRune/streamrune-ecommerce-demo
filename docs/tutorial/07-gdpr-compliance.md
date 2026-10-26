# Chapter 7 — GDPR Compliance

> **What you'll learn:**
> - How crypto shredding fulfils the right to erasure without deleting or modifying any event
> - How the `/forget` endpoint dispatches `ForgetCustomer` and then calls `ForgetSubjectService.forget(...)` to delete the encryption key so all PII becomes `[REDACTED]`
> - How the endpoint answers `200` only for a complete erasure, and why sending a failed forget again finishes it
> - How a `SubjectDataPurger` removes the plaintext read-model row that crypto shredding cannot reach
> - How `RequestDataExport` records a `DataExportRequested` event for the data-portability request
> - How to wire `ForgetSubjectService` and the purger in `StreamRuneConfig` alongside the existing crypto infrastructure
> - How to verify the complete forget-then-redact cycle with `curl` and raw database inspection

---

## What We're Building and Why

Chapter 6 gave every customer their own AES-256-GCM key. Encrypted fields travel through the system as opaque ciphertext; `CryptoShreddingModule` decrypts them on the way out so the API returns plaintext. The key is the only thing that makes the data readable.

That observation is exactly how GDPR Article 17 — the right to erasure — is satisfied in an event-sourced system.

Events are immutable. You cannot delete the `CustomerRegistered` event that captured Alice's name and email. But you do not need to delete the event: you just need to make the data permanently unreadable. If you delete Alice's encryption key, then every encrypted field in every event tied to her `customerId` becomes undecryptable. `CryptoShreddingModule` does not throw on a missing key — it writes `[REDACTED]` instead. The event stream stays intact and fully replayable; it just returns redacted values for the forgotten subject. Immutability is preserved, GDPR compliance is achieved.

This approach is known as **crypto shredding**.

There is one gap crypto shredding alone does not close. The encryption only protects the `@Encrypted` event fields. Projections that consumed those events *after* decryption — like the `customers_view` read model — already copied the plaintext into their own storage. Deleting the key does nothing to that copy. So a complete erasure is two steps: shred the key, then purge the read-model row. StreamRune wires the second step through a `SubjectDataPurger`, which `ForgetSubjectService` runs automatically after deleting the key.

One more copy of the personal data exists only when something has gone wrong: a customer command that failed on infrastructure waits in the command dead-letter queue (chapter 14) for a replay. `RegisterCustomer` and `UpdateProfile` carry `@Encrypted` on their personal data (chapter 6), so that copy is encrypted under the same key and the key deletion reaches it like the events: after a forget it decrypts to `[REDACTED]`, and the retry runner refuses to replay it. A command without the annotation would stay readable in the `dead_letter_queue` table until the retention sweeper deletes it (7 days by default, the command inbox's window).

Article 20 — the right to data portability — is the request side. When a customer asks for a copy of their data, the demo dispatches a `RequestDataExport` command, which the decider records as a `DataExportRequested` domain event. That event is the durable, auditable record that an export was requested; assembling the actual export package is left as a later enhancement (see the note at the end of Step 2).

Your job is to wire `ForgetSubjectService` and write the customer purger.

---

## Step by Step

### Step 1 — Understand what the decider already does

Open `commands/src/main/java/org/streamrune/ecommerce/commands/customer/CustomerDecider.java`.

The two GDPR-relevant cases are already implemented from chapter 6:

```java
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
```

`CustomerDecider` emits domain events — it does not touch keys or purge read models. That is infrastructure territory, handled outside the decider: the controller dispatches `ForgetCustomer` (which records `CustomerForgotten`) and then calls `ForgetSubjectService` to shred the key and purge the read model (Step 2). The decider stays pure. Its rules for a forget exist because of the step that follows it:

- **An id no customer registered is refused.** Deleting a key also records a permanent tombstone for the subject id: the key store never mints a key for that id again. Forgetting an id nobody registered would block it for good, before its customer exists. The decider throws `DomainException` (HTTP 400), and the controller never reaches the key. `UpdateProfile` and `RequestDataExport` refuse such an id the same way (`Customer not found`), and that is the other half of the rule: a profile stored under an id nobody registered would be personal data no forget could ever reach.
- **A repeat records nothing.** For a customer who is already `FORGOTTEN`, `decide` returns no events and the command succeeds. A forget request that failed after the event was recorded is finished by sending it again (Step 2), and the repeat has to get past this command to reach the key deletion. Refusing it as a "double erasure" would leave the key in place for good.

### Step 2 — Wire the forget flow in the controller

`ForgetSubjectService` is the framework's GDPR Article 17 orchestrator. It is **not** triggered by an event listener and there is no hidden "forget marker" on the command result — nothing happens until something calls `forget(...)`. In the demo, the controller does that explicitly: forgetting a customer is two ordered steps, both visible in the HTTP handler.

`CustomerCommandController` injects the `ForgetSubjectService` bean (declared in `StreamRuneConfig` in chapter 6; Step 4 gives it its purgers) and uses it in the `/forget` handler. Replace the controller with this version; the other three handlers and the two request records are those of chapter 6:

```java
package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.runtime.gdpr.ForgetResult;
import org.streamrune.runtime.gdpr.ForgetSubjectService;

@RestController
@RequestMapping("/api/customers")
public class CustomerCommandController {

  private static final Logger LOG = LoggerFactory.getLogger(CustomerCommandController.class);

  private final VirtualThreadCommandBus commandBus;
  private final ForgetSubjectService forgetSubjectService;

  public CustomerCommandController(
      VirtualThreadCommandBus commandBus, ForgetSubjectService forgetSubjectService) {
    this.commandBus = commandBus;
    this.forgetSubjectService = forgetSubjectService;
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

  /** GDPR Article 20 request. */
  @PostMapping("/{id}/export-data")
  public ResponseEntity<Void> exportData(@PathVariable String id) {
    commandBus.execute(new CustomerCommand.RequestDataExport(id));
    return ResponseEntity.ok().build();
  }

  /**
   * GDPR Article 17 right to erasure. Two explicit steps, in this order:
   *
   * <ol>
   *   <li>{@code ForgetCustomer} records the {@code CustomerForgotten} domain event (audited,
   *       replayable). The decider refuses an id no customer registered ({@code 400}), so nobody
   *       can erase, and so block for good, an id before its customer exists. For a customer
   *       already forgotten it records nothing and succeeds.
   *   <li>{@link ForgetSubjectService#forget} crypto-shreds the subject's encryption key (terminal:
   *       a later encrypt for this id throws {@code SubjectForgottenException}) and runs the
   *       registered purgers to delete the customer read-model row.
   * </ol>
   *
   * <p>The answer is {@code 200} only when {@link ForgetResult#fullyErased()} is true. Otherwise it
   * is {@code 500} with a {@link ForgetResponse} saying what is left: the key is still there
   * ({@code keyDeleted=false}), or the read models in {@code failedPurgers} may still hold the
   * customer's data. Repeat the request until it answers {@code 200}; every step converges when
   * re-run. If the process dies, or the shred fails, after step 2 committed, the repeat records no
   * second event and reaches the shred again. If the shred committed and a purger failed, the
   * repeat finds the key already gone ({@code deleteKey} is a no-op then) and runs every purger
   * again (each is idempotent).
   *
   * <p>The requester id for the GDPR audit is taken from the bound request context ({@code
   * X-User-Id}).
   */
  @PostMapping("/{id}/forget")
  public ResponseEntity<ForgetResponse> forgetCustomer(@PathVariable String id) {
    commandBus.execute(new CustomerCommand.ForgetCustomer(id));
    SubjectId subjectId = SubjectId.of(id);
    String requesterId = currentRequesterId();
    ForgetResult result;
    try {
      result =
          forgetSubjectService.forget(
              subjectId, requesterId != null ? UserId.of(requesterId) : null);
    } catch (RuntimeException e) {
      // forget() throws only when deleteKey failed; it audited the failure and ran no purger.
      LOG.warn(
          "GDPR forget for subject-hash={} is incomplete: the key could not be deleted;"
              + " repeat the request",
          subjectId.redacted(),
          e);
      return ResponseEntity.internalServerError().body(ForgetResponse.keyNotDeleted());
    }
    ForgetResponse body = ForgetResponse.of(result);
    return result.fullyErased()
        ? ResponseEntity.ok(body)
        : ResponseEntity.internalServerError().body(body);
  }

  /**
   * What a forget did. {@code keyDeleted}: the subject's encryption key is gone. {@code
   * fullyErased}: the key is gone and every read-model purger succeeded. {@code failedPurgers}: the
   * read models that may still hold the customer's data.
   */
  public record ForgetResponse(
      boolean keyDeleted, boolean fullyErased, List<String> failedPurgers) {

    static ForgetResponse of(ForgetResult result) {
      return new ForgetResponse(result.keyDeleted(), result.fullyErased(), result.failedPurgers());
    }

    static ForgetResponse keyNotDeleted() {
      return new ForgetResponse(false, false, List.of());
    }
  }

  /**
   * Requester id for the GDPR audit trail: the request identity, or {@code null} when none is
   * bound.
   */
  private static String currentRequesterId() {
    if (StreamRuneContext.CURRENT.isBound()) {
      var ctx = StreamRuneContext.CURRENT.get();
      if (ctx != null && ctx.userId() != null) {
        return ctx.userId().value();
      }
    }
    return null;
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

What each step does:

- `commandBus.execute(new CustomerCommand.ForgetCustomer(id))` appends a `CustomerForgotten` event, with the two rules from Step 1: `400` for an id no customer registered, nothing recorded for a customer already forgotten.
- `forgetSubjectService.forget(subjectId, ...)` crypto-shreds the subject's key via `CryptoEngine#deleteKey`, then runs every registered `SubjectDataPurger` to delete the customer's read-model row, and returns a `ForgetResult`. The requester id (for the GDPR audit entry) comes from the bound `StreamRuneContext`'s user id. The framework takes that from the `X-User-Id` header only in the trusted-gateway mode, which Chapter 9 switches on (`streamrune.security.trust-user-id-header: true`, with the reasons and the production caveat). Until then — and for any request without `X-User-Id`, such as the `curl` calls below — the requester is `null` and the GDPR audit entry records none.
- **The answer tells the truth.** It is `200` only when `result.fullyErased()` is `true`: the key is gone and every purger succeeded. Otherwise it is `500`, and the body says what is left. `{"keyDeleted":false,"fullyErased":false,"failedPurgers":[]}` means `deleteKey` threw: `forget(...)` wrote a `FAILURE` audit row, rethrew, and ran no purger, so the key is still there. `{"keyDeleted":true,"fullyErased":false,"failedPurgers":["customers_view"]}` means a purger failed: the key is gone for good, but that read model may still hold the plaintext. `ForgetSubjectService` does not throw for a failed purger (the shred cannot be undone, so a throw would read as a rollback); it reports it in the `ForgetResult`, and the endpoint must not tell the data subject the erasure completed while `fullyErased()` is `false`.

**A failed forget is finished by sending it again.** Each step is safe to repeat, so the same request converges from wherever the last attempt stopped:

| The last attempt stopped… | What is left | What the repeat does |
|---|---|---|
| before or inside `ForgetCustomer` | nothing was written | the same as a first attempt |
| after `CustomerForgotten` was recorded, before or inside `deleteKey` (the process died, the key store was down) | the event, and the key | the command records nothing; `deleteKey` runs |
| after `deleteKey`, with a purger failing | no key; the read-model row | `deleteKey` is a no-op on a deleted key; every purger runs again |
| after everything, before the answer reached the caller | nothing | answers `200`; nothing new is written |

Nothing in the demo sends the request again for you: the caller does, or an operator who sees the `WARN` line or the `FAILURE` audit row. The event goes first, before the key, for two reasons: the decider is the one place that knows whether the customer exists, and that check must run before the key store records a permanent tombstone; and once the customer is `FORGOTTEN`, `UpdateProfile` is refused, so no new PII is encrypted under a key that is about to be deleted.

**`CustomerForgotten` also closes a window the purger leaves open.** A projection reads a page of events, decrypts it, and writes its rows afterwards. A forget can run in between: the page was decrypted while the key existed, the purger deletes the row, `forget(...)` reports `fullyErased: true`, and then the projection writes the page and puts the customer's decrypted data back into `customers_view`. Neither the key deletion nor the purger reaches a page already in memory. `CustomerProjection` handles `CustomerForgotten` by deleting the row (chapter 6), and a projection handles events in the order of the global stream, so a page decrypted before the forget is written before the projection reaches the event, whose handler removes what the page wrote; a projection that is far behind, or a rebuild, ends at the same place. That holds only while no event after `CustomerForgotten` carries the customer's data, and in the demo none does: on the customer's stream the decider refuses every command once the customer is `FORGOTTEN` (a new `RegisterCustomer` for the id is refused one step later by the key store, with `410 Gone`), and no other stream holds the customer's encrypted fields. That is why appending the event first and shredding the key second is safe: the event cannot be followed by more of the customer's data, and the shred that follows makes every earlier copy unreadable.

The `/export-data` handler only dispatches `RequestDataExport` and returns `200 OK` with an empty body. The command records a `DataExportRequested` event — see Step 5 — but the demo does not assemble or return a data package.

> **Heads up for chapter 9: this route is not guarded yet.** As written here, anyone who can reach the API can erase any customer, and erasure cannot be undone. Chapter 9 makes `OwnerOrAdminAccess.requireCustomerAccess(…)` the first line of `forgetCustomer` and `exportData`, as of every other customer route: only an authenticated `ADMIN`, or the customer themself (`X-User-Id` equal to the customer id), gets past it; anyone else gets `403` and nothing is written. It is not added here because no request carries an identity until Chapter 9 switches on the trusted-gateway mode, so the check would refuse every call, including the `curl` in Step 5. Never expose the endpoint without it. From Chapter 9 on, the calls of Step 5 need the caller's headers — `-H 'X-User-Id: c-alice' -H 'X-User-Role: CUSTOMER'` for Alice's, and likewise for Bob's.

> **Why call `forget(...)` explicitly?** Keeping it in the controller makes the two-step erasure visible and ordered: the domain event first (audited, replayable, the existence check), then the irreversible crypto-shred. No annotation magic, no hidden event listener — the StreamRune philosophy of explicit, readable code.
>
> **Optional enhancement — exporting the data.** StreamRune ships an `ExportSubjectDataService` (Article 20) that assembles a `SubjectExport` from registered `SubjectDataCollector` beans. The demo does **not** wire it — `/export-data` records the request and stops there. If you want a returned package, inject `ExportSubjectDataService` into the controller, return its `SubjectExport`, and register one `SubjectDataCollector` per read model. That is an addition beyond the real demo.

### Step 3 — Implement the CustomerSubjectDataPurger

Crypto shredding makes the `@Encrypted` event fields undecryptable, but the `customers_view` read-model row still holds the plaintext PII the projection copied in before the forget. A `SubjectDataPurger` closes that gap: `ForgetSubjectService` invokes every registered purger after deleting the key, and the customer purger simply deletes the row.

`SubjectDataPurger` is a two-method interface — `String name()` (a stable label for the GDPR audit log) and `void purge(SubjectId subjectId)` (delete or redact the subject's rows; must be idempotent).

Create `projections/src/main/java/org/streamrune/ecommerce/projections/CustomerSubjectDataPurger.java`:

```java
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
```

A few details that matter:

- The purger lives in the **`projections`** module, beside `CustomerProjection`, not in `spring-app` — it operates on the read model, and all three runtime apps (Spring, Micronaut, Quarkus) reuse it.
- `name()` returns `"customers_view"` — a stable label recorded against each purge outcome in the audit log. It must be unique within the registered set of purgers. Keep it a literal: a `name()` that throws or returns blank does not stop the purge (the forget falls back to the class name, with a WARN), but a class name changes on a rename and the audit trail should not.
- `purge(...)` is idempotent: `ProjectionRepository#delete` silently succeeds when the id is absent, so a retried or no-op forget is safe — which the SPI requires.
- The customer id *is* the subject id (recall `@Encrypted(subjectId = "customerId")` from Chapter 6), so a purge is a single delete keyed by that id.

### Step 4 — Wire the GDPR beans in StreamRuneConfig

The Spring integration *does* ship an auto-configured `ForgetSubjectService` (`@ConditionalOnBean(CryptoEngine.class)` + `@ConditionalOnMissingBean`). It collects every `SubjectDataPurger` bean, the audit store and, with `streamrune.query-cache.enabled=true`, the `CachingQueryBus`. The demo declares its own `ForgetSubjectService` bean anyway, so every piece the forget uses is named in one place and nothing depends on which beans happen to be present. Because the default is `@ConditionalOnMissingBean`, defining your own suppresses it. The auto-configured service applies the snapshot rule too: it needs a `SnapshotPurger` bean (the integration cannot build one, since which streams hold a subject is the application's mapping), or `streamrune.gdpr.snapshot-purge=none` for an application that stores no snapshot (the default is `required`), and otherwise the application does not start. Quarkus and Micronaut apply the same property.

In `StreamRuneConfig` you already declared the `PostgresCryptoEngine` bean in Chapter 6 (wired manually as a `@Bean`; the demo leaves the Spring integration's `streamrune.crypto.postgres.enabled` switch at its default, off, so the engine is built explicitly), and a `forgetSubjectService` that runs no purger and states `withoutSnapshotPurge()`. This step adds the audit store and the purger, and replaces that service with the one that runs the purgers, a snapshot purger among them. The new imports are `java.util.List`, `org.streamrune.core.types.StreamId` and `org.streamrune.ecommerce.domain.customer.CustomerState`; `PostgresAuditStore` and `PostgresSnapshotStorePurger` come with `org.streamrune.postgres.*`, `CustomerSubjectDataPurger` with `org.streamrune.ecommerce.projections.*`.

`ForgetSubjectService` writes each forget/purge outcome to the `audit_log` table via a `PostgresAuditStore`, so declare that bean now (Chapter 10 builds the full audit feature on top of it), then the purger, the service and the snapshot purger it builds:

```java
@Bean
public PostgresAuditStore auditStore(DataSource ds) {
  return new PostgresAuditStore(ds);
}
```

The purger and the service go with the projections, after `customerProjection`:

```java
@Bean
public CustomerSubjectDataPurger customerSubjectDataPurger(ProjectionRepository repo) {
  return new CustomerSubjectDataPurger(repo);
}

/**
 * GDPR Article 17 orchestrator. {@link CustomerCommandController}'s forget endpoint calls {@code
 * forget(customerId, requester)} after emitting {@code ForgetCustomer}: step 1 deletes the
 * subject's key (crypto-shred + terminal-erasure tombstone), step 2 runs every registered purger
 * to remove the subject's read-model rows, step 3 evicts the {@link CachingQueryBus}, so no
 * cached query answer taken before the erasure is served after it. The audit store records each
 * outcome in {@code audit_log} (command_type {@code GDPR_FORGET}).
 */
@Bean
public ForgetSubjectService forgetSubjectService(
    PostgresCryptoEngine cryptoEngine,
    PostgresAuditStore auditStore,
    CustomerSubjectDataPurger customerPurger,
    DataSource dataSource,
    CachingQueryBus cachingQueryBus) {
  return ForgetSubjectService.builder()
      .cryptoEngine(cryptoEngine)
      .auditStore(auditStore)
      .purgers(List.of(customerPurger, customerSnapshotPurger(dataSource)))
      .queryCache(cachingQueryBus)
      .build();
}

/**
 * Purges the snapshots of a forgotten customer. A snapshot holds the aggregate's state as plain
 * JSON, and {@code CustomerState} carries the customer's name, email, address and phone in the
 * clear (the encryption lives on the events), so the key shred alone leaves them readable in
 * {@code snapshot_store}. The purger empties the state of the customer stream's snapshot row and
 * raises its purge generation, so a command that loaded the customer before the forget cannot
 * store its snapshot again. The customer's stream is {@code customer:<subject id>}.
 */
static PostgresSnapshotStorePurger customerSnapshotPurger(DataSource dataSource) {
  return new PostgresSnapshotStorePurger(
      dataSource,
      subject -> List.of(StreamId.of(CustomerState.TYPE, AggregateId.of(subject.value()))));
}
```

This is the bean `CustomerCommandController` injects in Step 2. The wiring is deliberately explicit:

- `.cryptoEngine(cryptoEngine)` — the same `PostgresCryptoEngine` that backs the event store, so `forget(...)` deletes the very key that encrypts the subject's fields.
- `.auditStore(auditStore)` — each forget and each purge outcome is written to the `audit_log` table.
- `.purgers(List.of(customerPurger, customerSnapshotPurger(dataSource)))` — the purger list `forget(...)` runs after the key is shredded. As you add aggregates in later chapters, register a purger per read model holding PII and add it to this list. The second purger clears the customer's snapshot: a snapshot (chapter 8) holds the aggregate's *decrypted* state as plain JSON, so `CustomerState` would keep the name, email, address and phone readable in `snapshot_store` after the key is gone. `PostgresSnapshotStorePurger` takes the streams to clear for a subject, here `customer:<subject id>`. It does not delete the stream's row: it empties the state (`state_type` and `state_payload` become `NULL`) and raises the row's purge generation. A command that loaded the customer before the forget saves its snapshot only while the row still has the generation that load saw, so it cannot put the cleared state back; the next command after the forget replays the events, whose encrypted fields read `[REDACTED]`. The demo wires the purger in all three apps. The snapshot purger is not a recommendation: `ForgetSubjectService.Builder.build()` refuses a service whose purgers hold no `SnapshotPurger` (`PostgresSnapshotStorePurger` is the PostgreSQL one) unless the application calls `withoutSnapshotPurge()` to say it has never stored a snapshot, and it refuses a purger and `withoutSnapshotPurge()` together. A snapshot keeps every field of the aggregate's state that is not marked `@Encrypted` in the clear, so deleting the key does not reach it, and the framework makes the application decide rather than leave snapshots behind by default.
- `.queryCache(cachingQueryBus)` — the `CachingQueryBus` from Chapter 5. After the last purger, `forget(...)` evicts every cached query answer, so none taken before the erasure (a decrypted field, or a row the purger just deleted) is served after it. If the eviction fails, `fullyErased()` is `false`, like a failed purge.

The whole class at the end of this chapter:

<details>
<summary><code>StreamRuneConfig.java</code> at the end of Chapter 7</summary>

```java
package org.streamrune.ecommerce.spring.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.time.Duration;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.*;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
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

  @Bean
  public PostgresAuditStore auditStore(DataSource ds) {
    return new PostgresAuditStore(ds);
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
   * Read-model purger for the customer subject. {@link ForgetSubjectService} runs it after the
   * encryption key is shredded, deleting the {@code customers_view} row so the forget removes the
   * plaintext PII the projection captured (crypto-shredding alone only kills the ciphertext).
   */
  @Bean
  public CustomerSubjectDataPurger customerSubjectDataPurger(ProjectionRepository repo) {
    return new CustomerSubjectDataPurger(repo);
  }

  /**
   * GDPR Article 17 orchestrator. {@link CustomerCommandController}'s forget endpoint calls {@code
   * forget(customerId, requester)} after emitting {@code ForgetCustomer}: step 1 deletes the
   * subject's key (crypto-shred + terminal-erasure tombstone), step 2 runs every registered purger
   * to remove the subject's read-model rows, step 3 evicts the {@link CachingQueryBus}, so no
   * cached query answer taken before the erasure is served after it. The audit store records each
   * outcome in {@code audit_log} (command_type {@code GDPR_FORGET}).
   */
  @Bean
  public ForgetSubjectService forgetSubjectService(
      PostgresCryptoEngine cryptoEngine,
      PostgresAuditStore auditStore,
      CustomerSubjectDataPurger customerPurger,
      DataSource dataSource,
      CachingQueryBus cachingQueryBus) {
    return ForgetSubjectService.builder()
        .cryptoEngine(cryptoEngine)
        .auditStore(auditStore)
        .purgers(List.of(customerPurger, customerSnapshotPurger(dataSource)))
        .queryCache(cachingQueryBus)
        .build();
  }

  /**
   * Purges the snapshots of a forgotten customer. A snapshot holds the aggregate's state as plain
   * JSON, and {@code CustomerState} carries the customer's name, email, address and phone in the
   * clear (the encryption lives on the events), so the key shred alone leaves them readable in
   * {@code snapshot_store}. The purger empties the state of the customer stream's snapshot row and
   * raises its purge generation, so a command that loaded the customer before the forget cannot
   * store its snapshot again. The customer's stream is {@code customer:<subject id>}.
   */
  static PostgresSnapshotStorePurger customerSnapshotPurger(DataSource dataSource) {
    return new PostgresSnapshotStorePurger(
        dataSource,
        subject -> List.of(StreamId.of(CustomerState.TYPE, AggregateId.of(subject.value()))));
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

### Step 5 — Verify the full cycle

Start the application:

```bash
./gradlew :spring-app:bootRun
```

**Register Alice Smith:**

```bash
curl -X POST http://localhost:8080/api/customers \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"c-alice","name":"Alice Smith","email":"alice@example.com","address":"1 Main St","phone":"+1234567890"}'
```

When your code reads this stream back through the event store, the `@Encrypted` fields are decrypted transparently — `name` and `email` come back as `Alice Smith` / `alice@example.com`. (You will add a read-only `GET /api/events/{aggregateType}/{aggregateId}` endpoint that shows this in Chapter 15's Event Explorer; for now we inspect the stored data directly with `psql`.)

**Confirm the raw database row is encrypted:**

```bash
docker compose exec postgres psql -U postgres -d streamrune_ecommerce \
  -c "SELECT aggregate_type, aggregate_id, event_type, payload->>'name' AS name_field FROM event_stream WHERE aggregate_type = 'customer' AND aggregate_id = 'c-alice';"
```

The `name_field` column will show Base64-encoded ciphertext, not `Alice Smith`. The encryption key is still present. `PostgresCryptoEngine` stores the SHA-256 hex hash of the subject id in `subject_id`, not the raw id, so the WHERE clause hashes `'c-alice'` the same way before comparing:

```bash
docker compose exec postgres psql -U postgres -d streamrune_ecommerce \
  -c "SELECT subject_id FROM encryption_keys WHERE subject_id = encode(sha256(convert_to('c-alice', 'UTF8')), 'hex');"
```

**Forget Alice:**

```bash
curl -i -X POST http://localhost:8080/api/customers/c-alice/forget
```

```
HTTP/1.1 200
Content-Type: application/json

{"keyDeleted":true,"fullyErased":true,"failedPurgers":[]}
```

The forget endpoint ran both steps: it dispatched `ForgetCustomer` (appending `CustomerForgotten`) and then `forgetSubjectService.forget(SubjectId.of("c-alice"), ...)`, which deleted the key and ran the purger. `fullyErased` is `true`, so the answer is `200`. Send the same request again: it answers the same way and records no second event. Forget an id nobody registered, such as `c-nobody`, and the answer is `400 Customer not found`, with no tombstone written. The key row is now deleted (same hashed lookup as above):

```bash
docker compose exec postgres psql -U postgres -d streamrune_ecommerce \
  -c "SELECT subject_id FROM encryption_keys WHERE subject_id = encode(sha256(convert_to('c-alice', 'UTF8')), 'hex');"
```

The query returns zero rows.

The purger also deleted the plaintext read-model row, so the `customers_view` table holds none of Alice's data:

```bash
docker compose exec postgres psql -U postgres -d streamrune_ecommerce \
  -c "SELECT id FROM customers_view WHERE id = 'c-alice';"
```

This too returns zero rows — crypto shredding killed the ciphertext in the event stream, and the purger removed the decrypted copy the projection had stored.

**The event stream is intact, but the PII is now unrecoverable.** The `CustomerRegistered` event still sits in `event_stream` — event sourcing never rewrites history — and its `@Encrypted` columns still hold the same Base64 ciphertext as before:

```bash
docker compose exec postgres psql -U postgres -d streamrune_ecommerce \
  -c "SELECT event_type, payload->>'name' AS name_field FROM event_stream WHERE aggregate_type = 'customer' AND aggregate_id = 'c-alice' ORDER BY version;"
```

You will see the original `CustomerRegistered` ciphertext plus a new `CustomerForgotten` row. But that ciphertext is now **permanently undecryptable**: its key was deleted (zero rows above). When the application reads this stream — the event store's `load`, and the Event Explorer endpoint of Chapter 15 — `CryptoShreddingModule` finds no key and substitutes `[REDACTED]` for every annotated field:

```json
{ "name": "[REDACTED]", "email": "[REDACTED]", "address": "[REDACTED]", "phone": "[REDACTED]" }
```

This is crypto-shredding: the data is erased not by deleting events, but by destroying the only key that could ever decrypt them.

**Request a data export:**

Register a second customer so you have a live subject to request an export for:

```bash
curl -X POST http://localhost:8080/api/customers \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"c-bob","name":"Bob Jones","email":"bob@example.com","address":"2 Oak Ave","phone":"+0987654321"}'
```

Request Bob's export:

```bash
curl -i -X POST http://localhost:8080/api/customers/c-bob/export-data
```

The response is `200 OK` with **no body** — the endpoint only dispatches `RequestDataExport`. The portability request is recorded durably as a `DataExportRequested` event in Bob's stream:

```bash
docker compose exec postgres psql -U postgres -d streamrune_ecommerce \
  -c "SELECT version, event_type, payload->>'customerId' AS customer FROM event_stream WHERE aggregate_type = 'customer' AND aggregate_id = 'c-bob' ORDER BY version;"
```

```
 version |     event_type      | customer
---------+---------------------+----------
       1 | CustomerRegistered  | c-bob
       2 | DataExportRequested | c-bob
```

The demo stops at recording the request. To turn that request into a returned data package, follow the optional enhancement in Step 2 — inject `ExportSubjectDataService` into the controller, register a `SubjectDataCollector` per read model, and return the resulting `SubjectExport`. The framework's export service calls every registered collector and merges their named sections into one portable document.

---

## What We Learned

- **Crypto shredding** is the standard technique for GDPR erasure in event-sourced systems. Deleting the per-subject encryption key makes every `@Encrypted` field permanently unreadable on replay — across all events and all future reads. The event store remains intact. Crypto shredding does **not** reach read models that already stored decrypted plaintext; those need a separate purge.
- **`ForgetSubjectService`** wraps `CryptoEngine#deleteKey` with audit logging and idempotency guarantees, then runs the registered purgers and returns a `ForgetResult`. Calling it twice for the same subject is safe. It is **not** triggered by an event listener and there is no "forget marker" — the controller calls `forgetSubjectService.forget(subjectId, requester)` explicitly, right after dispatching `ForgetCustomer`.
- **Report what actually happened.** The endpoint answers `200` only when `ForgetResult.fullyErased()` is `true`; a failed key deletion or a failed purger answers `500` with `keyDeleted`, `fullyErased` and `failedPurgers`. Every step is idempotent and the decider records nothing for a customer already forgotten, so sending the request again finishes an erasure that stopped part-way.
- **`SubjectDataPurger`** is how the demo achieves *complete* erasure. `CustomerSubjectDataPurger` (`name()` = `"customers_view"`) deletes the read-model row that crypto shredding cannot touch. Register one purger per read model holding PII and pass them into `ForgetSubjectService.builder().purgers(...)`.
- **Explicit wiring keeps the forget readable.** The Spring integration auto-configures a `ForgetSubjectService` when a `CryptoEngine` bean is present, collecting the `SubjectDataPurger` beans, the audit store and the `CachingQueryBus`. Because that bean is `@ConditionalOnMissingBean`, the demo's own `forgetSubjectService` `@Bean` — built with `.purgers(List.of(customerPurger, customerSnapshotPurger(dataSource)))` and `.queryCache(cachingQueryBus)` — replaces it. The `CryptoEngine` itself is a manual `@Bean` (`PostgresCryptoEngine`); the integration's `streamrune.crypto.postgres.enabled` switch stays off.
- **Right to erasure (Article 17)** does not require deleting events. It requires that the personal data becomes inaccessible everywhere it is stored: the key shred handles the encrypted event fields and the encrypted copy of a dead-lettered command, the read-model purger handles the read model, and the snapshot purger empties the customer's snapshot (its row stays, without a state and with a higher purge generation, so a command loaded before the forget cannot store the old state again).
- **Right to data portability (Article 20)** is the request side. The demo dispatches `RequestDataExport`, recording a `DataExportRequested` event; `/export-data` returns `200` with no body. Assembling and returning a `SubjectExport` (via `ExportSubjectDataService` + `SubjectDataCollector` beans) is an optional enhancement the framework supports but the demo does not wire.
- **The projection deletes on forget.** `CustomerProjection` handles `CustomerForgotten` by calling the inherited `delete(customerId)` helper — it drops the `customers_view` row rather than leaving a redacted shell, inside the same transaction as the rest of the batch, so a customer registered and forgotten in one batch leaves no row. This agrees with `CustomerSubjectDataPurger`, which deletes the same row, so projection and purger converge on the same terminal state ("row gone") regardless of which observes the forget first, and it closes the window in which a projection page decrypted before the forget writes the row back after the purger ran.

---

## Next Up

Now let's add orders with a complex lifecycle — and learn about snapshots.
