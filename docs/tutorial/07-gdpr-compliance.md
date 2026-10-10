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

One more copy of the personal data exists only when something has gone wrong: a customer command that failed on infrastructure waits in the command dead-letter queue (chapter 14) for a replay. `RegisterCustomer` and `UpdateProfile` carry `@Encrypted` on their personal data (chapter 6), so that copy is encrypted under the same key and the key deletion reaches it like the events: after a forget it decrypts to `[REDACTED]`, and the retry runner refuses to replay it. A command without the annotation would stay readable in the `dead_letter_queue` table until the retention sweeper deletes it (30 days by default).

Article 20 — the right to data portability — is the request side. When a customer asks for a copy of their data, the demo dispatches a `RequestDataExport` command, which the decider records as a `DataExportRequested` domain event. That event is the durable, auditable record that an export was requested; assembling the actual export package is left as a later enhancement (see the note at the end of Step 2).

Your job is to wire `ForgetSubjectService` and write the customer purger.

---

## Step by Step

### Step 1 — Understand what the decider already does

Open `commands/src/main/java/org/streamrune/ecommerce/commands/customer/CustomerDecider.java`.

The two GDPR-relevant cases are already implemented from chapter 6:

```java
case CustomerCommand.RequestDataExport c ->
    List.of(new CustomerEvent.DataExportRequested(c.customerId()));

case CustomerCommand.ForgetCustomer c -> {
    if (state.customerId() == null) throw new DomainException("Customer not found");
    if (state.status() == CustomerStatus.FORGOTTEN) yield List.of();
    yield List.of(new CustomerEvent.CustomerForgotten(c.customerId()));
}
```

`CustomerDecider` emits domain events — it does not touch keys or purge read models. That is infrastructure territory, handled outside the decider: the controller dispatches `ForgetCustomer` (which records `CustomerForgotten`) and then calls `ForgetSubjectService` to shred the key and purge the read model (Step 2). The decider stays pure. Its two rules exist because of the step that follows it:

- **An id no customer registered is refused.** Deleting a key also records a permanent tombstone for the subject id: the key store never mints a key for that id again. Forgetting an id nobody registered would block it for good, before its customer exists. The decider throws `DomainException` (HTTP 400), and the controller never reaches the key.
- **A repeat records nothing.** For a customer who is already `FORGOTTEN`, `decide` returns no events and the command succeeds. A forget request that failed after the event was recorded is finished by sending it again (Step 2), and the repeat has to get past this command to reach the key deletion. Refusing it as a "double erasure" would leave the key in place for good.

### Step 2 — Wire the forget flow in the controller

`ForgetSubjectService` is the framework's GDPR Article 17 orchestrator. It is **not** triggered by an event listener and there is no hidden "forget marker" on the command result — nothing happens until something calls `forget(...)`. In the demo, the controller does that explicitly: forgetting a customer is two ordered steps, both visible in the HTTP handler.

`CustomerCommandController` injects the `ForgetSubjectService` bean (wired in `StreamRuneConfig`, Step 4) and uses it in the `/forget` handler:

```java
package org.streamrune.ecommerce.spring.controller;

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

    // registerCustomer and updateProfile stay as they are from chapter 6.

    @PostMapping("/{id}/forget")
    public ResponseEntity<ForgetResponse> forgetCustomer(@PathVariable String id) {
        // Step 1: record the CustomerForgotten domain event. The decider refuses an id no
        // customer registered (HTTP 400) and records nothing for a customer already forgotten.
        commandBus.execute(new CustomerCommand.ForgetCustomer(id));
        // Step 2: crypto-shred the key and run the registered purgers. Both are idempotent.
        SubjectId subjectId = SubjectId.of(id);
        String requesterId = currentRequesterId();
        ForgetResult result;
        try {
            result = forgetSubjectService.forget(
                subjectId, requesterId != null ? UserId.of(requesterId) : null);
        } catch (RuntimeException e) {
            // forget() throws only when deleteKey failed; it audited the failure and ran no purger.
            LOG.warn("GDPR forget for subject-hash={} is incomplete: the key could not be deleted;"
                + " repeat the request", subjectId.redacted(), e);
            return ResponseEntity.internalServerError().body(ForgetResponse.keyNotDeleted());
        }
        ForgetResponse body = ForgetResponse.of(result);
        return result.fullyErased()
            ? ResponseEntity.ok(body)
            : ResponseEntity.internalServerError().body(body);
    }

    @PostMapping("/{id}/export-data")
    public ResponseEntity<Void> exportData(@PathVariable String id) {
        commandBus.execute(new CustomerCommand.RequestDataExport(id));
        return ResponseEntity.ok().build();
    }

    /** What a forget did, so the caller can tell a complete erasure from a partial one. */
    public record ForgetResponse(boolean keyDeleted, boolean fullyErased, List<String> failedPurgers) {

        static ForgetResponse of(ForgetResult result) {
            return new ForgetResponse(result.keyDeleted(), result.fullyErased(), result.failedPurgers());
        }

        static ForgetResponse keyNotDeleted() {
            return new ForgetResponse(false, false, List.of());
        }
    }

    /** Requester id for the GDPR audit trail, read from the request-scoped context; null if unbound. */
    private static String currentRequesterId() {
        if (StreamRuneContext.CURRENT.isBound()) {
            var ctx = StreamRuneContext.CURRENT.get();
            if (ctx != null && ctx.userId() != null) {
                return ctx.userId().value();
            }
        }
        return null;
    }
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

The `/export-data` handler only dispatches `RequestDataExport` and returns `200 OK` with an empty body. The command records a `DataExportRequested` event — see Step 5 — but the demo does not assemble or return a data package.

> **Heads up for chapter 9: this route is not guarded yet.** As written here, anyone who can reach the API can erase any customer, and erasure cannot be undone. Chapter 9 makes `requireAdminOrSelf(id)` the first line of `forgetCustomer` and `exportData`: only an ADMIN, or the customer themself (`X-User-Id` equal to the customer id), gets past it; anyone else gets `403` and nothing is written. It is not added here because no request carries an identity until Chapter 9 switches on the trusted-gateway mode, so the check would refuse every call, including the `curl` in Step 5. Never expose the endpoint without it.

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

The Spring integration *does* ship an auto-configured `ForgetSubjectService` (`@ConditionalOnBean(CryptoEngine.class)` + `@ConditionalOnMissingBean`). It collects every `SubjectDataPurger` bean, the audit store and, with `streamrune.query-cache.enabled=true`, the `CachingQueryBus`. The demo declares its own `ForgetSubjectService` bean anyway, so every piece the forget uses is named in one place and nothing depends on which beans happen to be present. Because the default is `@ConditionalOnMissingBean`, defining your own suppresses it.

In `StreamRuneConfig` you already declared the `PostgresCryptoEngine` bean in Chapter 6 (wired manually as a `@Bean`; the demo leaves the Spring integration's `streamrune.crypto.postgres.enabled` switch at its default, off, so the engine is built explicitly). Add three more beans: the audit store, the purger, and the purger-equipped forget service.

`ForgetSubjectService` writes each forget/purge outcome to the `audit_log` table via a `PostgresAuditStore`, so declare that bean now (Chapter 10 builds the full audit feature on top of it):

```java
import java.util.List;
import org.streamrune.postgres.PostgresAuditStore;
import org.streamrune.runtime.CachingQueryBus;
import org.streamrune.runtime.gdpr.ForgetSubjectService;
import org.streamrune.ecommerce.projections.CustomerSubjectDataPurger;

// ...

@Bean
public PostgresAuditStore auditStore(DataSource ds) {
    return new PostgresAuditStore(ds);
}

@Bean
public CustomerSubjectDataPurger customerSubjectDataPurger(ProjectionRepository repo) {
    return new CustomerSubjectDataPurger(repo);
}

@Bean
public ForgetSubjectService forgetSubjectService(
        PostgresCryptoEngine cryptoEngine,
        PostgresAuditStore auditStore,
        CustomerSubjectDataPurger customerPurger,
        CachingQueryBus cachingQueryBus) {
    return ForgetSubjectService.builder()
            .cryptoEngine(cryptoEngine)
            .auditStore(auditStore)
            .purgers(List.of(customerPurger))
            .queryCache(cachingQueryBus)
            .build();
}
```

This is the bean `CustomerCommandController` injects in Step 2. The wiring is deliberately explicit:

- `.cryptoEngine(cryptoEngine)` — the same `PostgresCryptoEngine` that backs the event store, so `forget(...)` deletes the very key used to encrypt the subject's fields.
- `.auditStore(auditStore)` — each forget and each purge outcome is written to the `audit_log` table.
- `.purgers(List.of(customerPurger))` — the purger list `forget(...)` runs after the key is shredded. As you add aggregates in later chapters, register a purger per read model holding PII and add it to this list.
- `.queryCache(cachingQueryBus)` — the `CachingQueryBus` from Chapter 5. After the last purger, `forget(...)` evicts every cached query answer, so none taken before the erasure (a decrypted field, or a row the purger just deleted) is served after it. If the eviction fails, `fullyErased()` is `false`, like a failed purge.

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

> The `psql` commands below use `docker exec -it streamrune-pg ...`, matching Chapter 6. That assumes you gave the Postgres container the name `streamrune-pg`. The demo's `docker-compose.yml` does not set `container_name`, so Compose generates one like `streamrune-ecommerce-demo-postgres-1`. Either add `container_name: streamrune-pg` under the `postgres` service, or run the same SQL with `docker compose exec postgres psql -U postgres -d streamrune_ecommerce -c "..."`.

```bash
docker exec -it streamrune-pg psql -U postgres -d streamrune_ecommerce \
  -c "SELECT aggregate_type, aggregate_id, event_type, payload->>'name' AS name_field FROM event_stream WHERE aggregate_type = 'customer' AND aggregate_id = 'c-alice';"
```

The `name_field` column will show Base64-encoded ciphertext, not `Alice Smith`. The encryption key is still present. `PostgresCryptoEngine` stores the SHA-256 hex hash of the subject id in `subject_id`, not the raw id, so the WHERE clause hashes `'c-alice'` the same way before comparing:

```bash
docker exec -it streamrune-pg psql -U postgres -d streamrune_ecommerce \
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
docker exec -it streamrune-pg psql -U postgres -d streamrune_ecommerce \
  -c "SELECT subject_id FROM encryption_keys WHERE subject_id = encode(sha256(convert_to('c-alice', 'UTF8')), 'hex');"
```

The query returns zero rows.

The purger also deleted the plaintext read-model row, so the `customers_view` table no longer holds Alice's data:

```bash
docker exec -it streamrune-pg psql -U postgres -d streamrune_ecommerce \
  -c "SELECT id FROM customers_view WHERE id = 'c-alice';"
```

This too returns zero rows — crypto shredding killed the ciphertext in the event stream, and the purger removed the decrypted copy the projection had stored.

**The event stream is intact, but the PII is now unrecoverable.** The `CustomerRegistered` event still sits in `event_stream` — event sourcing never rewrites history — and its `@Encrypted` columns still hold the same Base64 ciphertext as before:

```bash
docker exec -it streamrune-pg psql -U postgres -d streamrune_ecommerce \
  -c "SELECT event_type, payload->>'name' AS name_field FROM event_stream WHERE aggregate_type = 'customer' AND aggregate_id = 'c-alice' ORDER BY version;"
```

You will see the original `CustomerRegistered` ciphertext plus a new `CustomerForgotten` row. But that ciphertext is now **permanently undecryptable**: its key was deleted (zero rows above). When you read this stream through the app's events endpoint (Chapter 15's Event Explorer), `CryptoShreddingModule` fails to decrypt and substitutes `[REDACTED]` for every annotated field — so the API returns:

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
curl -H "X-User-Role: ADMIN" http://localhost:8080/api/events/customer/c-bob
```

```json
[
  {
    "globalOffset": 3,
    "streamId": "customer:c-bob",
    "aggregateType": "customer",
    "aggregateId": "c-bob",
    "eventType": "CustomerRegistered",
    "timestamp": "2026-04-25T10:35:00Z",
    "payload": {
      "customerId": "c-bob",
      "name": "Bob Jones",
      "email": "bob@example.com",
      "address": "2 Oak Ave",
      "phone": "+0987654321"
    }
  },
  {
    "globalOffset": 4,
    "streamId": "customer:c-bob",
    "aggregateType": "customer",
    "aggregateId": "c-bob",
    "eventType": "DataExportRequested",
    "timestamp": "2026-04-25T10:36:00Z",
    "payload": { "customerId": "c-bob" }
  }
]
```

The demo stops at recording the request. To turn that request into a returned data package, follow the optional enhancement in Step 2 — inject `ExportSubjectDataService` into the controller, register a `SubjectDataCollector` per read model, and return the resulting `SubjectExport`. The framework's export service calls every registered collector and merges their named sections into one portable document.

---

## What We Learned

- **Crypto shredding** is the standard technique for GDPR erasure in event-sourced systems. Deleting the per-subject encryption key makes every `@Encrypted` field permanently unreadable on replay — across all events and all future reads. The event store remains intact. Crypto shredding does **not** reach read models that already stored decrypted plaintext; those need a separate purge.
- **`ForgetSubjectService`** wraps `CryptoEngine#deleteKey` with audit logging and idempotency guarantees, then runs the registered purgers and returns a `ForgetResult`. Calling it twice for the same subject is safe. It is **not** triggered by an event listener and there is no "forget marker" — the controller calls `forgetSubjectService.forget(subjectId, requester)` explicitly, right after dispatching `ForgetCustomer`.
- **Report what actually happened.** The endpoint answers `200` only when `ForgetResult.fullyErased()` is `true`; a failed key deletion or a failed purger answers `500` with `keyDeleted`, `fullyErased` and `failedPurgers`. Every step is idempotent and the decider records nothing for a customer already forgotten, so sending the request again finishes an erasure that stopped part-way.
- **`SubjectDataPurger`** is how the demo achieves *complete* erasure. `CustomerSubjectDataPurger` (`name()` = `"customers_view"`) deletes the read-model row that crypto shredding cannot touch. Register one purger per read model holding PII and pass them into `ForgetSubjectService.builder().purgers(...)`.
- **Explicit wiring keeps the forget readable.** The Spring integration auto-configures a `ForgetSubjectService` when a `CryptoEngine` bean is present, collecting the `SubjectDataPurger` beans, the audit store and the `CachingQueryBus`. Because that bean is `@ConditionalOnMissingBean`, the demo's own `forgetSubjectService` `@Bean` — built with `.purgers(List.of(customerPurger))` and `.queryCache(cachingQueryBus)` — replaces it. The `CryptoEngine` itself is a manual `@Bean` (`PostgresCryptoEngine`); the integration's `streamrune.crypto.postgres.enabled` switch stays off.
- **Right to erasure (Article 17)** does not require deleting events. It requires that the personal data becomes inaccessible everywhere it is stored: the key shred handles the encrypted event fields and the encrypted copy of a dead-lettered command, the purger handles the read model.
- **Right to data portability (Article 20)** is the request side. The demo dispatches `RequestDataExport`, recording a `DataExportRequested` event; `/export-data` returns `200` with no body. Assembling and returning a `SubjectExport` (via `ExportSubjectDataService` + `SubjectDataCollector` beans) is an optional enhancement the framework supports but the demo does not wire.
- **The projection deletes on forget.** `CustomerProjection` handles `CustomerForgotten` by calling the inherited `delete(customerId)` helper — it drops the `customers_view` row rather than leaving a redacted shell, inside the same transaction as the rest of the batch, so a customer registered and forgotten in one batch leaves no row. This agrees with `CustomerSubjectDataPurger`, which deletes the same row, so projection and purger converge on the same terminal state ("row gone") regardless of which observes the forget first.

---

## Next Up

Now let's add orders with a complex lifecycle — and learn about snapshots.
