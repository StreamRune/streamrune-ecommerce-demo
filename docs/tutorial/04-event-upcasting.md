# Chapter 4 — Event Upcasting

> **What you'll learn:**
> - Why stored events cannot be changed and what upcasting does instead
> - How to implement `EventUpcaster` to migrate a v1 event to v2 at read time
> - How to test an upcaster with plain JUnit before wiring it into the store
> - How to register an upcaster as an `EventUpcaster` bean picked up by Spring auto-config
> - How `UpcasterChain` and `EventTypeRegistry` interact during stream replay

---

## What We're Building and Why

In chapter 2 we defined `ProductCreated` with a `category` field from the start. In a real project, though, it is common to ship the first version of an event without a field — and only add it later when a new requirement arrives. Suppose the original `ProductCreated` record looked like this:

```java
// v1 — shipped without a category field
record ProductCreated(
    String productId, String name, String description,
    Money price, int stock)
    implements ProductEvent {}
```

After v1 events are stored in PostgreSQL, you cannot alter them. The raw JSON rows on disk are immutable facts — that is the entire point of an event store. You also cannot simply change the Java record and add `category`, because the deserialiser would fail when it reads an old row that has no `"category"` key.

The solution is an **upcaster**: a small function that is called when the event store reads a raw event payload, *before* deserialisation. The upcaster receives the raw `Map<String, Object>` payload and the version number of the stored event, and returns a transformed `Map` that matches the current schema. In our case: if the stored version is 1 and the payload has no `category`, add `"category": "Uncategorized"`.

Nothing changes on disk. Every old event is upgraded in memory as it is loaded. Events written once the upcaster is registered are stamped with schema version 2, so the store does not call the upcaster for them at all.

---

## Step by Step

### Step 1 — Understand the scenario

Our current `ProductCreated` record has `category` in it (as built in chapter 2). For this chapter, imagine you are adding upcasting retroactively: events written before the `category` field was introduced are still sitting in the database without it. When the `CommandBus` replays those events to rebuild `ProductState`, the deserialiser would set `category` to `null`. An upcaster fills in the default so the rest of the system never sees a null category.

The upcast contract:

- **`eventType()`** — the event type name that this upcaster handles (must match the name registered in `SimpleEventTypeRegistry`).
- **`currentVersion()`** — the version this upcaster produces. The event store uses this to decide whether to call the upcaster for a given stored event.
- **`upcast(Map<String, Object> eventData, int fromVersion) → Map`** — transforms a v`fromVersion` payload into v`fromVersion+1`. The `UpcasterChain` calls it once per version step until it reaches `currentVersion()`.

**What the stored version means.** When the event store writes an event, it records in the `schema_version` column the `currentVersion()` of the upcaster registered for that event type *at that moment* — `1` when there is none. It does not look at the payload. That matters right here: the products you created in chapters 2 and 3 already carry a `category`, but they were written before this upcaster existed, so they are stamped `1`, and from now on every load runs the v1→v2 step on them too. A step that unconditionally `put`s the default would replace their real category (say `"Gadgets"`) with `"Uncategorized"` on every read — silently, and in the read model as well. Any deployment that adds a field in one release and registers its upcaster in a later one has the same window. The rule: every step must leave a payload that already has the target shape unchanged. For a new field with a default, that means `putIfAbsent`, not `put`.

### Step 2 — Write the test first

Create `commands/src/test/java/org/streamrune/ecommerce/commands/upcaster/ProductCreatedUpcasterTest.java`:

```java
package org.streamrune.ecommerce.commands.upcaster;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.EventType;

class ProductCreatedUpcasterTest {

    private final ProductCreatedUpcaster upcaster = new ProductCreatedUpcaster();

    @Test
    void eventType() {
        assertThat(upcaster.eventType()).isEqualTo(new EventType("ProductCreated"));
    }

    @Test
    void currentVersion() {
        assertThat(upcaster.currentVersion()).isEqualTo(2);
    }

    @Test
    void upcast_v1_to_v2_addsCategoryField() {
        Map<String, Object> v1 = new HashMap<>();
        v1.put("productId", "p-1");
        v1.put("name", "Widget");
        v1.put("description", "A widget");
        v1.put("price", Map.of("amount", 10, "currency", "USD"));
        v1.put("stock", 50);

        Map<String, Object> v2 = upcaster.upcast(v1, 1);

        assertThat(v2).containsEntry("category", "Uncategorized");
        assertThat(v2).containsEntry("productId", "p-1");
        assertThat(v2).containsEntry("name", "Widget");
    }

    @Test
    void upcast_v1_keepsACategoryThePayloadAlreadyCarries() {
        // A product created before this upcaster was registered is stamped 1
        // although its payload already has a category: leave that category alone.
        Map<String, Object> stampedV1 = new HashMap<>();
        stampedV1.put("productId", "p-2");
        stampedV1.put("category", "Gadgets");

        Map<String, Object> v2 = upcaster.upcast(stampedV1, 1);

        assertThat(v2).containsEntry("category", "Gadgets");
    }

    @Test
    void upcast_v1_fillsANullCategory() {
        Map<String, Object> v1 = new HashMap<>();
        v1.put("productId", "p-3");
        v1.put("category", null);

        Map<String, Object> v2 = upcaster.upcast(v1, 1);

        assertThat(v2).containsEntry("category", "Uncategorized");
    }
}
```

The tests cover the event type name (so the upcaster is matched to the right events), the current version `2` (so the store calls it only for events stamped below 2), a v1 payload without `category` getting `"Uncategorized"`, a v1-stamped payload that already carries a category keeping it — the chapter 2 and 3 products from Step 1 — and a `null` category being filled in like a missing one.

Run it now — the module will not compile until you create `ProductCreatedUpcaster` in Step 3:

```bash
./gradlew :commands:test --tests "*.ProductCreatedUpcasterTest"
```

Expected: compilation error. This is our starting point — the test exists but its subject does not.

### Step 3 — Implement the upcaster

Create `commands/src/main/java/org/streamrune/ecommerce/commands/upcaster/ProductCreatedUpcaster.java`:

```java
package org.streamrune.ecommerce.commands.upcaster;

import java.util.Map;
import org.streamrune.core.types.EventType;
import org.streamrune.core.upcasting.EventUpcaster;

public class ProductCreatedUpcaster implements EventUpcaster {

    @Override
    public EventType eventType() {
        return new EventType("ProductCreated");
    }

    @Override
    public int currentVersion() {
        return 2;
    }

    @Override
    public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
        if (fromVersion == 1) {
            eventData.putIfAbsent("category", "Uncategorized");
        }
        return eventData;
    }
}
```

A few implementation notes:

- The method mutates `eventData` in place and returns it. This is acceptable because the map is a transient copy created by the deserialisation layer — no caller holds a reference to the original.
- `putIfAbsent` sets the category only when the payload has none (or `null`), so the step changes nothing on a v1-stamped payload that already carries one.
- The `if (fromVersion == 1)` guard ties the change to the v1→v2 step. The store never calls the upcaster for an event already stamped at `currentVersion()`, but once a v3 exists the chain calls this same method again with `fromVersion == 2`.
- The chain hands every step for an event type to this one upcaster and refuses a second upcaster for the same type. A future v3 therefore adds an `if (fromVersion == 2)` block here (or turns the method into a `switch` on `fromVersion`); it does not get a class of its own.

### Step 4 — Run the test

```bash
./gradlew :commands:test --tests "*.ProductCreatedUpcasterTest"
```

All five tests pass. Green phase.

The upcaster is a pure function with no Spring context, no database, and no mocks needed — just a map in, a map out. This is intentional: upcasters run on every event store read and must be as fast as a map lookup.

### Step 5 — Register the upcaster

Open `spring-app/src/main/java/org/streamrune/ecommerce/spring/config/StreamRuneConfig.java` and add a `productCreatedUpcaster` bean. StreamRune's Spring auto-configuration discovers every `EventUpcaster` bean via `ObjectProvider` and wraps them in an `UpcasterChain` when it builds the `EventStore` — no changes to any other bean are needed:

```java
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.ecommerce.commands.upcaster.ProductCreatedUpcaster;

/**
 * Exposes the product-created upcaster so the auto-configured event store
 * can collect it via ObjectProvider<EventUpcaster>.
 */
@Bean
public EventUpcaster productCreatedUpcaster() {
    return new ProductCreatedUpcaster();
}
```

That is the complete change for this chapter. The auto-configured event store already holds the `EventTypeRegistry` from chapter 2; now it also discovers this upcaster bean and includes it in the chain. No `PostgresEventStore.create(...)` call to modify.

> The `CryptoEngine` is still absent at this point — chapter 6 adds the `cryptoEngine` bean, which the auto-config also discovers and wires in automatically. No further structural changes to `StreamRuneConfig` are needed to enable encryption.

### Step 6 — Verify end-to-end

Start the application:

```bash
./gradlew :spring-app:bootRun
```

Insert a product *without* a category field by writing a raw v1 event directly into PostgreSQL — a row with no `category` key in its JSON payload:

```bash
docker exec -i streamrune-pg psql -U postgres -d streamrune_ecommerce <<'SQL'
WITH reserved AS (
  UPDATE global_offset_sequence SET next_value = next_value + 1 WHERE id = 1
  RETURNING next_value AS global_offset
)
INSERT INTO event_stream
  (global_offset, aggregate_type, aggregate_id, version, event_type, payload, metadata, schema_version)
SELECT
  reserved.global_offset,
  'product',
  'p-legacy',
  1,
  'ProductCreated',
  '{"productId":"p-legacy","name":"Old Widget","description":"Pre-category product","price":{"amount":9.99,"currency":"USD"},"stock":20}'::jsonb,
  jsonb_build_object(
    'eventId',       gen_random_uuid()::text,
    'commandId',     'tutorial-legacy-insert',
    'correlationId', 'tutorial-legacy-insert',
    'timestamp',     extract(epoch FROM now()),
    'baggage',       '{}'::jsonb),
  1
FROM reserved;
SQL
```

The `SELECT` lists one value per column, in the order of the column list. A few notes on the columns:

- `global_offset` is the row's position in the global stream. It has no default: the framework's append reserves it from the single-row `global_offset_sequence` table inside the same transaction, which keeps offsets gapless and in commit order. The `WITH reserved AS (UPDATE … RETURNING next_value)` CTE does exactly the same, so the hand-written row gets the next offset and the application's own appends carry on after it without a collision.
- `aggregate_type` is the name the product decider was registered under (`ProductState.TYPE`, `product`) and `aggregate_id` is the id — together they are the stream; there is no composed stream-id column, and a `CHECK` refuses a type outside `[a-z][a-z0-9_]{0,31}`.
- `version` is the per-stream sequence number (NOT NULL, no default): `1` for the first event of the `p-legacy` stream.
- `payload` is the v1 JSON — no `category` key.
- `metadata` is the event's causal metadata, which the store reads back on every load (see below).
- `schema_version` defaults to `1`; we set it explicitly to make it clear this is a v1 row — exactly the rows the upcaster targets.

**The metadata must be complete.** The store binds the `metadata` column to the framework's `EventMetadata` record each time it reads the row. Required are `eventId`, `commandId` and `correlationId` (non-blank strings) and `timestamp` (epoch seconds, the form the framework itself writes, or an ISO-8601 instant such as `"2026-01-01T00:00:00Z"`). `traceId`, `spanId`, `causationId`, `userId` and `baggage` are optional. A key `EventMetadata` does not define is refused as well.

> **An incomplete object poisons the stream.** The `INSERT` accepts any JSON — say `'{}'` — but the row then cannot be read: every command on `p-legacy` fails with a server error, and so does every retry. Because the row is also part of the global stream, every projection and saga subscription that reads past its offset fails on it too, from chapter 5 on. Nothing in the application repairs such a row, so get the metadata right before you insert it.

> If you started Postgres with `docker compose up` instead of the named `docker run` from Chapter 1, address it by service name: replace the first line with `docker compose exec -T postgres psql -U postgres -d streamrune_ecommerce <<'SQL'` and keep the rest.

Now send a command that forces the aggregate to be replayed from the event store:

```bash
curl -X PUT http://localhost:8080/api/products/p-legacy/price \
  -H 'Content-Type: application/json' \
  -d '{"price": 12.99}'
```

The `CommandBus` loads the `p-legacy` stream, finds the v1 `ProductCreated` row, passes it through the `UpcasterChain`, and the `ProductCreatedUpcaster` inserts `"category": "Uncategorized"` before the JSON is deserialised into a `ProductEvent.ProductCreated` record. The `evolve` function receives a fully-formed record with a non-null `category`, and state is rebuilt correctly.

If you query the product in a later chapter (once the read-side projection is wired up), it will show `category: "Uncategorized"` — populated by the upcaster, never written back to disk.

---

## What We Learned

- **Event upcasting** transforms raw event payloads at read time, allowing the Java domain model to evolve without modifying stored events.
- **`EventUpcaster`** is the interface with three methods: `eventType()`, `currentVersion()`, and `upcast(Map, int)`. The implementation is a pure function — fast, stateless, and trivially unit-testable.
- **`UpcasterChain`** is the internal component that the event store wraps around your upcaster list. When a stored event has a `schema_version` lower than `currentVersion()`, the chain calls `upcast` before deserialisation. Events already at the current version are passed through untouched. Event types with no registered upcaster default to `currentVersion()` of 1, so all the other event types (orders, customers, payments, inventory) have their payloads passed through unchanged.
- **`schema_version` records the upcaster in effect when the event was written, not the payload's shape.** An event written before its upcaster was registered is stamped 1 even if it already has the new field, so every step must leave an already-upgraded payload unchanged — `putIfAbsent`, not `put`.
- **Hand-written events need complete metadata.** The store reads the `metadata` column back into `EventMetadata` on every load; a row whose metadata lacks a required field fails every reader of its stream and of the global stream.
- **`EventTypeRegistry`** (specifically `SimpleEventTypeRegistry`) provides the mapping from wire type name (e.g. `"ProductCreated"`) to Java class. The registry and the upcaster chain work together: the chain transforms the raw map, then the registry deserialises the result into the correct record.
- **Registration** is done by declaring an `EventUpcaster` bean. The auto-configured `postgresEventStoreFactory` collects every such bean via `ObjectProvider<EventUpcaster>` and wraps them in an `UpcasterChain`. Adding a new upcaster is one bean declaration. Keep it registered for as long as the store holds events of its type: upcasting never rewrites stored rows, and a store whose chain no longer reaches a row's stamped `schema_version` refuses to read that row.
- **Schema evolution strategy**: one upcaster class per event type owns every step; use `fromVersion == N` guards (or a `switch`) so it applies multiple sequential migrations.

---

## Next Up

So far we can only write data. Next, we will build the read side with Projections and QueryBus.
