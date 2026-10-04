# Chapter 3 — Bean Validation

> **What you'll learn:**
> - What command interceptors are and how they compose around `decide()`
> - How to annotate command fields with Jakarta Validation constraints
> - How to wire `BeanValidationInterceptor` into the `VirtualThreadCommandBus`
> - How `ValidationException` propagates to a 400 response automatically
> - How to verify the interceptor with a curl call before any domain logic runs

---

## What We're Building and Why

In chapter 2 we built the `Product` aggregate and verified that a `POST /api/products` call stores a `ProductCreated` event in PostgreSQL. The controller's `CreateProductRequest` DTO already carries some `@Valid` constraints — `@NotBlank` on the product ID and name, `@NotNull @Positive` on the price — so Spring rejects those at the web layer before a command is even constructed. But that DTO is only one entry point, and it does not cover every field: `initialStock`, for example, is left unconstrained on the DTO, so a negative stock value sails straight through to the command. Any command issued programmatically (a test, a batch importer, a future message consumer) bypasses the DTO layer entirely and reaches the `CommandBus` with no structural checks at all.

We want a single guarantee: a command that reaches the `Decider` is structurally well-formed, no matter how it entered the system. That separation is intentional; business invariants belong in `decide()`, but structural constraints ("name must not be blank", "stock must not be negative") belong earlier and should hold regardless of entry point.

StreamRune solves this with **command interceptors** — middleware that wraps the `CommandBus` execution pipeline. Before the bus calls `decide(command, state)`, it calls `before(ctx)` on every registered interceptor in order. If any interceptor throws, the command never reaches the `Decider`.

`BeanValidationInterceptor` is the built-in interceptor for Jakarta Bean Validation (JSR-380). It reads the constraint annotations on the command record's fields, runs the validator, and throws a `ValidationException` — which extends `DomainException` (itself a `RuntimeException`) — carrying a structured list of every violation. The `GlobalExceptionHandler` you created in Chapter 1 (Step 7a) maps `DomainException` to a 400 response via its `handleDomainException` handler, so the `ValidationException` thrown by the interceptor is caught there and nothing else needs to change in the web layer.

The interceptor chain looks like this:

```
HTTP Request
    └─> ProductCommandController
            └─> CommandBus.execute(command)
                    ├─> BeanValidationInterceptor.before()  ← throws here on bad input
                    └─> ProductDecider.decide(cmd, state)    ← only reached on valid input
```

---

## Step by Step

### Step 1 — Add validation annotations to ProductCommand

Open `domain/src/main/java/org/streamrune/ecommerce/domain/product/ProductCommand.java` and add the Jakarta Validation imports and field annotations:

```java
package org.streamrune.ecommerce.domain.product;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import org.streamrune.core.Command;
import org.streamrune.ecommerce.domain.common.Money;

public sealed interface ProductCommand extends Command {
    record CreateProduct(
        @NotBlank String productId,
        @NotBlank String name,
        String description,
        String category,
        @NotNull Money price,
        @PositiveOrZero int initialStock)
        implements ProductCommand {}

    record UpdatePrice(@NotBlank String productId, @NotNull Money newPrice)
        implements ProductCommand {}

    record AdjustStock(@NotBlank String productId, int quantityChange, String reason)
        implements ProductCommand {}

    record DiscontinueProduct(@NotBlank String productId) implements ProductCommand {}
}
```

A few decisions worth noting:

- `description` and `category` are deliberately left unconstrained. They are optional fields and the domain accepts a product with no description.
- `@PositiveOrZero` on `initialStock` means zero is allowed — a product can be created with no initial inventory (it would start as `OUT_OF_STOCK`). A strictly positive constraint would prevent that.
- `@NotNull` on `price` prevents a null `Money` object. On the HTTP path the controller always builds `new Money(req.price(), "USD")` from a DTO-validated `BigDecimal`, so the command's price is never null there; this constraint is a defense-in-depth net for callers that construct the command directly. The `Money` record's own fields (`amount`, `currency`) could be validated further with `@Valid`, but for now a non-null price is sufficient.

The `domain` module already has `jakarta.validation-api` on its classpath as an `implementation` dependency (added in Chapter 1, Step 3). Using `implementation` rather than `compileOnly` ensures the annotation types are available at runtime so `BeanValidationInterceptor` can read them reflectively. The validator implementation (`hibernate-validator`) lives in `spring-app` via `spring-boot-starter-validation`.

### Step 2 — Create the BeanValidationInterceptor bean

Open `spring-app/src/main/java/org/streamrune/ecommerce/spring/config/StreamRuneConfig.java`. Add the following import and bean definition:

```java
import org.streamrune.runtime.BeanValidationInterceptor;
```

Then add the bean method alongside the existing `eventTypeRegistry` and `commandBus` beans:

```java
@Bean
public BeanValidationInterceptor validationInterceptor() {
    var factory = jakarta.validation.Validation.buildDefaultValidatorFactory();
    return new BeanValidationInterceptor(factory.getValidator());
}
```

`Validation.buildDefaultValidatorFactory()` is the standard JSR-380 bootstrap. It discovers the Hibernate Validator implementation (which is on the classpath via the Spring Boot starter) and builds a `ValidatorFactory`. We build the factory once at startup and inject the resulting `Validator`; the interceptor holds it as a `final` field, so there is no per-command factory allocation.

### Step 3 — Wire the interceptor into the CommandBus

Update the `commandBus` bean to accept the interceptor as a parameter and register it in the builder chain:

```java
@Bean
public VirtualThreadCommandBus commandBus(
        EventStore store,
        BeanValidationInterceptor validation) {
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
        .build();
}
```

The `.interceptors(...)` call takes a varargs list. Order matters: the bus calls `before()` left to right and `after()`/`onError()` right to left, and only on the interceptors whose `before()` completed. With only one interceptor for now the order is trivial. Later chapters put the audit and authorization interceptors in front of validation and a circuit breaker behind it, in the framework's canonical order (`CommandInterceptorOrdering` in `streamrune-runtime`): authorization runs before validation, so a caller who may not send a command learns nothing about its validation rules, and audit runs before authorization, so a refused command is still recorded.

### Step 4 — Verify: valid command still works

Start the application and confirm the happy path still passes through:

```bash
./gradlew :spring-app:bootRun
```

```bash
curl -X POST http://localhost:8080/api/products \
  -H 'Content-Type: application/json' \
  -d '{"productId":"p-1","name":"Widget","description":"A fine widget","category":"Gadgets","price":29.99,"initialStock":100}'
```

Expected: `200 OK` with empty body. The interceptor validated the command, found no violations, and execution proceeded to the `Decider` as before.

### Step 5 — Verify: the interceptor rejects an invalid command

The clearest way to see `BeanValidationInterceptor` fire is to send a violation that the controller's DTO does *not* also catch. Recall that `CreateProductRequest` leaves `initialStock` unconstrained — but the command's `@PositiveOrZero int initialStock` does not. So a negative stock value passes the DTO `@Valid` check, gets mapped into a `CreateProduct` command, and is then rejected by the interceptor on the command bus.

Send a request with a negative initial stock (and an otherwise-valid body so it clears the DTO layer):

```bash
curl -i -X POST http://localhost:8080/api/products \
  -H 'Content-Type: application/json' \
  -d '{"productId":"p-2","name":"Widget","price":5.00,"initialStock":-1}'
```

Expected response:

```
HTTP/1.1 400 Bad Request
Content-Type: text/plain;charset=UTF-8

Command validation failed for CreateProduct
```

The `BeanValidationInterceptor` caught the `@PositiveOrZero` violation on `initialStock`, threw a `ValidationException`, and the `GlobalExceptionHandler`'s `handleDomainException` handler mapped it to a 400. The `Decider` was never called — no event reached the store.

### Step 5a — Two layers of validation

The two negative cases below are *also* rejected with a 400, but by a different layer — Spring's `@Valid` check on the controller DTO, which runs *before* a command is ever constructed. Send a command with a blank name:

```bash
curl -i -X POST http://localhost:8080/api/products \
  -H 'Content-Type: application/json' \
  -d '{"productId":"p-3","name":"","price":9.99,"initialStock":10}'
```

```bash
curl -i -X POST http://localhost:8080/api/products \
  -H 'Content-Type: application/json' \
  -d '{"productId":"p-4","name":"Gadget","initialStock":5}'
```

Both return `400 Bad Request`, but with Spring's default validation error body (`Content-Type: application/problem+json`, an RFC 7807 problem document listing the field errors) — *not* the plain-text `Command validation failed for CreateProduct` message. That is because `CreateProductRequest` carries `@NotBlank String name` and `@NotNull @Positive BigDecimal price`, so Spring rejects the blank name and the missing price at the web layer, raising a `MethodArgumentNotValidException` before `commandBus.execute(...)` is ever called.

This is defense in depth, not redundancy:

- The **DTO `@Valid` layer** guards the HTTP entry point and gives clients a detailed field-by-field error document.
- The **command-bus interceptor** is the safety net that validates the command itself — it fires for fields the DTO leaves open (like `initialStock`) and for any command constructed in code that bypasses the controller entirely (tests, importers, message consumers). It guarantees that whatever reaches the `Decider` is structurally well-formed.

---

## What We Learned

- **Command interceptors** implement `CommandInterceptor` and are called by the `CommandBus` before and after `decide()`. They are the right place for cross-cutting concerns that should not pollute domain logic.
- **`BeanValidationInterceptor`** reads JSR-380 annotations on command fields, runs the Jakarta Validator, and throws a **`ValidationException`** — which extends `DomainException` (a `RuntimeException`) — carrying a list of `ValidationError` records (`field`, `message`, `code`) before the command reaches the `Decider`.
- **Interceptor chain ordering**: `before()` fires left-to-right, `after()` and `onError()` fire right-to-left (like servlet filters). Validation is not first: from Chapter 9 on it sits behind authorization, so a caller without the required role is refused before validation runs, and from Chapter 10 on behind audit, so that refusal is recorded.
- **Two validation layers**: the controller's DTO `@Valid` check rejects malformed HTTP bodies at the web layer (a Spring `MethodArgumentNotValidException` → 400 problem+json), while the command-bus interceptor is the defense-in-depth net that validates the command regardless of entry point and catches fields the DTO leaves open (like `initialStock`).
- **`GlobalExceptionHandler`** maps `DomainException` (and therefore `ValidationException`) to `400 Bad Request` via its `handleDomainException` handler. No changes to the web layer were needed.
- The **domain module uses `implementation` for `jakarta.validation-api`** (not `compileOnly`) so that annotation types are present at runtime for the interceptor to read reflectively. The actual validator implementation (`hibernate-validator`) stays in `spring-app` — the domain jar never pulls in the validator engine.

---

## Next Up

What happens when we need to change the shape of an event that's already stored? Next: event upcasting.
