# Chapter 9 — Authorization

> **What you'll learn:**
> - How to annotate command records with `@RequireRole` to declare access requirements at the domain boundary
> - How `AnnotationAuthorizationInterceptor` enforces those annotations before a command reaches the decider
> - How `UserRoleResolver` bridges the gap between request context and authorization decisions
> - How `ScopedValueFilter` uses Java's `ScopedValue` (a preview API in Java 21–24, finalized in Java 25) to propagate request identity through a virtual-thread call stack
> - How to test authorization in practice: a `CUSTOMER` cannot ship an order; an `ADMIN` can
> - How to guard the GDPR routes from Chapter 7 so only an `ADMIN` or the customer themself can erase a customer
> - Why the demo's identity and role headers are a stand-in for a gateway, and why its header-based role resolver must not be copied into a real deployment

---

## What We're Building and Why

Our order lifecycle enforces *state machine* rules — you cannot ship an order that was never confirmed. But so far there is no rule about *who* can send each command. Any caller can ship an order, discontinue a product, or deliver goods. That is not a realistic e-commerce system.

The pattern StreamRune uses for authorization mirrors how it handles validation: you declare the requirement on the command record as an annotation, and a `CommandInterceptor` checks it in `before()` before the command ever reaches the decider. This keeps authorization out of the domain layer. The decider has no security imports. The `@RequireRole` annotation sits on the command class in the `domain` module, which is exactly where you want it — it is part of the contract for that command.

The interceptor reads the caller's role from the request context. Rather than thread-locals (which break virtual threads) or Spring's `SecurityContextHolder` (which couples you to Spring), StreamRune propagates context via `ScopedValue` (a preview API in Java 21–24, finalized in Java 25 — the version this project targets). A servlet filter wraps each request in a `ScopedValue.where(...)` call, binding a `StreamRuneContext` that carries the caller's identity. Any code that runs in that scope — including virtual-thread command dispatches — can read the context without explicit parameter passing.

---

## Step by Step

### Step 1 — Annotate the restricted commands

Open `domain/src/main/java/org/streamrune/ecommerce/domain/order/OrderCommand.java` and add `@RequireRole("ADMIN")` to `ShipOrder` and `DeliverOrder`:

```java
package org.streamrune.ecommerce.domain.order;

import java.util.List;
import org.streamrune.core.Command;
import org.streamrune.core.RequireRole;
import org.streamrune.core.types.IdConstraints;
import org.streamrune.ecommerce.domain.common.Money;

public sealed interface OrderCommand extends Command {
  /**
   * Places an order. The constructor checks the order id's length, so every app that builds a
   * {@code PlaceOrder} applies the same rule: a client-supplied order id must also become a valid
   * saga id. The order stream takes any id {@code AggregateId.of} accepts, up to 255 characters,
   * but the fulfillment saga that {@code OrderPlaced} starts is identified by {@code "fulfillment-"
   * + orderId}, and a {@code SagaId} holds 255 characters at most. Refused only there, the order
   * would already be placed and the saga could never start: its first event would be quarantined as
   * poison and the order would stay {@code CREATED} for good. Refused here, the order endpoint
   * answers {@code 400} and nothing is written. The message never echoes the id.
   *
   * <p>A blank order id or one with a control character is refused where the command bus builds the
   * stream id with {@code AggregateId.of}, before anything is written, so the constructor adds only
   * the length rule. The payment id {@code "pay-" + orderId} is shorter by eight characters than
   * the saga id and fits whenever that does.
   */
  record PlaceOrder(String orderId, String customerId, List<OrderLine> lines)
      implements OrderCommand {

    /** The prefix of the saga id the order starts: {@code "fulfillment-" + orderId}. */
    public static final String SAGA_ID_PREFIX = "fulfillment-";

    /** The longest order id whose saga id {@code "fulfillment-" + orderId} fits 255 characters. */
    public static final int MAX_ORDER_ID_LENGTH =
        IdConstraints.MAX_LENGTH - SAGA_ID_PREFIX.length();

    public PlaceOrder {
      if (orderId != null && orderId.length() > MAX_ORDER_ID_LENGTH) {
        throw new IllegalArgumentException(
            "orderId must be at most "
                + MAX_ORDER_ID_LENGTH
                + " characters, got "
                + orderId.length());
      }
    }
  }

  record ConfirmOrder(String orderId) implements OrderCommand {}

  @RequireRole("ADMIN")
  record ShipOrder(String orderId) implements OrderCommand {}

  @RequireRole("ADMIN")
  record DeliverOrder(String orderId) implements OrderCommand {}

  record CancelOrder(String orderId, String reason) implements OrderCommand {}

  /**
   * One line of an order to place. The constructor checks the product id, so every app that builds
   * a {@code PlaceOrder} applies the same rule: a client-supplied product id must become a valid
   * inventory id. The saga reserves stock on the inventory aggregate whose id is the product id,
   * and the inventory extractor builds that id with {@code AggregateId.of}, which refuses a control
   * character or an id longer than 255 characters. Refused only there, the order would already be
   * placed and paid for, and the saga would refund and cancel it. Refused here, the order endpoint
   * answers {@code 400} and nothing is written. The message never echoes the id.
   *
   * <p>{@link OrderEvent.OrderLine}, the line an {@code OrderPlaced} event records, has no such
   * rule: it is rebuilt from the event log and from saga state, where a rule would make a stored
   * order unreadable instead of stopping a write.
   */
  record OrderLine(String productId, int quantity, Money unitPrice) {

    /** The longest product id: it is the inventory aggregate id, so the id bound applies as is. */
    public static final int MAX_PRODUCT_ID_LENGTH = IdConstraints.MAX_LENGTH;

    public OrderLine {
      if (productId == null || productId.isBlank()) {
        throw new IllegalArgumentException("productId is required");
      }
      if (productId.length() > MAX_PRODUCT_ID_LENGTH) {
        throw new IllegalArgumentException(
            "productId must be at most "
                + MAX_PRODUCT_ID_LENGTH
                + " characters, got "
                + productId.length());
      }
      IdConstraints.requireNoControlCharacters(productId, "productId");
    }
  }
}
```

Open `domain/src/main/java/org/streamrune/ecommerce/domain/product/ProductCommand.java` and add `@RequireRole("ADMIN")` to `DiscontinueProduct`:

```java
package org.streamrune.ecommerce.domain.product;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import org.streamrune.core.Command;
import org.streamrune.core.RequireRole;
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

  @RequireRole("ADMIN")
  record DiscontinueProduct(@NotBlank String productId) implements ProductCommand {}
}
```

The annotation lives in the `domain` module where the command is defined. It carries no Spring or runtime import — just `org.streamrune.core.RequireRole`. The `@RequireRole` annotation has `RetentionPolicy.RUNTIME`, so the interceptor can read it via reflection.

> **The Spring app does not start again until Step 5.** A `VirtualThreadCommandBus` refuses to build when a command it registers carries `@RequireRole` (or `@RequirePermission`) and no `AnnotationAuthorizationInterceptor` is in its own interceptor chain, so an annotation can never be silently ignored. Until Step 5 adds the interceptor to the bus, startup fails with an `IllegalStateException` saying the annotated commands "would execute UNGUARDED" and naming them. Work through Steps 2–5 before you restart the application.

### Step 2 — Implement HeaderUserRoleResolver

The interceptor needs to know the caller's roles. It delegates to a `UserRoleResolver` — a single-method interface that takes a `UserId` and returns a `UserAuthority`. You provide the implementation; StreamRune calls it.

Create `spring-app/src/main/java/org/streamrune/ecommerce/spring/config/HeaderUserRoleResolver.java`:

```java
package org.streamrune.ecommerce.spring.config;

import java.util.Set;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.UserId;

/**
 * DEMO SHORTCUT, do not copy into a real deployment: the caller's role comes from the
 * client-supplied X-User-Role header (see "Why the role header is a demo shortcut" below).
 */
public class HeaderUserRoleResolver implements UserRoleResolver {

  @Override
  public UserAuthority resolve(UserId userId) {
    String role = resolveCurrentRole();
    return new UserAuthority(Set.of(role), permissionsForRole(role));
  }

  @Override
  public boolean requiresRequestContext() {
    return true; // the answer comes from the request, not from the userId (see below)
  }

  private static String resolveCurrentRole() {
    if (org.streamrune.core.StreamRuneContext.CURRENT.isBound()) {
      var ctx = org.streamrune.core.StreamRuneContext.CURRENT.get();
      if (ctx != null) {
        return ctx.baggage().getOrDefault("role", "GUEST");
      }
    }
    return "GUEST";
  }

  private static Set<String> permissionsForRole(String role) {
    return switch (role) {
      case "ADMIN" ->
          Set.of(
              "ORDER_CANCEL_CONFIRMED",
              "ORDER_SHIP",
              "ORDER_DELIVER",
              "PRODUCT_MANAGE",
              "CUSTOMER_MANAGE",
              "AUDIT_VIEW",
              "ADMIN_PANEL");
      case "CUSTOMER" -> Set.of("ORDER_PLACE", "ORDER_CANCEL_OWN", "PROFILE_MANAGE");
      default -> Set.of();
    };
  }
}
```

`resolveCurrentRole()` reads `StreamRuneContext.CURRENT`, which is a `ScopedValue`. If the current thread is inside a scope where `CURRENT` is bound, it returns the `role` entry from the context's baggage map. If no scope is active (for example, in a unit test that calls the resolver directly), it falls back to `"GUEST"`. The role-to-permissions mapping is kept simple and in-process for this tutorial; a production system would query a database or an identity provider.

> **Why the role header is a demo shortcut — do not copy this resolver.** The `role` baggage entry is copied from the `X-User-Role` request header (Step 3), so the role is whatever the client says:
>
> - **Anyone can claim any role.** Sending `X-User-Role: ADMIN` makes the caller an admin; nothing checks the value. You will do exactly that with `curl` in Step 6.
> - **Only the trusted-gateway mode lets it through — and that mode trusts a gateway the demo does not have.** The filter copies `X-User-Role` into the baggage only when `streamrune.security.trust-user-id-header` (Step 3) is on, the same switch that makes `X-User-Id` the identity: it declares that a gateway in front sets or strips both headers. The demo has no such gateway; your `curl` plays it. In any other mode the framework ignores the header (this resolver would then answer `GUEST` for everyone), and a W3C `baggage: role=ADMIN` header never reaches the entry in any mode.
> - **The role is not tied to the identity.** `resolve(UserId userId)` ignores its argument: it trusts that whoever set `X-User-Role` set it for the same caller as `X-User-Id`.
>
> The demo accepts this only because it has no login: the frontend's role switcher (Chapter 16), the `curl` examples and the integration tests pick a role by header. A real deployment derives the authority from the authenticated identity instead. With Spring Security, drop the `HeaderUserRoleResolver` bean (and let `authInterceptor` take a `UserRoleResolver`) and the framework registers `SpringSecurityUserRoleResolver`, which reads the authenticated principal's granted authorities (Quarkus and Micronaut ship `QuarkusSecurityUserRoleResolver` and `MicronautSecurityUserRoleResolver`). Without a security framework, implement `resolve(userId)` as a lookup keyed by the user id it receives — your user database or identity provider — never by reading a request header.

> **Why `requiresRequestContext()` returns `true`.** `UserRoleResolver` has a second method, `false` by default, with which a resolver declares that its answer comes from the request rather than from the `userId` it is given. `HeaderUserRoleResolver` is such a resolver: it reads the `role` baggage entry of the request in progress. The declaration has two effects.
>
> - **The role is captured at the request edge.** The framework's request filter calls `resolve()` once per request that carries a user, with `StreamRuneContext.CURRENT` already bound to that request's context, and stores the answer in the context. `AnnotationAuthorizationInterceptor` decides every guarded command of the request against that captured answer, including one that continues on another thread (`executeAsync`).
> - **A dead-letter replay does not ask the resolver.** A replay (Chapter 14) has no request at all, so the resolver could only answer `"GUEST"` there and the replayed command would be refused on every attempt until its retries ran out. The interceptor instead runs the command, which passed this very check before it was dead-lettered, under the dead-letter-replay system principal, as the user the entry recorded, and logs a `WARN` naming the command. An entry with no recorded user is still refused (`Authentication required`).
>
> A resolver that is a pure function of the `userId`, such as a lookup in your user database, keeps the `false` default: the framework then asks it on every path, replays included, so a role revoked in the meantime still stops a queued command.

> **A note on `spring-security-core`.** The demo declares `spring-security-core` on its classpath. When Spring Security's `SecurityContextHolder` is present and no `UserRoleResolver` bean is defined, `StreamRuneAutoConfiguration` registers a default `SpringSecurityUserRoleResolver` (it reads Spring Security's context rather than the baggage header). Because we register our own `HeaderUserRoleResolver` as a `UserRoleResolver` bean in Step 4, that default is suppressed (the framework's default is `@ConditionalOnMissingBean(UserRoleResolver.class)`), so our header-based resolver is the one in effect.

### Step 3 — The `ScopedValueFilter` is auto-registered by the framework

You do **not** need to create a custom filter. The `streamrune-spring` auto-configuration (`StreamRuneAutoConfiguration`) automatically registers a `ScopedValueFilter` bean that:

1. Reads the `X-User-Id`, `X-User-Role`, and `X-Correlation-Id` HTTP headers (`X-User-Id` becomes the user only in the trusted-gateway mode — see below).
2. Constructs a `StreamRuneContext.RequestContext` and binds it via `ScopedValue.where(StreamRuneContext.CURRENT, ctx).call(...)`.
3. Puts `X-User-Role` into the context's baggage map under the key `"role"` — only in the trusted-gateway mode below, the same flag that makes `X-User-Id` the identity. In any other mode the header is ignored and there is no `"role"` entry.

Your `HeaderUserRoleResolver` (Step 2) reads exactly this `"role"` baggage entry via `StreamRuneContext.CURRENT.get().baggage().getOrDefault("role", "GUEST")`. The whole chain works without any filter code in your project.

> **`X-User-Id` is required for any annotated command.** Before checking roles, `AnnotationAuthorizationInterceptor` calls `Authorization.currentUserId()` (which returns `ctx.userId()`, populated from the `X-User-Id` header once you enable the trusted-gateway mode below). If it is null — because the request omitted `X-User-Id` — the interceptor throws `AuthorizationException("Authentication required")` rather than the role message. Like every other `AuthorizationException`, this surfaces as `400 Bad Request`. So a request to a guarded command always needs `X-User-Id` set; `X-User-Role` then decides whether the caller is authorized.

**Trust `X-User-Id` explicitly.** Out of the box the framework never believes a client-supplied `X-User-Id`: the user is the authenticated Spring Security principal when a resolver is available, and otherwise every request is anonymous. The header becomes the identity — and `X-User-Role` the `"role"` baggage entry — only in the *trusted-gateway* mode, which you switch on in `spring-app/src/main/resources/application.yml`:

```yaml
streamrune:
  security:
    # Trusted-gateway mode, not an authenticated resolver: the demo has no login and stands in for a
    # gateway that would authenticate every caller, overwrite X-User-Id with the authenticated id and
    # strip any X-User-Id or X-User-Role a client sent. Never enable this in production unless such a
    # gateway is in front. The flag covers X-User-Role too: it becomes baggage `role` only in this
    # mode, and the HeaderUserRoleResolver shortcut (Step 2) authorizes on it.
    trust-user-id-header: true
```

Without it, the demo would not authorize anyone. `spring-security-core` is on the classpath (Chapter 1), so the framework would take the user from Spring Security — and with no login every request is unauthenticated, so every guarded command fails with `Authentication required`. Remove Spring Security as well and the application refuses to start instead: annotation authorization is configured (the `UserRoleResolver` bean in Step 4) but requests would have no identity source, and the startup error names both remedies. Trusting the header is right for a showcase without an authentication layer; a production service either uses Spring Security and leaves the flag off, or sits behind a gateway that authenticates every caller, overwrites `X-User-Id` with the authenticated id and strips any value a client sent. The framework logs which mode it runs in once at startup (`Request identity: TRUSTED_GATEWAY …` for the demo). Because Spring Security is on the classpath, that line also names the Spring Security resolver the header overrides: with the flag on, the resolver is never asked. Send `X-User-Id` once per request — a request that carries it twice is anonymous in this mode, at the command filter and the SSE endpoint alike, because the framework will not guess which value a gateway meant.

**Why the flag and not an authenticated resolver?** Wiring a real resolver means a login: Spring Security's filter chain, user accounts, and a sign-in flow in the frontend and in every integration test. That is not what this demo teaches, so all three apps make the same explicit choice — the trusted-gateway mode — and say so in their configuration. In this setup the frontend, your `curl` commands and the integration tests play the part of the gateway: they set `X-User-Id` themselves. That is acceptable on your laptop and nowhere else.

> **The Quarkus app needs the same flag.** The Quarkus integration follows the same rule: its `StreamRuneRequestFilter` and SSE endpoint ignore `X-User-Id` (and the filter `X-User-Role`) unless the trusted-gateway mode is on, and with no `quarkus-security` module a Quarkus REST application whose command bus carries annotation authorization refuses to start. The demo's `quarkus-app` therefore sets `streamrune.security.trust-user-id-header=true` in `quarkus-app/src/main/resources/application.properties`, and its startup log shows the same `Request identity: TRUSTED_GATEWAY …` line.

> **So does the Micronaut app.** The Micronaut integration follows the same rule too: its `StreamRuneContextFilter` and SSE endpoint ignore `X-User-Id` (and the filter `X-User-Role`) unless the trusted-gateway mode is on, and with no Micronaut Security (or with `micronaut.security.enabled=false`) an application whose command bus carries annotation authorization refuses to start. The demo's `micronaut-app` sets `streamrune.security.trust-user-id-header: true` in `micronaut-app/src/main/resources/application.yml` and logs the same `Request identity: TRUSTED_GATEWAY …` line at startup. The same file sets `micronaut.server.netty.server-type: full_content`: the filter binds the request context on the thread it runs the route on, and with Micronaut's default streamed server type a request body that arrives after the headers resumes the route on the Netty event loop instead, where the controller sees no user and no role (an ADMIN call answers `403`).

`ScopedValue` (finalized in Java 25; a preview API in Java 21–24) is safe to use with virtual threads: the value is visible to the current thread and any threads started within the scope's call block, and it is automatically unbound when the scope exits. This is why it replaced `ThreadLocal` in StreamRune's context propagation.

### Step 4 — Create the AnnotationAuthorizationInterceptor bean

Open `spring-app/src/main/java/org/streamrune/ecommerce/spring/config/StreamRuneConfig.java` and add two beans: the `HeaderUserRoleResolver` and the `AnnotationAuthorizationInterceptor` that wraps it.

```java
@Bean
public HeaderUserRoleResolver userRoleResolver() {
  return new HeaderUserRoleResolver();
}

@Bean
public AnnotationAuthorizationInterceptor authInterceptor(HeaderUserRoleResolver resolver) {
  return new AnnotationAuthorizationInterceptor(resolver);
}
```

`AnnotationAuthorizationInterceptor` comes from `streamrune-runtime`. Its constructor takes a `UserRoleResolver`. In `before()`, it reads `@RequireRole` and `@RequirePermission` from the command class using reflection. If neither annotation is present, it is a no-op. If an annotation is present, it takes the caller's authority (for this resolver, the one the request filter captured at the request edge) and throws `AuthorizationException` if the requirement is not met. The exception is thrown before `decide` is called — no event is ever written.

Add the import:

```java
import org.streamrune.runtime.AnnotationAuthorizationInterceptor;
```

### Step 5 — Add the auth interceptor to the command bus chain

Still in `StreamRuneConfig.java`, wire the auth interceptor into the `VirtualThreadCommandBus` builder, placing it before validation:

```java
@Bean
public VirtualThreadCommandBus commandBus(
    EventStore store,
    BeanValidationInterceptor validation,
    AnnotationAuthorizationInterceptor auth) {
  // more interceptors added in later chapters
  return VirtualThreadCommandBus.builder()
      .eventStore(store)
      .interceptors(auth, validation)
      .snapshotPolicy(SnapshotPolicy.everyNEvents(5))
      // ... register deciders
      .build();
}
```

You do **not** need to register a filter bean. The framework's `ScopedValueFilter` is auto-registered by `StreamRuneAutoConfiguration` and runs for all requests.

The interceptor order inside `interceptors(...)` matters: the bus calls each `before()` in the listed order. Authorization runs first, so a caller who may not send a command is refused before Bean Validation looks at it, and the validation messages never tell that caller what a valid command looks like. The audit interceptor you add in the next chapter goes in front of both, and the circuit breaker from Chapter 12 behind them. This is the framework's canonical order (`CommandInterceptorOrdering` in `streamrune-runtime`), the one the Quarkus and Micronaut integrations give the chains they assemble themselves.

### Step 6 — Test: CUSTOMER cannot ship, ADMIN can

Start the application:

```bash
./gradlew :spring-app:bootRun
```

**Place an order as a customer and confirm it:**

```bash
curl -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -H 'X-User-Id: customer-1' \
  -H 'X-User-Role: CUSTOMER' \
  -d '{
    "orderId": "order-auth-test",
    "customerId": "customer-1",
    "lines": [{"productId": "p-1", "quantity": 1, "unitPrice": 19.99}]
  }'

curl -X POST http://localhost:8080/api/orders/order-auth-test/confirm \
  -H 'X-User-Id: customer-1' \
  -H 'X-User-Role: CUSTOMER'
```

Both succeed: `PlaceOrder` and `ConfirmOrder` have no `@RequireRole` annotation, so the interceptor is a no-op for those commands.

**Try to ship as a CUSTOMER — expect rejection:**

```bash
curl -v -X POST http://localhost:8080/api/orders/order-auth-test/ship \
  -H 'X-User-Id: customer-1' \
  -H 'X-User-Role: CUSTOMER'
```

The response is `400 Bad Request` with a body like:

```
Required role: ADMIN
```

The filter bound `role=CUSTOMER` in the `ScopedValue` (and `userId=customer-1` from `X-User-Id`), resolved the caller's authority via `HeaderUserRoleResolver` and stored it in the request context. The interceptor read `@RequireRole("ADMIN")` from `ShipOrder`, found no `ADMIN` role in that authority, and threw `AuthorizationException`. The command never reached `OrderDecider`. Nothing was written to the event store.

> **Why 400, not 403?** `AuthorizationException` extends `DomainException`, and the demo's `GlobalExceptionHandler` maps every `DomainException` to `400 Bad Request`. The demo does not register a dedicated handler for `AuthorizationException`, and it depends only on `spring-security-core` (not the full Spring Security filter chain), so nothing produces a `403`. If you want a `403` for authorization failures, add an `@ExceptionHandler(AuthorizationException.class)` to `GlobalExceptionHandler` that returns `HttpStatus.FORBIDDEN` — the body (`Required role: ADMIN`) stays the same.

**Ship as an ADMIN — expect success:**

```bash
curl -X POST http://localhost:8080/api/orders/order-auth-test/ship \
  -H 'X-User-Id: admin-1' \
  -H 'X-User-Role: ADMIN'
```

This returns `200 OK`. The filter bound `role=ADMIN`, the resolver returned `UserAuthority` containing `ADMIN`, and `authority.hasAnyRole("ADMIN")` returned `true`.

Notice what made you an admin: two headers you typed. The identity half is the trusted-gateway mode (Step 3) — in production a gateway sets `X-User-Id`, not the client. The role half is the demo shortcut from Step 2 — in production the role comes from the authenticated identity, never from a header.

**Verify the discontinue guard on products:**

Discontinuing a product is a `POST` to `/api/products/{id}/discontinue` (see `ProductCommandController`), and like the order commands it needs `X-User-Id` so the interceptor can resolve a caller:

```bash
# Attempt as CUSTOMER
curl -v -X POST http://localhost:8080/api/products/p-1/discontinue \
  -H 'X-User-Id: customer-1' \
  -H 'X-User-Role: CUSTOMER'
# → 400 Bad Request — "Required role: ADMIN"

# Attempt as ADMIN
curl -X POST http://localhost:8080/api/products/p-1/discontinue \
  -H 'X-User-Id: admin-1' \
  -H 'X-User-Role: ADMIN'
# → 200 OK
```

### Step 7 — Guard the GDPR routes: an ADMIN or the customer themself

Chapter 7 left `POST /api/customers/{id}/forget` and `POST /api/customers/{id}/export-data` open, because until now no request carried an identity. Now one does, and the forget route needs it most: it destroys the customer's encryption key, which cannot be undone. Who may call it depends on *which* customer the request names — an `ADMIN` may erase anyone, a customer only themself — and `@RequireRole` cannot express that: it looks at the command's type, never at the id inside it. So the check is a few lines in `CustomerCommandController`, run as the first line of both handlers, before anything is written:

```java
@PostMapping("/{id}/forget")
public ResponseEntity<ForgetResponse> forgetCustomer(@PathVariable String id) {
    requireAdminOrSelf(id);
    commandBus.execute(new CustomerCommand.ForgetCustomer(id));
    // ... the rest as in Chapter 7
}

@PostMapping("/{id}/export-data")
public ResponseEntity<Void> exportData(@PathVariable String id) {
    requireAdminOrSelf(id);
    commandBus.execute(new CustomerCommand.RequestDataExport(id));
    return ResponseEntity.ok().build();
}

/**
 * Answers 403 unless the caller holds the ADMIN role or is the customer themself (the request
 * identity equals the customer id).
 */
private static void requireAdminOrSelf(String customerId) {
    var ctx = StreamRuneContext.CURRENT.isBound() ? StreamRuneContext.CURRENT.get() : null;
    if (ctx != null) {
        if ("ADMIN".equals(ctx.baggage().get("role"))) {
            return;
        }
        if (ctx.userId() != null && ctx.userId().value().equals(customerId)) {
            return;
        }
    }
    throw new ResponseStatusException(
        HttpStatus.FORBIDDEN, "Only an ADMIN or the customer themself may do this");
}
```

It reads the same two things the interceptor does: the `role` baggage entry (the demo shortcut from Step 2) and the user id (the trusted-gateway mode from Step 3). The customer id is the user id in this demo — the frontend's customer persona sends its own customer id as `X-User-Id` — so "themself" is a plain comparison. Add the imports `org.springframework.http.HttpStatus` and `org.springframework.web.server.ResponseStatusException`.

Try it with Bob from Chapter 7 (register him again if your database was reset):

```bash
# No identity, or someone else's: 403, and nothing is written
curl -i -X POST http://localhost:8080/api/customers/c-bob/forget
curl -i -X POST http://localhost:8080/api/customers/c-bob/forget \
  -H 'X-User-Id: customer-1' -H 'X-User-Role: CUSTOMER'
# → 403 Forbidden

# Bob himself may ask for his export
curl -i -X POST http://localhost:8080/api/customers/c-bob/export-data \
  -H 'X-User-Id: c-bob' -H 'X-User-Role: CUSTOMER'
# → 200 OK

# An ADMIN may erase any customer
curl -s -X POST http://localhost:8080/api/customers/c-bob/forget \
  -H 'X-User-Id: admin-1' -H 'X-User-Role: ADMIN'
# → {"keyDeleted":true,"fullyErased":true,"failedPurgers":[]}
```

A refused request writes no event and leaves the key alone: the check runs before `ForgetCustomer` is dispatched. The Quarkus and Micronaut apps apply the same check in their `CustomerCommandController`s.

---

## What We Learned

- **`@RequireRole`** is a runtime-retained annotation you place on a command record to declare that the caller must hold at least one of the listed roles. When multiple roles are listed, any single match suffices (OR). The annotation lives in the `domain` module with no infrastructure imports.

- **`@RequirePermission`** works identically but checks fine-grained permissions rather than broad roles. You can combine both on one command: when both annotations are present, the caller must satisfy both (AND).

- **`UserRoleResolver`** is a `@FunctionalInterface` that maps a `UserId` to a `UserAuthority`. You implement it and register it as a bean; you own the data source. A resolver that reads the request, like the demo's, also overrides `requiresRequestContext()` to return `true`: the framework then calls it once at the request edge and never on a dead-letter replay, which runs an already-authorized command under the dead-letter-replay system principal instead.

- **`AnnotationAuthorizationInterceptor`** is a `CommandInterceptor` from `streamrune-runtime`. It reads both annotations from the command class via reflection, checks them against the caller's authority (the one captured at the request edge for a resolver that declares `requiresRequestContext()`, otherwise the answer of `UserRoleResolver.resolve()`), and throws `AuthorizationException` if the requirement is not met. If neither annotation is present it is a pure no-op with effectively zero overhead.

- **`ScopedValue`** (finalized in Java 25; preview in Java 21–24) replaces `ThreadLocal` for propagating request context through virtual threads. The framework's `ScopedValueFilter` (auto-registered by `StreamRuneAutoConfiguration`) wraps each request in `ScopedValue.where(StreamRuneContext.CURRENT, ctx).call(...)`, making the context available to all code that runs in that scope, including code dispatched onto virtual threads by the `CommandBus`. In the trusted-gateway mode the filter also reads `X-User-Role` and places it into the baggage map under the key `"role"`, which `HeaderUserRoleResolver` reads; in any other mode it ignores that header.

- **Identity and role in the demo are stand-ins.** The apps run in the trusted-gateway mode (`streamrune.security.trust-user-id-header: true`), so `X-User-Id` is the identity — acceptable only because the demo stands in for a gateway that would authenticate callers and set the header itself. `HeaderUserRoleResolver` goes further and authorizes on `X-User-Role`. The framework reads that header only in the trusted-gateway mode, where a real gateway would set it; in the demo nothing authenticates it: a demo shortcut a real deployment must replace with a resolver that derives the authority from the authenticated identity.

- **Ownership is checked where the id is known.** `@RequireRole` decides by the command's type; "an `ADMIN`, or the customer themself" depends on the id the request names, so the GDPR routes check it in the controller (`requireAdminOrSelf`) and answer `403` before anything is written.

- **Interceptor ordering** matters: authorization runs before validation, so an unauthorized caller cannot probe the validation rules. In the next chapter audit goes in front of authorization, so a refused command is still recorded.

---

## Next Up

We can control who does what. Next, let's add an audit trail.
