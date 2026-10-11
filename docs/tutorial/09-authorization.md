# Chapter 9 — Authorization

> **What you'll learn:**
> - How to annotate command records with `@RequireRole` to declare access requirements at the domain boundary
> - How `AnnotationAuthorizationInterceptor` enforces those annotations before a command reaches the decider
> - How `UserRoleResolver` bridges the gap between request context and authorization decisions
> - How `ScopedValueFilter` uses Java's `ScopedValue` (a preview API in Java 21–24, finalized in Java 25) to propagate request identity through a virtual-thread call stack
> - How to test authorization in practice: a `CUSTOMER` cannot ship an order; an `ADMIN` can
> - How to guard a customer's data — the customer and order routes, the GDPR routes from Chapter 7 — with one rule: an authenticated `ADMIN`, or the customer the data belongs to
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

### Step 7 — A customer's data: an ADMIN or the customer themself

Chapters 6 to 8 left every customer and order route open, because until now no request carried an identity. Now one does, and the routes need it: `GET /api/customers` returns every customer's name, e-mail, address and phone, `GET /api/orders` every customer's purchases, and `POST /api/customers/{id}/forget` destroys a customer's encryption key, which cannot be undone.

Who may call them depends on *which* customer the request names — an `ADMIN` may read or erase anyone, a customer only themself — and `@RequireRole` cannot express that: it looks at the command's type, never at the id inside it, and it guards commands, not reads. So the demo has one rule of its own, written once and asked everywhere a customer's data is handed out or changed:

> **An authenticated caller who holds the `ADMIN` role, or the customer the data belongs to. Everyone else is refused — a caller without an identity first of all.**

The rule is plain Java with no Spring in it, so it goes into the shared `queries` module, where `quarkus-app` and `micronaut-app` use the same class. A refusal is an exception of its own. Create `queries/src/main/java/org/streamrune/ecommerce/queries/access/AccessRefusedException.java`:

```java
package org.streamrune.ecommerce.queries.access;

/**
 * The caller may not read or change what the request names. Each application answers it with 403
 * Forbidden. The message never names the resource.
 */
public final class AccessRefusedException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public AccessRefusedException(String message) {
    super(message);
  }
}
```

and next to it `OwnerOrAdminAccess.java`:

```java
package org.streamrune.ecommerce.queries.access;

import org.streamrune.core.StreamRuneContext;
import org.streamrune.ecommerce.queries.dto.OrderView;

/**
 * The demo's one rule for a customer's data: an authenticated caller who holds the ADMIN role, or
 * the customer the data belongs to. Everyone else is refused.
 */
public final class OwnerOrAdminAccess {

  private static final String ROLE_BAGGAGE_KEY = "role";
  private static final String ADMIN = "ADMIN";
  private static final String REFUSED = "Only an ADMIN or the customer themself may do this";
  private static final String ADMIN_REQUIRED = "Required role: ADMIN";

  private OwnerOrAdminAccess() {}

  /** The caller's id, or null when the request has no request context or no identity. */
  public static String identityOf(StreamRuneContext.RequestContext caller) {
    return caller == null || caller.userId() == null ? null : caller.userId().value();
  }

  /** The request carries the ADMIN role: the demo's only definition of the role. */
  public static boolean hasAdminRole(StreamRuneContext.RequestContext caller) {
    return caller != null && ADMIN.equals(caller.baggage().get(ROLE_BAGGAGE_KEY));
  }

  /** Refuses a request that does not carry the ADMIN role: the guard of the ADMIN-only endpoints. */
  public static void requireAdminRole(StreamRuneContext.RequestContext caller) {
    if (!hasAdminRole(caller)) {
      throw new AccessRefusedException(ADMIN_REQUIRED);
    }
  }

  /** An authenticated ADMIN: the role alone, without an identity, is nobody. */
  public static boolean isAdmin(StreamRuneContext.RequestContext caller) {
    return identityOf(caller) != null && hasAdminRole(caller);
  }

  /** An authenticated ADMIN, or the customer themself. Needs no read model. */
  public static boolean mayAccessCustomer(
      StreamRuneContext.RequestContext caller, String customerId) {
    String identity = identityOf(caller);
    return identity != null && (hasAdminRole(caller) || identity.equals(customerId));
  }

  /**
   * An authenticated ADMIN, or the customer the order's read-model row names. An order without a
   * row (null) has no known owner: only an ADMIN may go on.
   */
  public static boolean mayAccessOrder(StreamRuneContext.RequestContext caller, OrderView order) {
    String identity = identityOf(caller);
    if (identity == null) {
      return false;
    }
    return hasAdminRole(caller) || (order != null && identity.equals(order.customerId()));
  }

  public static void requireCustomerAccess(
      StreamRuneContext.RequestContext caller, String customerId) {
    if (!mayAccessCustomer(caller, customerId)) {
      throw new AccessRefusedException(REFUSED);
    }
  }

  public static void requireOrderAccess(StreamRuneContext.RequestContext caller, OrderView order) {
    if (!mayAccessOrder(caller, order)) {
      throw new AccessRefusedException(REFUSED);
    }
  }

  /** The caller's id; refuses a request without an identity. */
  public static String requireIdentity(StreamRuneContext.RequestContext caller) {
    String identity = identityOf(caller);
    if (identity == null) {
      throw new AccessRefusedException(REFUSED);
    }
    return identity;
  }
}
```

The class reads the same two things the interceptor does, from the one object the request filter built: the user id (the trusted-gateway mode from Step 3) and the `role` baggage entry (the demo shortcut from Step 2). The customer id is the user id in this demo — the frontend's customer persona sends its own customer id as `X-User-Id` — so "themself" is a plain comparison. `hasAdminRole` is the demo's only definition of `ADMIN`; every later chapter that restricts an endpoint to operators (the audit log, the saga list, the admin panel, the event history) calls `requireAdminRole`.

**The reads go through one more class.** A single customer or order is a yes-or-no question, but a list is not: an `ADMIN` gets every row, a customer their own, and the rows must be chosen before anything is returned, not filtered out of a full list in the controller. That needs the read models, so the class lives in the `projections` module. Create `projections/src/main/java/org/streamrune/ecommerce/projections/access/OwnerOrAdminQueries.java`:

```java
package org.streamrune.ecommerce.projections.access;

import java.util.List;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.ecommerce.projections.CustomerProjection;
import org.streamrune.ecommerce.projections.OrderProjection;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.ecommerce.queries.dto.CustomerView;
import org.streamrune.ecommerce.queries.dto.OrderView;

/**
 * The customer and order read models as one caller may see them: OwnerOrAdminAccess applied to
 * every read. A refusal is an AccessRefusedException.
 */
public final class OwnerOrAdminQueries {

  private final CustomerProjection customers;
  private final OrderProjection orders;

  public OwnerOrAdminQueries(CustomerProjection customers, OrderProjection orders) {
    if (customers == null) {
      throw new IllegalArgumentException("customers is required");
    }
    if (orders == null) {
      throw new IllegalArgumentException("orders is required");
    }
    this.customers = customers;
    this.orders = orders;
  }

  /** One customer, or null when the read model has no row. Decided before the read. */
  public CustomerView customer(StreamRuneContext.RequestContext caller, String customerId) {
    OwnerOrAdminAccess.requireCustomerAccess(caller, customerId);
    return customers.get(customerId);
  }

  /** Every customer for an ADMIN; the caller's own record, read by its id, for anyone else. */
  public List<CustomerView> customers(StreamRuneContext.RequestContext caller) {
    String identity = OwnerOrAdminAccess.requireIdentity(caller);
    if (OwnerOrAdminAccess.isAdmin(caller)) {
      return customers.listAll();
    }
    CustomerView own = customers.get(identity);
    return own == null ? List.of() : List.of(own);
  }

  /** One order; null only for an ADMIN, when the read model has no row for the id. */
  public OrderView order(StreamRuneContext.RequestContext caller, String orderId) {
    OwnerOrAdminAccess.requireIdentity(caller);
    OrderView order = orders.get(orderId);
    OwnerOrAdminAccess.requireOrderAccess(caller, order);
    return order;
  }

  /**
   * Every order for an ADMIN, narrowed to one customer when the request names one. The caller's own
   * orders for anyone else: the filter can repeat the caller's id and cannot name another customer.
   */
  public List<OrderView> orders(StreamRuneContext.RequestContext caller, String customerId) {
    String identity = OwnerOrAdminAccess.requireIdentity(caller);
    boolean filtered = customerId != null && !customerId.isEmpty();
    if (OwnerOrAdminAccess.isAdmin(caller)) {
      return filtered ? orders.listByCustomer(customerId) : orders.listAll();
    }
    if (filtered) {
      OwnerOrAdminAccess.requireCustomerAccess(caller, customerId);
    }
    return orders.listByCustomer(identity);
  }

  /** The guard of the order commands that act on an existing order. */
  public void requireOrderAccess(StreamRuneContext.RequestContext caller, String orderId) {
    order(caller, orderId);
  }
}
```

**The Spring side is three small pieces.** First, where the caller comes from: the framework's `ScopedValueFilter` (Step 3) binds the request context around the whole servlet chain. Create `spring-app/src/main/java/org/streamrune/ecommerce/spring/controller/RequestCaller.java`:

```java
package org.streamrune.ecommerce.spring.controller;

import org.streamrune.core.StreamRuneContext;

/** The caller of the request being served: the request context the framework's filter bound. */
final class RequestCaller {

  private RequestCaller() {}

  /** The request context of the current request, or null when none is bound. */
  static StreamRuneContext.RequestContext current() {
    return StreamRuneContext.CURRENT.isBound() ? StreamRuneContext.CURRENT.get() : null;
  }
}
```

Second, the status of a refusal. Add the import `org.streamrune.ecommerce.queries.access.AccessRefusedException` to `GlobalExceptionHandler` and one handler:

```java
@ExceptionHandler(AccessRefusedException.class)
public ResponseEntity<String> handleAccessRefused(AccessRefusedException e) {
  return ResponseEntity.status(HttpStatus.FORBIDDEN).body(e.getMessage());
}
```

Third, the bean. Add the import `org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries` to `StreamRuneConfig` and:

```java
@Bean
public OwnerOrAdminQueries ownerOrAdminQueries(
    CustomerProjection customerProjection, OrderProjection orderProjection) {
  return new OwnerOrAdminQueries(customerProjection, orderProjection);
}
```

**Now the controllers ask.** The two query controllers hand every read to `OwnerOrAdminQueries`. Replace `CustomerQueryController` from Chapter 6:

```java
package org.streamrune.ecommerce.spring.controller;

import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.dto.CustomerView;

/** The customer read model, for an authenticated ADMIN or the customer themself; 403 otherwise. */
@RestController
@RequestMapping("/api/customers")
public class CustomerQueryController {

  private final OwnerOrAdminQueries queries;

  public CustomerQueryController(OwnerOrAdminQueries queries) {
    this.queries = queries;
  }

  /** One customer: 404 when the caller may read it and the read model has no row. */
  @GetMapping("/{id}")
  public ResponseEntity<CustomerView> getCustomer(@PathVariable String id) {
    CustomerView customer = queries.customer(RequestCaller.current(), id);
    return customer != null ? ResponseEntity.ok(customer) : ResponseEntity.notFound().build();
  }

  /** Every customer for an ADMIN; the caller's own record for anyone else. */
  @GetMapping
  public List<CustomerView> listCustomers() {
    return queries.customers(RequestCaller.current());
  }
}
```

and `OrderQueryController` from Chapter 8:

```java
package org.streamrune.ecommerce.spring.controller;

import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.dto.OrderView;

/** The order read model, for an authenticated ADMIN or the customer an order names; 403 otherwise. */
@RestController
@RequestMapping("/api/orders")
public class OrderQueryController {

  private final OwnerOrAdminQueries queries;

  public OrderQueryController(OwnerOrAdminQueries queries) {
    this.queries = queries;
  }

  /** One order. 404 is an ADMIN's answer for an id without a row; a customer gets 403 for it. */
  @GetMapping("/{id}")
  public ResponseEntity<OrderView> getOrder(@PathVariable String id) {
    OrderView order = queries.order(RequestCaller.current(), id);
    return order != null ? ResponseEntity.ok(order) : ResponseEntity.notFound().build();
  }

  /** Every order for an ADMIN, narrowed by customerId; the caller's own orders for anyone else. */
  @GetMapping
  public List<OrderView> listOrders(@RequestParam(required = false) String customerId) {
    return queries.orders(RequestCaller.current(), customerId);
  }
}
```

The command controllers ask as their first line, before anything is written. In `CustomerCommandController` (import `org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess`) every handler names a customer, by the body for a registration and by the path for the rest:

```java
@PostMapping
public ResponseEntity<Void> registerCustomer(@Valid @RequestBody RegisterRequest req) {
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), req.customerId());
    // ... the rest as in Chapter 6
}

@PutMapping("/{id}")
public ResponseEntity<Void> updateProfile(
    @PathVariable String id, @RequestBody UpdateProfileRequest req) {
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), id);
    // ... the rest as in Chapter 6
}

@PostMapping("/{id}/export-data")
public ResponseEntity<Void> exportData(@PathVariable String id) {
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), id);
    commandBus.execute(new CustomerCommand.RequestDataExport(id));
    return ResponseEntity.ok().build();
}

@PostMapping("/{id}/forget")
public ResponseEntity<ForgetResponse> forgetCustomer(@PathVariable String id) {
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), id);
    commandBus.execute(new CustomerCommand.ForgetCustomer(id));
    // ... the rest as in Chapter 7
}
```

`OrderCommandController` takes the `OwnerOrAdminQueries` bean as a second constructor argument (field `queries`; imports `org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries` and `org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess`). Placing an order names its customer in the body; confirming and cancelling name an order, whose owner the read model knows:

```java
@PostMapping
public ResponseEntity<Void> placeOrder(@Valid @RequestBody PlaceOrderRequest req) {
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), req.customerId());
    // ... the rest as in Chapter 8
}

@PostMapping("/{id}/confirm")
public ResponseEntity<Void> confirmOrder(@PathVariable String id) {
    queries.requireOrderAccess(RequestCaller.current(), id);
    commandBus.execute(new OrderCommand.ConfirmOrder(id));
    return ResponseEntity.ok().build();
}

@PostMapping("/{id}/cancel")
public ResponseEntity<Void> cancelOrder(
    @PathVariable String id, @Valid @RequestBody CancelOrderRequest req) {
    queries.requireOrderAccess(RequestCaller.current(), id);
    commandBus.execute(new OrderCommand.CancelOrder(id, req.reason()));
    return ResponseEntity.ok().build();
}
```

`ShipOrder` and `DeliverOrder` keep their `@RequireRole("ADMIN")` from Step 1: they are an operator's commands whoever the order belongs to.

**Who gets what.** Four callers — one without an identity, an authenticated customer who is not the owner, the owner, and an `ADMIN`:

| Endpoint | No identity | Another customer | The owner | `ADMIN` |
|----------|-------------|------------------|-----------|---------|
| `GET /api/customers/{id}` | `403` | `403` | `200`, or `404` without a row | `200`, or `404` without a row |
| `GET /api/customers` | `403` | `200`: their own record only | `200`: their own record only | `200`: every customer |
| `POST /api/customers` | `403` | `403` | `200` | `200` |
| `PUT /api/customers/{id}`, `POST …/export-data`, `POST …/forget` | `403` | `403` | `200` | `200` |
| `GET /api/orders/{id}` | `403` | `403` | `200` | `200`, or `404` without a row |
| `GET /api/orders` | `403` | `200`: their own orders only | `200`: their own orders only | `200`: every order |
| `GET /api/orders?customerId=X` | `403` | `403` when `X` is not the caller | `200`: their own orders | `200`: the orders of `X` |
| `POST /api/orders` | `403` | `403` | `200` | `200` |
| `POST /api/orders/{id}/confirm`, `…/cancel` | `403` | `403` | `200` | `200` |

Three decisions are worth a second look:

- **A refusal is always `403`, never `404`, and it says nothing about existence.** For a customer the rule compares two ids and never asks the read model, so another customer's record and a record that does not exist are refused alike. For an order a caller who is not an `ADMIN` gets the same `403` for another customer's order and for an id without a row: an order without a row has no known owner. `404` is kept for the two callers who may know — the customer for their own record (a forgotten customer has no row, Chapter 7), and an `ADMIN`. A request without an identity is refused with `403` as well; the demo has no login to send a `401` to. One consequence to know: the read model follows the event store by up to the projection's polling interval (five seconds by default), so right after `POST /api/orders` the order has no row yet and its own customer is answered `403` too, until the row is there. The customer's list never refuses them: it simply does not hold the new order for that moment.
- **A list is scoped where it is read.** A customer's `GET /api/customers` is one read by their own id, and their `GET /api/orders` is `OrderProjection.listByCustomer` with their own id (Chapter 8), never `listAll()`: the rows are chosen by the read model's own query, not picked out of everybody's in the controller. No query parameter widens the result: `customerId` may repeat the caller's id, and naming anybody else is a `403`.
- **An order belongs to the customer it names, and only that customer or an `ADMIN` may place it.** `POST /api/orders` takes `customerId` from the request body, so the check ties it to the caller.

Try it. Register Carol and Dave as themselves, then read as each caller (`p-1` is the product from Chapter 2). Give the projection its five seconds after each write before you read: until the row is there, Carol's own record answers `404` and her own order `403`.

```bash
for c in carol dave; do
  curl -s -X POST http://localhost:8080/api/customers \
    -H 'Content-Type: application/json' -H "X-User-Id: c-$c" -H 'X-User-Role: CUSTOMER' \
    -d "{\"customerId\":\"c-$c\",\"name\":\"$c\",\"email\":\"$c@example.com\",\"address\":\"1 Main St\",\"phone\":\"555-0100\"}"
done

# No identity, or someone else's: 403
curl -i http://localhost:8080/api/customers/c-carol
curl -i http://localhost:8080/api/customers/c-carol -H 'X-User-Id: c-dave' -H 'X-User-Role: CUSTOMER'
# → 403 Forbidden

# Carol reads her own record, and her list holds nobody else
curl -s http://localhost:8080/api/customers/c-carol -H 'X-User-Id: c-carol' -H 'X-User-Role: CUSTOMER'
curl -s http://localhost:8080/api/customers -H 'X-User-Id: c-carol' -H 'X-User-Role: CUSTOMER'
# → [{"customerId":"c-carol", ...}]

# An ADMIN reads everyone
curl -s http://localhost:8080/api/customers -H 'X-User-Id: admin-1' -H 'X-User-Role: ADMIN'
# → [..., {"customerId":"c-carol", ...}, {"customerId":"c-dave", ...}]

# The role header alone is nobody
curl -i http://localhost:8080/api/customers -H 'X-User-Role: ADMIN'
# → 403 Forbidden
```

Orders follow the same lines:

```bash
# Dave may not place an order in Carol's name
curl -i -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' -H 'X-User-Id: c-dave' -H 'X-User-Role: CUSTOMER' \
  -d '{"orderId":"order-acl-1","customerId":"c-carol","lines":[{"productId":"p-1","quantity":1,"unitPrice":19.99}]}'
# → 403 Forbidden

# Carol places it herself and reads it; Dave gets 403 for it, and his own list is empty
curl -s -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' -H 'X-User-Id: c-carol' -H 'X-User-Role: CUSTOMER' \
  -d '{"orderId":"order-acl-1","customerId":"c-carol","lines":[{"productId":"p-1","quantity":1,"unitPrice":19.99}]}'
curl -s http://localhost:8080/api/orders/order-acl-1 -H 'X-User-Id: c-carol' -H 'X-User-Role: CUSTOMER'
curl -i http://localhost:8080/api/orders/order-acl-1 -H 'X-User-Id: c-dave' -H 'X-User-Role: CUSTOMER'
# → 403 Forbidden
curl -s http://localhost:8080/api/orders -H 'X-User-Id: c-dave' -H 'X-User-Role: CUSTOMER'
# → []
curl -i "http://localhost:8080/api/orders?customerId=c-carol" -H 'X-User-Id: c-dave' -H 'X-User-Role: CUSTOMER'
# → 403 Forbidden
```

And the GDPR routes from Chapter 7, with Bob (register him again, as himself, if your database was reset):

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

# Bob's record is gone for the two callers who may ask
curl -i http://localhost:8080/api/customers/c-bob -H 'X-User-Id: admin-1' -H 'X-User-Role: ADMIN'
# → 404 Not Found
```

A refused request writes no event and leaves the key alone: the check runs before any command is dispatched.

> **Demo shortcut — protect this in production.** The rule is the one a real deployment keeps; what it is handed is not. The demo runs in the trusted-gateway mode without a gateway, so anyone who sends `X-User-Id: c-alice` is Alice, and anyone who adds `X-User-Role: ADMIN` is an operator. Behind real authentication the same classes decide on an identity the client cannot choose.

> **Quarkus and Micronaut.** Both apps use the same `OwnerOrAdminAccess` and `OwnerOrAdminQueries` and answer the table above status for status. Each has its own three small pieces: a `RequestCaller` that says where its integration keeps the request context (the request-scoped `StreamRuneRequestContextHolder` on Quarkus, the `ScopedValue` or the `StreamRuneContextHelper` thread-local on Micronaut), a mapper from `AccessRefusedException` to `403` (`AccessRefusedExceptionMapper`, `AccessRefusedExceptionHandler`), and the `OwnerOrAdminQueries` bean. Each of the three apps has an `OwnerOrAdminRestAccessIT` that walks the table over HTTP with the four callers and asserts exactly which rows a list returns; the rule itself is unit-tested as a table in `queries` (`OwnerOrAdminAccessTest`), and the rows of the lists in `projections` (`OwnerOrAdminQueriesTest`).

---

## What We Learned

- **`@RequireRole`** is a runtime-retained annotation you place on a command record to declare that the caller must hold at least one of the listed roles. When multiple roles are listed, any single match suffices (OR). The annotation lives in the `domain` module with no infrastructure imports.

- **`@RequirePermission`** works identically but checks fine-grained permissions rather than broad roles. You can combine both on one command: when both annotations are present, the caller must satisfy both (AND).

- **`UserRoleResolver`** is a `@FunctionalInterface` that maps a `UserId` to a `UserAuthority`. You implement it and register it as a bean; you own the data source. A resolver that reads the request, like the demo's, also overrides `requiresRequestContext()` to return `true`: the framework then calls it once at the request edge and never on a dead-letter replay, which runs an already-authorized command under the dead-letter-replay system principal instead.

- **`AnnotationAuthorizationInterceptor`** is a `CommandInterceptor` from `streamrune-runtime`. It reads both annotations from the command class via reflection, checks them against the caller's authority (the one captured at the request edge for a resolver that declares `requiresRequestContext()`, otherwise the answer of `UserRoleResolver.resolve()`), and throws `AuthorizationException` if the requirement is not met. If neither annotation is present it is a pure no-op with effectively zero overhead.

- **`ScopedValue`** (finalized in Java 25; preview in Java 21–24) replaces `ThreadLocal` for propagating request context through virtual threads. The framework's `ScopedValueFilter` (auto-registered by `StreamRuneAutoConfiguration`) wraps each request in `ScopedValue.where(StreamRuneContext.CURRENT, ctx).call(...)`, making the context available to all code that runs in that scope, including code dispatched onto virtual threads by the `CommandBus`. In the trusted-gateway mode the filter also reads `X-User-Role` and places it into the baggage map under the key `"role"`, which `HeaderUserRoleResolver` reads; in any other mode it ignores that header.

- **Identity and role in the demo are stand-ins.** The apps run in the trusted-gateway mode (`streamrune.security.trust-user-id-header: true`), so `X-User-Id` is the identity — acceptable only because the demo stands in for a gateway that would authenticate callers and set the header itself. `HeaderUserRoleResolver` goes further and authorizes on `X-User-Role`. The framework reads that header only in the trusted-gateway mode, where a real gateway would set it; in the demo nothing authenticates it: a demo shortcut a real deployment must replace with a resolver that derives the authority from the authenticated identity.

- **Ownership is checked where the id is known.** `@RequireRole` decides by the command's type; "an `ADMIN`, or the customer themself" depends on the id the request names. The demo writes that rule once (`OwnerOrAdminAccess`), asks it in every customer and order route — reads through `OwnerOrAdminQueries`, which also scopes a list to the caller's own rows — and answers `403` before anything is read or written. A refusal never tells a caller whether the thing exists.

- **Interceptor ordering** matters: authorization runs before validation, so an unauthorized caller cannot probe the validation rules. In the next chapter audit goes in front of authorization, so a refused command is still recorded.

---

## Next Up

We can control who does what. Next, let's add an audit trail.
