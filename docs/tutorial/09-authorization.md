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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
   *
   * <p>The lines are not empty and name each product once: stock is reserved per order and product,
   * so two lines of one product would be reserved once. The message never echoes a product id.
   */
  record PlaceOrder(String orderId, String customerId, List<OrderLine> lines)
      implements OrderCommand {

    /** The prefix of the saga id the order starts: {@code "fulfillment-" + orderId}. */
    public static final String SAGA_ID_PREFIX = "fulfillment-";

    /** The longest order id whose saga id {@code "fulfillment-" + orderId} fits 255 characters. */
    public static final int MAX_ORDER_ID_LENGTH =
        IdConstraints.MAX_LENGTH - SAGA_ID_PREFIX.length();

    public PlaceOrder {
      requireValidOrderId(orderId);
      if (lines == null || lines.isEmpty()) {
        throw new IllegalArgumentException("lines must not be empty");
      }
      Set<String> products = new HashSet<>();
      for (OrderLine line : lines) {
        if (line == null) {
          throw new IllegalArgumentException("lines must not hold null");
        }
        if (!products.add(line.productId())) {
          throw new IllegalArgumentException("lines must name each product once");
        }
      }
      lines = List.copyOf(lines);
    }

    /**
     * The length rule of an order id: at most {@value #MAX_ORDER_ID_LENGTH} characters, so that the
     * saga id fits. A blank id or one with a control character is the command bus's to refuse. The
     * message never echoes the id.
     */
    public static void requireValidOrderId(String orderId) {
      if (orderId != null && orderId.length() > MAX_ORDER_ID_LENGTH) {
        throw new IllegalArgumentException(
            "orderId must be at most "
                + MAX_ORDER_ID_LENGTH
                + " characters, got "
                + orderId.length());
      }
    }
  }

  /**
   * Confirms a placed order. The fulfillment saga sends it once the payment is captured and every
   * line reserved; the order decider's guard refuses it from anybody else.
   */
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
   * placed and paid for, and the saga would cancel it and the cancellation refund it. Refused here,
   * the order endpoint answers {@code 400} and nothing is written. The message never echoes the id.
   *
   * <p>The order endpoints take a product and a quantity from the client and the unit price from
   * the product catalogue.
   *
   * <p>{@link OrderEvent.OrderLine}, the line an {@code OrderPlaced} event records, has no such
   * rule: it is rebuilt from the event log and from saga state, where a rule would make a stored
   * order unreadable instead of stopping a write.
   */
  record OrderLine(String productId, int quantity, Money unitPrice) {

    /** The longest product id: it is the inventory aggregate id, so the id bound applies as is. */
    public static final int MAX_PRODUCT_ID_LENGTH = IdConstraints.MAX_LENGTH;

    public OrderLine {
      requireValidProductId(productId);
      if (quantity <= 0) {
        throw new IllegalArgumentException("quantity must be positive, got " + quantity);
      }
      if (unitPrice == null || unitPrice.amount() == null || unitPrice.amount().signum() <= 0) {
        throw new IllegalArgumentException("unitPrice must be positive");
      }
    }

    /**
     * The rule a product id of an order line follows: present, at most {@value
     * #MAX_PRODUCT_ID_LENGTH} characters, no control character. The message never echoes the id.
     */
    public static void requireValidProductId(String productId) {
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
 * client-supplied {@code X-User-Role} header.
 *
 * <p>The StreamRune request filter copies {@code X-User-Role} into the context baggage under {@code
 * role} — only in the trusted-gateway mode ({@code streamrune.security.trust-user-id-header=true},
 * which this app runs in) — and {@link #resolveCurrentRole()} reads it back. So the role is
 * whatever the client says:
 *
 * <ul>
 *   <li>Any caller becomes {@code ADMIN} by sending {@code X-User-Role: ADMIN}; nothing checks the
 *       value. The trusted-gateway mode assumes a gateway in front that sets or strips the header,
 *       exactly as it does {@code X-User-Id}; the demo has none and only stands in for one.
 *   <li>Outside that mode the framework ignores the header, so this resolver would answer {@code
 *       GUEST} for everyone. A W3C {@code baggage: role=} header never reaches the entry in any
 *       mode.
 *   <li>{@link #resolve(UserId)} ignores its {@code userId}: the authority is not tied to the
 *       identity, only to whatever the gateway (here: the client) put in {@code X-User-Role}.
 * </ul>
 *
 * <p>The demo accepts this because it has no login: the frontend's role switcher, the tutorial's
 * curl examples and the integration tests pick a role by header. A real resolver derives the
 * authority from the authenticated identity: the roles of the Spring Security principal (the
 * framework's {@code SpringSecurityUserRoleResolver} does this), or a lookup keyed by the {@code
 * userId} it is given.
 */
public class HeaderUserRoleResolver implements UserRoleResolver {

  @Override
  public UserAuthority resolve(UserId userId) {
    String role = resolveCurrentRole();
    return new UserAuthority(Set.of(role), permissionsForRole(role));
  }

  /**
   * {@code true}: the answer comes from the request (the {@code role} baggage entry), not from the
   * {@code userId}. The framework's request filter therefore resolves it once, at the request edge,
   * with {@code StreamRuneContext.CURRENT} bound to that request's context, and every authz-gated
   * command of the request is decided against that captured answer.
   *
   * <p>A dead-letter replay has no request, so the framework never asks this resolver there (it
   * could only answer {@code GUEST} and deny the command on every attempt). The authorization
   * interceptor instead completes the already-authorized command under the dead-letter-replay
   * system principal, as the identity the entry recorded, and logs a WARN naming the command. An
   * entry with no recorded identity is still refused.
   */
  @Override
  public boolean requiresRequestContext() {
    return true;
  }

  /**
   * Reads the {@code role} baggage entry, which the request filter copies from {@code X-User-Role}
   * in the trusted-gateway mode (a demo shortcut, see the class comment); {@code GUEST} when no
   * request context is bound or the entry is absent.
   */
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

> **What `X-Correlation-Id` is, and what the filter refuses.** A correlation id is tracing data. The filter binds the value the client sends (the first one when the header arrives more than once) and generates an id when the header is missing or blank; the id then travels into the metadata of every event and into every audit row the request causes. Nothing is addressed, authorized or deduplicated by it. One form is not the client's to send: an id that starts with `streamrune:` is reserved for the framework, which binds `streamrune:saga:<sagaId>` around the commands a saga dispatches (Chapter 11). The filter answers a request whose correlation id has that prefix with `400 Bad Request`, `Content-Type: text/plain;charset=UTF-8` and the body `The request's correlation id, trace id or user id is not acceptable`, before any command runs. It gives the same answer to a correlation id, trace id or user id that is longer than 255 characters or holds a control character. The Quarkus and Micronaut filters answer alike.

> **`X-User-Id` is required for any annotated command.** Before checking roles, `AnnotationAuthorizationInterceptor` reads the caller's id from the request context it captures (`StreamRuneContext`, whose `userId()` the filter populates from the `X-User-Id` header once you enable the trusted-gateway mode below). If it is null — because the request omitted `X-User-Id` — the interceptor throws `AuthorizationException("Authentication required")` rather than the role message. Like every other `AuthorizationException`, this surfaces as `403 Forbidden` once Step 6 has given the exception its handler. So a request to a guarded command always needs `X-User-Id` set; `X-User-Role` then decides whether the caller is authorized.

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

> **So does the Micronaut app.** The Micronaut integration follows the same rule too: its `StreamRuneContextFilter` and SSE endpoint ignore `X-User-Id` (and the filter `X-User-Role`) unless the trusted-gateway mode is on, and with no Micronaut Security (or with `micronaut.security.enabled=false`) an application whose command bus carries annotation authorization refuses to start. The demo's `micronaut-app` sets `streamrune.security.trust-user-id-header: true` in `micronaut-app/src/main/resources/application.yml` and logs the same `Request identity: TRUSTED_GATEWAY …` line at startup. The same file sets `micronaut.server.netty.server-type: full_content`. The request context reaches a route under either server type: the filter stores it on the request, and the Micronaut module's command bus and query bus bind it from there on whatever thread the route runs, so authorization and the audit see the caller in both modes. The setting decides where a route runs. With Micronaut's default `streamed` type, a request whose body arrives after its headers resumes the route on a Netty event loop, and the demo's routes block on the command bus, which there would stall every connection that event loop serves; `full_content` reads the whole request before routing, so every route runs on the blocking executor the filter runs on.

`ScopedValue` (finalized in Java 25; a preview API in Java 21–24) is safe to use with virtual threads: the value is visible to the current thread and any threads started within the scope's call block, and it is automatically unbound when the scope exits. This is why it replaced `ThreadLocal` in StreamRune's context propagation.

### Step 4 — Create the AnnotationAuthorizationInterceptor bean

Open `spring-app/src/main/java/org/streamrune/ecommerce/spring/config/StreamRuneConfig.java` and add two beans: the `HeaderUserRoleResolver` and the `AnnotationAuthorizationInterceptor` that wraps it.

```java
@Bean
public HeaderUserRoleResolver userRoleResolver() {
  return new HeaderUserRoleResolver();
}
```

and, among the interceptors after `validationInterceptor`:

```java
@Bean
public AnnotationAuthorizationInterceptor authInterceptor(HeaderUserRoleResolver resolver) {
  return new AnnotationAuthorizationInterceptor(resolver);
}
```

`AnnotationAuthorizationInterceptor` comes from `streamrune-runtime`. Its constructor takes a `UserRoleResolver`. In `before()`, it reads `@RequireRole` and `@RequirePermission` from the command class using reflection. If neither annotation is present, it is a no-op. If an annotation is present, it takes the caller's authority (for this resolver, the one the request filter captured at the request edge) and throws `AuthorizationException` if the requirement is not met. The exception is thrown before `decide` is called — no event is ever written.

`AnnotationAuthorizationInterceptor` is in `org.streamrune.runtime`, which the file imports whole; `HeaderUserRoleResolver` is in the same package as `StreamRuneConfig`.

### Step 5 — Add the auth interceptor to the command bus chain

Still in `StreamRuneConfig.java`, wire the auth interceptor into the `VirtualThreadCommandBus` builder, placing it before validation. The bean takes it as a parameter and lists it first in `.interceptors(...)`:

```java
@Bean
public VirtualThreadCommandBus commandBus(
    EventStore store,
    BeanValidationInterceptor validation,
    AnnotationAuthorizationInterceptor auth) {
  return VirtualThreadCommandBus.builder()
      .eventStore(store)
      .interceptors(auth, validation)
      .snapshotPolicy(SnapshotPolicy.everyNEvents(5))
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
          OrderState.TYPE,
          OrderCommand.class,
          cmd ->
              AggregateId.of(
                  switch (cmd) {
                    case OrderCommand.PlaceOrder c -> c.orderId();
                    case OrderCommand.ConfirmOrder c -> c.orderId();
                    case OrderCommand.ShipOrder c -> c.orderId();
                    case OrderCommand.DeliverOrder c -> c.orderId();
                    case OrderCommand.CancelOrder c -> c.orderId();
                  }),
          new OrderDecider())
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
```

You do **not** need to register a filter bean. The framework's `ScopedValueFilter` is auto-registered by `StreamRuneAutoConfiguration` and runs for all requests.

The interceptor order inside `interceptors(...)` matters: the bus calls each `before()` in the listed order. Authorization runs first, so a caller who may not send a command is refused before Bean Validation looks at it, and the validation messages never tell that caller what a valid command looks like. The audit interceptor you add in the next chapter goes in front of both, and the circuit breaker from Chapter 12 behind them. This is the framework's canonical order (`CommandInterceptorOrdering` in `streamrune-runtime`), the one the Quarkus and Micronaut integrations give the chains they assemble themselves.

### Step 6 — Test: CUSTOMER cannot ship, ADMIN can

Start the application:

```bash
./gradlew :spring-app:bootRun
```

**Place an order as a customer:**

```bash
curl -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -H 'X-User-Id: customer-1' \
  -H 'X-User-Role: CUSTOMER' \
  -d '{
    "orderId": "order-auth-test",
    "customerId": "customer-1",
    "lines": [{"productId": "p-1", "quantity": 1}]
  }'
```

It succeeds: `PlaceOrder` has no `@RequireRole` annotation, so the interceptor is a no-op for it. The order stays `CREATED`, because nobody but the fulfillment saga of Chapter 11 confirms an order. That rule is not about a role: an `ADMIN` may not confirm by hand either. It is a rule about the sender, and it lives in the order decider's `guard` (Chapter 8, Step 2), which refuses a `ConfirmOrder` unless `StreamRuneContext.isSagaDispatch()` is `true`. `@RequireRole` asks *which role* the caller has; `guard` sees the loaded state and the dispatch context, so it can ask *who sends* the command, a saga or anybody else. There is no confirm endpoint, and a `ConfirmOrder` that reached the bus by another door would be refused with an `AuthorizationException`.

**First decide what a refusal looks like over HTTP.** `AuthorizationException` extends `DomainException`, and the `GlobalExceptionHandler` from Chapter 1 maps every `DomainException` to `400 Bad Request`: the status of a broken business rule, which a refusal is not. Give the exception a handler of its own in `GlobalExceptionHandler` (import `org.streamrune.core.AuthorizationException`); Spring picks the most specific handler, so the one for `DomainException` keeps every other case:

```java
@ExceptionHandler(AuthorizationException.class)
public ResponseEntity<String> handleAuthorization(AuthorizationException e) {
  return ResponseEntity.status(HttpStatus.FORBIDDEN).body(e.getMessage());
}
```

**Try to ship as a CUSTOMER — expect rejection:**

```bash
curl -v -X POST http://localhost:8080/api/orders/order-auth-test/ship \
  -H 'X-User-Id: customer-1' \
  -H 'X-User-Role: CUSTOMER'
```

The response is `403 Forbidden` with the body:

```
Required role: ADMIN
```

The filter bound `role=CUSTOMER` in the `ScopedValue` (and `userId=customer-1` from `X-User-Id`), resolved the caller's authority via `HeaderUserRoleResolver` and stored it in the request context. The interceptor read `@RequireRole("ADMIN")` from `ShipOrder`, found no `ADMIN` role in that authority, and threw `AuthorizationException`. The command never reached `OrderDecider`. Nothing was written to the event store.

> **`403` without an identity too.** A request with no `X-User-Id` is refused with the body `Authentication required`, and the same handler answers it with `403`: the demo has no login to send a `401` to. Without the handler both refusals would be a `400`, indistinguishable from an order that cannot be shipped in its current state.

**Ship as an ADMIN — the role check passes:**

```bash
curl -i -X POST http://localhost:8080/api/orders/order-auth-test/ship \
  -H 'X-User-Id: admin-1' \
  -H 'X-User-Role: ADMIN'
```

The filter bound `role=ADMIN`, the resolver returned `UserAuthority` containing `ADMIN`, and `authority.hasAnyRole("ADMIN")` returned `true`: the command reached `OrderDecider`. The answer is `400 Bad Request` with `Can only ship CONFIRMED orders, current status: CREATED`, the state machine's rule, because nobody has confirmed the order yet. Once the saga of Chapter 11 confirms orders, the same call answers `200 OK`.

Notice what made you an admin: two headers you typed. The identity half is the trusted-gateway mode (Step 3) — in production a gateway sets `X-User-Id`, not the client. The role half is the demo shortcut from Step 2 — in production the role comes from the authenticated identity, never from a header.

**Verify the discontinue guard on products:**

Discontinuing a product is a `POST` to `/api/products/{id}/discontinue` (see `ProductCommandController`), and like the order commands it needs `X-User-Id` so the interceptor can resolve a caller. Discontinue a product of its own, not `p-1`: the catalogue prices every order, and a discontinued product cannot be ordered (`400 Product is discontinued`), and Step 7 orders `p-1`.

```bash
# A product to retire
curl -X POST http://localhost:8080/api/products \
  -H 'Content-Type: application/json' -H 'X-User-Id: admin-1' -H 'X-User-Role: ADMIN' \
  -d '{"productId":"p-retired","name":"Old widget","description":"Retired","category":"Gadgets","price":9.99,"initialStock":0}'

# Attempt as CUSTOMER
curl -v -X POST http://localhost:8080/api/products/p-retired/discontinue \
  -H 'X-User-Id: customer-1' \
  -H 'X-User-Role: CUSTOMER'
# → 403 Forbidden — "Required role: ADMIN"

# Attempt as ADMIN
curl -X POST http://localhost:8080/api/products/p-retired/discontinue \
  -H 'X-User-Id: admin-1' \
  -H 'X-User-Role: ADMIN'
# → 200 OK
```

### Step 7 — A customer's data: an ADMIN or the customer themself

Chapters 6 to 8 left every customer and order route open, because no request in them carried an identity. Now one does, and the routes need it: `GET /api/customers` returns every customer's name, e-mail, address and phone, `GET /api/orders` every customer's purchases, and `POST /api/customers/{id}/forget` destroys a customer's encryption key, which cannot be undone.

Who may call them depends on *which* customer the request names — an `ADMIN` may read or erase anyone, a customer only themself — and `@RequireRole` cannot express that: it looks at the command's type, never at the id inside it, and it guards commands, not reads. So the demo has one rule of its own, written once and asked everywhere a customer's data is handed out or changed:

> **An authenticated caller who holds the `ADMIN` role, or the customer the data belongs to. Everyone else is refused — a caller without an identity first of all.**

The rule is plain Java with no Spring in it, so it goes into the shared `queries` module, where `quarkus-app` and `micronaut-app` use the same class. A refusal is an exception of its own. Create `queries/src/main/java/org/streamrune/ecommerce/queries/access/AccessRefusedException.java`:

```java
package org.streamrune.ecommerce.queries.access;

/**
 * The caller may not read or change what the request names: thrown by {@link OwnerOrAdminAccess}
 * and by the guarded reads built on it. Each application answers it with {@code 403 Forbidden},
 * whether the request carried no identity or the wrong one.
 *
 * <p>The message never names the resource: a caller who is refused learns nothing about whether it
 * exists.
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
 * The demo's one rule for a customer's data: an authenticated caller who holds the {@code ADMIN}
 * role, or the customer the data belongs to. Everyone else is refused, a caller without an identity
 * first of all.
 *
 * <p>Every place that hands out or changes one customer's data decides here: the REST reads and
 * writes of customers and orders and the reads of the fulfillment sagas ({@code
 * OwnerOrAdminQueries} for the reads, the command controllers for the writes) and the per-stream
 * SSE endpoint ({@link OwnerOrAdminStreamAccess}). The {@code ADMIN}-only endpoints (the admin
 * panel, the audit log, the event history, the payment-failure switch, the product and inventory
 * writes) ask {@link #requireAdmin}. Both go through {@link #isAdmin}, the demo's one definition of
 * an {@code ADMIN}: the role together with an identity.
 *
 * <p><b>Who the caller is.</b> The request context the framework's request filter built for the
 * request. Its {@code userId()} is the caller's identity, {@code null} for a request without one.
 * The {@code ADMIN} role is the {@code role} entry of its baggage: the entry the request filter
 * writes from {@code X-User-Role}, and the one each app's {@code HeaderUserRoleResolver} resolves
 * the caller's authority from. A role without an identity is nobody, on every endpoint.
 *
 * <p><b>Who owns what.</b>
 *
 * <ul>
 *   <li>A customer record belongs to the caller whose id equals the customer id.
 *   <li>An order belongs to the customer its read-model row names. An order without a row has no
 *       known owner: only an {@code ADMIN} gets past it, and a customer is answered as for an order
 *       of somebody else. The refusal therefore does not tell a caller whether an order exists.
 *   <li>A fulfillment saga belongs to the order it fulfils, the order its id names ({@code
 *       "fulfillment-" + orderId}), and so to that order's customer.
 * </ul>
 *
 * <p>DEMO SHORTCUT, the gateway stand-in ch. 9 of the tutorial describes: the demo runs in the
 * trusted-gateway mode without a gateway, so the id and the role are whatever the client put in
 * {@code X-User-Id} and {@code X-User-Role}. The rule is the one a real deployment keeps; the
 * identity it is handed must then come from real authentication.
 */
public final class OwnerOrAdminAccess {

  private static final String ROLE_BAGGAGE_KEY = "role";
  private static final String ADMIN = "ADMIN";
  private static final String REFUSED = "Only an ADMIN or the customer themself may do this";
  private static final String ADMIN_REQUIRED = "Required role: ADMIN";

  private OwnerOrAdminAccess() {}

  /**
   * The caller's id.
   *
   * @param caller the request context of the request, or {@code null} when none is bound
   * @return the id, or {@code null} when the request has no request context or no identity
   */
  public static String identityOf(StreamRuneContext.RequestContext caller) {
    return caller == null || caller.userId() == null ? null : caller.userId().value();
  }

  /**
   * Whether the caller is an {@code ADMIN}: a caller with an identity whose request carries the
   * role, that is, whose {@code role} baggage entry equals {@code ADMIN} exactly. This is the
   * demo's only definition of an {@code ADMIN}, asked for a customer's data and for the {@code
   * ADMIN}-only endpoints alike. {@code X-User-Role: ADMIN} without an identity is nobody.
   *
   * @param caller the request context of the request, or {@code null} when none is bound
   */
  public static boolean isAdmin(StreamRuneContext.RequestContext caller) {
    return identityOf(caller) != null && ADMIN.equals(caller.baggage().get(ROLE_BAGGAGE_KEY));
  }

  /**
   * Refuses a caller who is not an {@code ADMIN}: the guard of the {@code ADMIN}-only endpoints.
   *
   * @throws AccessRefusedException unless {@link #isAdmin} holds
   */
  public static void requireAdmin(StreamRuneContext.RequestContext caller) {
    if (!isAdmin(caller)) {
      throw new AccessRefusedException(ADMIN_REQUIRED);
    }
  }

  /**
   * Whether the caller may read or change the customer with the given id: an authenticated {@code
   * ADMIN}, or the customer themself. The answer needs no read model, so it is the same for a
   * customer that exists and one that does not.
   *
   * @param caller the request context of the request, or {@code null} when none is bound
   * @param customerId the customer the request names; {@code null} is nobody's
   */
  public static boolean mayAccessCustomer(
      StreamRuneContext.RequestContext caller, String customerId) {
    String identity = identityOf(caller);
    return identity != null && (isAdmin(caller) || identity.equals(customerId));
  }

  /**
   * Whether the caller may read or change an order: an authenticated {@code ADMIN}, or the customer
   * the order's read-model row names.
   *
   * @param caller the request context of the request, or {@code null} when none is bound
   * @param order the order's read-model row, or {@code null} when the read model has none for the
   *     requested id; only an {@code ADMIN} may go on then
   */
  public static boolean mayAccessOrder(StreamRuneContext.RequestContext caller, OrderView order) {
    String identity = identityOf(caller);
    if (identity == null) {
      return false;
    }
    return isAdmin(caller) || (order != null && identity.equals(order.customerId()));
  }

  /**
   * Refuses a caller who may not read or change the customer with the given id.
   *
   * @throws AccessRefusedException unless {@link #mayAccessCustomer} holds
   */
  public static void requireCustomerAccess(
      StreamRuneContext.RequestContext caller, String customerId) {
    if (!mayAccessCustomer(caller, customerId)) {
      throw new AccessRefusedException(REFUSED);
    }
  }

  /**
   * Refuses a caller who may not read or change the given order.
   *
   * @throws AccessRefusedException unless {@link #mayAccessOrder} holds
   */
  public static void requireOrderAccess(StreamRuneContext.RequestContext caller, OrderView order) {
    if (!mayAccessOrder(caller, order)) {
      throw new AccessRefusedException(REFUSED);
    }
  }

  /**
   * Refuses a request without an identity; what an identified caller sees is then scoped by the
   * list it asked for.
   *
   * @return the caller's id
   * @throws AccessRefusedException when the request has no request context or no identity
   */
  public static String requireIdentity(StreamRuneContext.RequestContext caller) {
    String identity = identityOf(caller);
    if (identity == null) {
      throw new AccessRefusedException(REFUSED);
    }
    return identity;
  }
}
```

The class reads the same two things the interceptor does, from the one object the request filter built: the user id (the trusted-gateway mode from Step 3) and the `role` baggage entry (the demo shortcut from Step 2). The customer id is the user id in this demo — the frontend's customer persona sends its own customer id as `X-User-Id` — so "themself" is a plain comparison. `isAdmin` is the demo's only definition of an `ADMIN`: the role together with an identity, so `X-User-Role: ADMIN` on a request without `X-User-Id` is nobody. Every endpoint that is for operators alone calls `requireAdmin`, which is `isAdmin` as a guard: the product writes below, and in later chapters the audit log, the admin panel, the event history and the inventory. The fulfillment sagas of Chapter 11 are not among them: a saga belongs to its order, so its reads follow the order's rule.

**The reads go through one more class.** A single customer or order is a yes-or-no question, but a list is not: an `ADMIN` gets every row, a customer their own, and the rows must be chosen before anything is returned, not filtered out of a full list in the controller. That needs the read models, so the class lives in the `projections` module. Create `projections/src/main/java/org/streamrune/ecommerce/projections/access/OwnerOrAdminQueries.java`:

```java
package org.streamrune.ecommerce.projections.access;

import java.util.List;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.ecommerce.projections.CustomerProjection;
import org.streamrune.ecommerce.projections.OrderProjection;
import org.streamrune.ecommerce.queries.access.AccessRefusedException;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.ecommerce.queries.dto.CustomerView;
import org.streamrune.ecommerce.queries.dto.OrderView;

/**
 * The customer and order read models as one caller may see them: {@link OwnerOrAdminAccess} applied
 * to every read. The three applications answer {@code GET /api/customers}, {@code GET
 * /api/customers/{id}}, {@code GET /api/orders} and {@code GET /api/orders/{id}} from this class
 * and hand it the caller's request context; a refusal is an {@link AccessRefusedException}, which
 * each application answers with {@code 403}.
 *
 * <ul>
 *   <li><b>One customer, one order:</b> an authenticated {@code ADMIN} or the owner; {@code null}
 *       when the caller may read it and the read model has no row (the application answers {@code
 *       404}).
 *   <li><b>A list:</b> an {@code ADMIN} gets every row; any other authenticated caller gets the
 *       rows that are their own, selected by their id before anything is returned; a request
 *       without an identity is refused.
 * </ul>
 *
 * <p>A forgotten customer has no row: {@code CustomerProjection} deletes it. The customer and an
 * {@code ADMIN} then read {@code null} for it, and it is on no list.
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

  /**
   * One customer, for an {@code ADMIN} or the customer themself. The decision is made on the two
   * ids alone, before the read model is asked, so a refused caller cannot tell a customer that
   * exists from one that does not.
   *
   * @return the customer, or {@code null} when the read model has no row for the id
   * @throws AccessRefusedException for everyone else
   */
  public CustomerView customer(StreamRuneContext.RequestContext caller, String customerId) {
    OwnerOrAdminAccess.requireCustomerAccess(caller, customerId);
    return customers.get(customerId);
  }

  /**
   * The customers the caller may see: all of them for an {@code ADMIN}; for any other authenticated
   * caller their own record, read by its id, or an empty list when they have none.
   *
   * @throws AccessRefusedException for a request without an identity
   */
  public List<CustomerView> customers(StreamRuneContext.RequestContext caller) {
    String identity = OwnerOrAdminAccess.requireIdentity(caller);
    if (OwnerOrAdminAccess.isAdmin(caller)) {
      return customers.listAll();
    }
    CustomerView own = customers.get(identity);
    return own == null ? List.of() : List.of(own);
  }

  /**
   * One order, for an {@code ADMIN} or the customer the order names.
   *
   * @return the order; {@code null} only for an {@code ADMIN}, when the read model has no row for
   *     the id
   * @throws AccessRefusedException for everyone else, and for a caller who is not an {@code ADMIN}
   *     when the read model has no row for the id: an order without a row has no known owner, and
   *     the same answer as for somebody else's order keeps its existence to itself
   */
  public OrderView order(StreamRuneContext.RequestContext caller, String orderId) {
    OwnerOrAdminAccess.requireIdentity(caller);
    OrderView order = orders.get(orderId);
    OwnerOrAdminAccess.requireOrderAccess(caller, order);
    return order;
  }

  /**
   * The orders the caller may see. An {@code ADMIN} gets all of them, or those of {@code
   * customerId} when the request names one. Any other authenticated caller gets their own orders
   * and nothing else: the filter can repeat the caller's own id, and it cannot name another
   * customer.
   *
   * @param customerId the {@code customerId} filter of the request; {@code null} or empty for none
   * @throws AccessRefusedException for a request without an identity, and for a caller who is not
   *     an {@code ADMIN} and asks for another customer's orders
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

  /**
   * Refuses a caller who may not change the order with the given id: the guard of the order
   * commands that act on an existing order. The owner is the customer the order's read-model row
   * names, as for {@link #order}.
   *
   * @throws AccessRefusedException unless the caller is an {@code ADMIN} or that customer
   */
  public void requireOrderAccess(StreamRuneContext.RequestContext caller, String orderId) {
    order(caller, orderId);
  }
}
```

**The Spring side is three small pieces.** First, where the caller comes from: the framework's `ScopedValueFilter` (Step 3) binds the request context around the whole servlet chain. Create `spring-app/src/main/java/org/streamrune/ecommerce/spring/controller/RequestCaller.java`:

```java
package org.streamrune.ecommerce.spring.controller;

import org.streamrune.core.StreamRuneContext;

/**
 * The caller of the request being served: the request context the framework's {@code
 * ScopedValueFilter} bound around the servlet chain. The controllers hand it to {@code
 * OwnerOrAdminAccess} and {@code OwnerOrAdminQueries}, which decide what the caller may do.
 */
final class RequestCaller {

  private RequestCaller() {}

  /** The request context of the current request, or {@code null} when none is bound. */
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

With the handler of Step 6, `GlobalExceptionHandler` now reads:

```java
package org.streamrune.ecommerce.spring.config;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.DomainException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.ecommerce.queries.access.AccessRefusedException;

@RestControllerAdvice
public class GlobalExceptionHandler {

  @ExceptionHandler(DomainException.class)
  public ResponseEntity<String> handleDomainException(DomainException e) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
  }

  /**
   * A command the caller's role does not allow ({@code @RequireRole}, checked in the command bus):
   * {@code 403}, the status {@link #handleAccessRefused} answers with. {@code
   * AuthorizationException} is a {@code DomainException}; this handler is the more specific one, so
   * a refusal is not reported as the {@code 400} of a broken business rule.
   */
  @ExceptionHandler(AuthorizationException.class)
  public ResponseEntity<String> handleAuthorization(AuthorizationException e) {
    return ResponseEntity.status(HttpStatus.FORBIDDEN).body(e.getMessage());
  }

  @ExceptionHandler(IllegalArgumentException.class)
  public ResponseEntity<String> handleIllegalArgument(IllegalArgumentException e) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
  }

  /**
   * Terminal erasure: a command tried to encrypt PII for a crypto-shredded subject (e.g.
   * re-registering a forgotten customer id). The subject was erased under GDPR Article 17, so the
   * write is refused — 410 Gone is the honest status for a resource that was deliberately removed.
   */
  @ExceptionHandler(SubjectForgottenException.class)
  public ResponseEntity<String> handleSubjectForgotten(SubjectForgottenException e) {
    return ResponseEntity.status(HttpStatus.GONE).body(e.getMessage());
  }

  /**
   * The caller may not read or change what the request names ({@code OwnerOrAdminAccess}): {@code
   * 403}, the same answer for a request without an identity and for one with the wrong identity.
   */
  @ExceptionHandler(AccessRefusedException.class)
  public ResponseEntity<String> handleAccessRefused(AccessRefusedException e) {
    return ResponseEntity.status(HttpStatus.FORBIDDEN).body(e.getMessage());
  }
}
```

Third, the bean. `StreamRuneConfig` imports `org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries` and declares:

```java
@Bean
public OwnerOrAdminQueries ownerOrAdminQueries(
    CustomerProjection customerProjection, OrderProjection orderProjection) {
  return new OwnerOrAdminQueries(customerProjection, orderProjection);
}
```

That is the last change to `StreamRuneConfig` in this chapter. The whole class:

<details>
<summary><code>StreamRuneConfig.java</code> at the end of Chapter 9</summary>

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
import org.streamrune.ecommerce.commands.order.OrderDecider;
import org.streamrune.ecommerce.commands.order.OrderPricing;
import org.streamrune.ecommerce.commands.product.ProductDecider;
import org.streamrune.ecommerce.commands.upcaster.ProductCreatedUpcaster;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.projections.*;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
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
        // Order events
        .registerEvent(
            "OrderPlaced", org.streamrune.ecommerce.domain.order.OrderEvent.OrderPlaced.class)
        .registerEvent(
            "OrderConfirmed", org.streamrune.ecommerce.domain.order.OrderEvent.OrderConfirmed.class)
        .registerEvent(
            "OrderShipped", org.streamrune.ecommerce.domain.order.OrderEvent.OrderShipped.class)
        .registerEvent(
            "OrderDelivered", org.streamrune.ecommerce.domain.order.OrderEvent.OrderDelivered.class)
        .registerEvent(
            "OrderCancelled", org.streamrune.ecommerce.domain.order.OrderEvent.OrderCancelled.class)
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
        // Aggregate states, by simple class name: the bus snapshots a state under that name and
        // refuses to write a snapshot for a state the registry does not resolve.
        .registerState("ProductState", ProductState.class)
        .registerState("OrderState", OrderState.class)
        .registerState("CustomerState", CustomerState.class)
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

  @Bean
  public HeaderUserRoleResolver userRoleResolver() {
    return new HeaderUserRoleResolver();
  }

  // Interceptors
  @Bean
  public BeanValidationInterceptor validationInterceptor() {
    var factory = jakarta.validation.Validation.buildDefaultValidatorFactory();
    return new BeanValidationInterceptor(factory.getValidator());
  }

  @Bean
  public AnnotationAuthorizationInterceptor authInterceptor(HeaderUserRoleResolver resolver) {
    return new AnnotationAuthorizationInterceptor(resolver);
  }

  @Bean
  public VirtualThreadCommandBus commandBus(
      EventStore store,
      BeanValidationInterceptor validation,
      AnnotationAuthorizationInterceptor auth) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .interceptors(auth, validation)
        .snapshotPolicy(SnapshotPolicy.everyNEvents(5))
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
            OrderState.TYPE,
            OrderCommand.class,
            cmd ->
                AggregateId.of(
                    switch (cmd) {
                      case OrderCommand.PlaceOrder c -> c.orderId();
                      case OrderCommand.ConfirmOrder c -> c.orderId();
                      case OrderCommand.ShipOrder c -> c.orderId();
                      case OrderCommand.DeliverOrder c -> c.orderId();
                      case OrderCommand.CancelOrder c -> c.orderId();
                    }),
            new OrderDecider())
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
  public OrderProjection orderProjection(ProjectionRepository repo) {
    return new OrderProjection(repo);
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
      OrderProjection orderProjection,
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
                "orders",
                new CacheAwareProjection(orderProjection, cacheInvalidator),
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

  /**
   * The customer and order read models as one caller may see them: an authenticated {@code ADMIN}
   * or the owner, the rule of {@code OwnerOrAdminAccess}. The customer and order controllers answer
   * every read from it.
   */
  @Bean
  public OwnerOrAdminQueries ownerOrAdminQueries(
      CustomerProjection customerProjection, OrderProjection orderProjection) {
    return new OwnerOrAdminQueries(customerProjection, orderProjection);
  }

  /** Prices every order from the product catalogue, as the product streams hold it. */
  @Bean
  public OrderPricing orderPricing(EventStore eventStore) {
    return new OrderPricing(eventStore);
  }
}
```

</details>

**Now the controllers ask.** The two query controllers hand every read to `OwnerOrAdminQueries`. Replace `CustomerQueryController` from Chapter 6:

```java
package org.streamrune.ecommerce.spring.controller;

import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.dto.CustomerView;

/**
 * The customer read model, for an authenticated {@code ADMIN} or the customer themself ({@link
 * OwnerOrAdminQueries}); {@code 403} for everyone else.
 */
@RestController
@RequestMapping("/api/customers")
public class CustomerQueryController {

  private final OwnerOrAdminQueries queries;

  public CustomerQueryController(OwnerOrAdminQueries queries) {
    this.queries = queries;
  }

  /** One customer: {@code 404} when the caller may read it and the read model has no row. */
  @GetMapping("/{id}")
  public ResponseEntity<CustomerView> getCustomer(@PathVariable String id) {
    CustomerView customer = queries.customer(RequestCaller.current(), id);
    return customer != null ? ResponseEntity.ok(customer) : ResponseEntity.notFound().build();
  }

  /** Every customer for an {@code ADMIN}; the caller's own record for anyone else. */
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

/**
 * The order read model, for an authenticated {@code ADMIN} or the customer an order names ({@link
 * OwnerOrAdminQueries}); {@code 403} for everyone else.
 */
@RestController
@RequestMapping("/api/orders")
public class OrderQueryController {

  private final OwnerOrAdminQueries queries;

  public OrderQueryController(OwnerOrAdminQueries queries) {
    this.queries = queries;
  }

  /**
   * One order. {@code 404} is an {@code ADMIN}'s answer for an id without a row; a customer gets
   * {@code 403} for it, as for another customer's order.
   */
  @GetMapping("/{id}")
  public ResponseEntity<OrderView> getOrder(@PathVariable String id) {
    OrderView order = queries.order(RequestCaller.current(), id);
    return order != null ? ResponseEntity.ok(order) : ResponseEntity.notFound().build();
  }

  /**
   * Every order for an {@code ADMIN}, narrowed to one customer by {@code customerId}; the caller's
   * own orders for anyone else, who may not name another customer.
   */
  @GetMapping
  public List<OrderView> listOrders(@RequestParam(required = false) String customerId) {
    return queries.orders(RequestCaller.current(), customerId);
  }
}
```

The command controllers ask as their first line, before anything is written. In `CustomerCommandController` every handler names a customer, by the body for a registration and by the path for the rest; the requester of a forget comes from the same rule (`OwnerOrAdminAccess.identityOf`). Replace the controller:

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
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
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

  /** A customer registers themself; an {@code ADMIN} registers anyone ({@code 403} otherwise). */
  @PostMapping
  public ResponseEntity<Void> registerCustomer(@Valid @RequestBody RegisterRequest req) {
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), req.customerId());
    commandBus.execute(
        new CustomerCommand.RegisterCustomer(
            req.customerId(), req.name(), req.email(), req.address(), req.phone()));
    return ResponseEntity.ok().build();
  }

  /** For an ADMIN or the customer themself (403 otherwise). */
  @PutMapping("/{id}")
  public ResponseEntity<Void> updateProfile(
      @PathVariable String id, @RequestBody UpdateProfileRequest req) {
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), id);
    commandBus.execute(
        new CustomerCommand.UpdateProfile(id, req.name(), req.email(), req.address(), req.phone()));
    return ResponseEntity.ok().build();
  }

  /** GDPR Article 20 request, for an ADMIN or the customer themself (403 otherwise). */
  @PostMapping("/{id}/export-data")
  public ResponseEntity<Void> exportData(@PathVariable String id) {
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), id);
    commandBus.execute(new CustomerCommand.RequestDataExport(id));
    return ResponseEntity.ok().build();
  }

  /**
   * GDPR Article 17 right to erasure, for an ADMIN or the customer themself. Three explicit steps,
   * in this order:
   *
   * <ol>
   *   <li>{@link OwnerOrAdminAccess#requireCustomerAccess} answers {@code 403} to anyone else,
   *       before anything is written.
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
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), id);
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
    return OwnerOrAdminAccess.identityOf(RequestCaller.current());
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

`OrderCommandController` takes the `OwnerOrAdminQueries` bean as a second constructor argument (field `queries`). Placing an order names its customer in the body; cancelling names an order, whose owner the read model knows:

```java
package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.commands.order.OrderPricing;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * The order commands. An order is placed and cancelled by an authenticated {@code ADMIN} or by the
 * customer the order names ({@link OwnerOrAdminAccess}); {@code 403} for everyone else. The price
 * of each line comes from the product catalogue ({@link OrderPricing}). The fulfillment saga
 * confirms an order; there is no endpoint for it. Shipping and delivering are for the {@code ADMIN}
 * role, which {@code @RequireRole} on the two commands enforces in the command bus. A shipped or
 * delivered order is not cancelled ({@code 400}).
 */
@RestController
@RequestMapping("/api/orders")
public class OrderCommandController {

  private final VirtualThreadCommandBus commandBus;
  private final OwnerOrAdminQueries queries;
  private final OrderPricing orderPricing;

  public OrderCommandController(
      VirtualThreadCommandBus commandBus, OwnerOrAdminQueries queries, OrderPricing orderPricing) {
    this.commandBus = commandBus;
    this.queries = queries;
    this.orderPricing = orderPricing;
  }

  /**
   * The order's customer is the caller, unless the caller is an {@code ADMIN}. A product nobody
   * created, or one that is discontinued, answers {@code 400}.
   */
  @PostMapping
  public ResponseEntity<Void> placeOrder(@Valid @RequestBody PlaceOrderRequest req) {
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), req.customerId());
    commandBus.execute(
        orderPricing.price(
            req.orderId(),
            req.customerId(),
            req.lines().stream()
                .map(l -> new OrderPricing.RequestedLine(l.productId(), l.quantity()))
                .toList()));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/{id}/ship")
  public ResponseEntity<Void> shipOrder(@PathVariable String id) {
    commandBus.execute(new OrderCommand.ShipOrder(id));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/{id}/deliver")
  public ResponseEntity<Void> deliverOrder(@PathVariable String id) {
    commandBus.execute(new OrderCommand.DeliverOrder(id));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/{id}/cancel")
  public ResponseEntity<Void> cancelOrder(
      @PathVariable String id, @Valid @RequestBody CancelOrderRequest req) {
    queries.requireOrderAccess(RequestCaller.current(), id);
    commandBus.execute(new OrderCommand.CancelOrder(id, req.reason()));
    return ResponseEntity.ok().build();
  }

  public record PlaceOrderRequest(
      @NotBlank String orderId,
      @NotBlank String customerId,
      @NotEmpty List<@Valid OrderLineRequest> lines) {}

  /** One line as a client asks for it: a product and a quantity; the catalogue sets the price. */
  public record OrderLineRequest(@NotBlank String productId, @Positive int quantity) {}

  public record CancelOrderRequest(String reason) {}
}
```

`ShipOrder` and `DeliverOrder` keep their `@RequireRole("ADMIN")` from Step 1: they are an operator's commands whoever the order belongs to.

**The catalogue's writes are an operator's.** Chapter 2 left `POST /api/products`, `PUT /api/products/{id}/stock` and `PUT /api/products/{id}/price` open, and anyone who can reach the API could set a price of one cent. These routes name no customer, so there is no owner to compare: they are for an `ADMIN`. Each write handler of `ProductCommandController` asks as its first line:

```java
package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * The product commands: the catalogue is the operators', so every write is for an {@code ADMIN}
 * ({@code 403} for everyone else). Creating a product, adjusting its stock and changing its price
 * ask {@link OwnerOrAdminAccess#requireAdmin} as their first line; discontinuing is guarded in the
 * command bus by {@code @RequireRole("ADMIN")} on the command. Reading the catalogue is open
 * ({@code ProductQueryController}).
 */
@RestController
@RequestMapping("/api/products")
public class ProductCommandController {

  private final VirtualThreadCommandBus commandBus;

  public ProductCommandController(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  @PostMapping
  public ResponseEntity<Void> createProduct(@Valid @RequestBody CreateProductRequest req) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    commandBus.execute(
        new ProductCommand.CreateProduct(
            req.productId(),
            req.name(),
            req.description(),
            req.category(),
            new Money(req.price(), "USD"),
            req.initialStock()));
    return ResponseEntity.ok().build();
  }

  @PutMapping("/{id}/stock")
  public ResponseEntity<Void> adjustStock(
      @PathVariable String id, @Valid @RequestBody AdjustStockRequest req) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    commandBus.execute(new ProductCommand.AdjustStock(id, req.quantity(), req.reason()));
    return ResponseEntity.ok().build();
  }

  @PutMapping("/{id}/price")
  public ResponseEntity<Void> updatePrice(
      @PathVariable String id, @Valid @RequestBody UpdatePriceRequest req) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    commandBus.execute(new ProductCommand.UpdatePrice(id, new Money(req.price(), "USD")));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/{id}/discontinue")
  public ResponseEntity<Void> discontinueProduct(@PathVariable String id) {
    commandBus.execute(new ProductCommand.DiscontinueProduct(id));
    return ResponseEntity.ok().build();
  }

  public record CreateProductRequest(
      @NotBlank String productId,
      @NotBlank String name,
      String description,
      String category,
      @NotNull @Positive BigDecimal price,
      int initialStock) {}

  public record AdjustStockRequest(int quantity, String reason) {}

  public record UpdatePriceRequest(@NotNull @Positive BigDecimal price) {}
}
```

`DiscontinueProduct` keeps its `@RequireRole("ADMIN")` from Step 1, and both refusals are a `403`. The two mechanisms guard different doors. `@RequireRole` is declared on the command and enforced in the command bus, for every caller of the bus. The controller line guards the HTTP route and leaves the bus to the callers that have no request: the seed data of Chapter 17 creates its products through the command bus at startup, with no caller at all. Reading the catalogue stays open: `ProductQueryController` asks nobody. Chapter 11 gives the inventory's one write, receiving a shipment, the same first line.

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
| `POST /api/orders/{id}/cancel` | `403` | `403` | `200` | `200` |

The endpoints that are for an `ADMIN` alone have no owner. Their four callers are a request without any header, the role without an identity, an authenticated customer, and an `ADMIN`:

| Endpoint | No header | `X-User-Role: ADMIN`, no `X-User-Id` | A customer | `ADMIN` |
|----------|-----------|--------------------------------------|------------|---------|
| `POST /api/products`, `PUT /api/products/{id}/stock`, `PUT …/price` | `403` | `403` | `403` | `200` |
| `POST /api/products/{id}/discontinue`, `POST /api/orders/{id}/ship`, `…/deliver` (`@RequireRole`) | `403` | `403` | `403` | `200` |
| `GET /api/products`, `GET /api/products/{id}` | `200` | `200` | `200` | `200` |

Five decisions are worth a second look:

- **A request is read before the caller is asked.** `@Valid @RequestBody` runs before the handler, so a request without a body, or without a field its request record requires, is a `400` for anyone, also for a caller the rule would refuse. The handler's first line then decides on a request it can read: a well-formed request from a refused caller is a `403`. Inside the command bus the order is the other one (Step 5): authorization runs before Bean Validation of the command, so the command's own constraints and the decider's business rules stay out of a refused caller's reach.
- **A refusal is `403`, never `404`, and the refusal says nothing about existence.** For a customer the rule compares two ids and never asks the read model, so another customer's record and a record that does not exist are refused alike. For an order a caller who is not an `ADMIN` gets the same `403` for another customer's order and for an id without a row: an order without a row has no known owner. `404` is kept for the two callers who may know — the customer for their own record (a forgotten customer has no row, Chapter 7), and an `ADMIN`. A request without an identity is refused with `403` as well; the demo has no login to send a `401` to. One consequence to know: the read model follows the event store by up to the projection's polling interval (five seconds by default), so right after `POST /api/orders` the order has no row yet and its own customer is answered `403` too, until the row is there. The customer's list never refuses them: it simply does not hold the new order for that moment.
- **A list is scoped where it is read.** A customer's `GET /api/customers` is one read by their own id, and their `GET /api/orders` is `OrderProjection.listByCustomer` with their own id (Chapter 8), never `listAll()`: the rows are chosen inside the read model's class, before the controller sees one, not picked out of everybody's in the controller. (`listByCustomer` itself reads the order table and keeps that customer's rows in memory, which a demo's table allows; a read model with real volumes gives its repository a query by `customerId`.) No query parameter widens the result: `customerId` may repeat the caller's id, and naming anybody else is a `403`.
- **An order belongs to the customer it names, and only that customer or an `ADMIN` may place it.** `POST /api/orders` takes `customerId` from the request body, so the check ties it to the caller.
- **What is open names nobody.** A caller without headers reaches the public catalogue (`GET /api/products`), the health and metrics answers of the apps (Chapter 15), the health answer of the notifications service (Chapter 13), and the live event feed of Chapter 15 (`GET /api/events/sse`). The feed is open to everyone, because a browser's `EventSource` cannot send the two headers. It therefore says only what a caller without an identity may know: that an event of a type happened to an aggregate of a type, and when. A frame carries no stream id, no aggregate id and no payload, so nobody reads `customer:c-carol` or `order:order-acl-1` off it, and a connection is sent the events stored while it is open, never the store's history. What an anonymous caller can make the app do is bounded too: one reader serves all the connections, and the feed takes 100 of them at once. The notifications service of Chapter 13 holds the bodies of the published integration events, which name orders; its `GET /received` answers an `ADMIN` and its open endpoint, `GET /health`, carries no body. One answer does tell a caller that an id is taken: `POST /api/orders` answers `400 Order already exists` to a customer who places an order of their own under an id in use. What `403` keeps from a refused caller is the record, the order and the events' payloads.

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

The catalogue: anyone reads it, an `ADMIN` changes it.

```bash
curl -s http://localhost:8080/api/products/p-1

# A customer, and the role without an identity: 403
curl -i -X PUT http://localhost:8080/api/products/p-1/price \
  -H 'Content-Type: application/json' -H 'X-User-Id: c-carol' -H 'X-User-Role: CUSTOMER' \
  -d '{"price": 0.01}'
curl -i -X PUT http://localhost:8080/api/products/p-1/price \
  -H 'Content-Type: application/json' -H 'X-User-Role: ADMIN' \
  -d '{"price": 0.01}'
# → 403 Forbidden

# An ADMIN
curl -i -X PUT http://localhost:8080/api/products/p-1/price \
  -H 'Content-Type: application/json' -H 'X-User-Id: admin-1' -H 'X-User-Role: ADMIN' \
  -d '{"price": 21.99}'
# → 200 OK
```

Orders follow the same lines:

```bash
# Dave may not place an order in Carol's name
curl -i -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' -H 'X-User-Id: c-dave' -H 'X-User-Role: CUSTOMER' \
  -d '{"orderId":"order-acl-1","customerId":"c-carol","lines":[{"productId":"p-1","quantity":1}]}'
# → 403 Forbidden

# Carol places it herself and reads it; Dave gets 403 for it, and his own list is empty
curl -s -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' -H 'X-User-Id: c-carol' -H 'X-User-Role: CUSTOMER' \
  -d '{"orderId":"order-acl-1","customerId":"c-carol","lines":[{"productId":"p-1","quantity":1}]}'
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

> **Quarkus and Micronaut.** Both apps use the same `OwnerOrAdminAccess` and `OwnerOrAdminQueries` and answer the two tables above status for status. Each has its own small pieces: a `RequestCaller` that says where its integration keeps the request context (the request-scoped `StreamRuneRequestContextHolder` on Quarkus, the `ScopedValue` or the `StreamRuneContextHelper` thread-local on Micronaut), a mapper from `AccessRefusedException` and one from `AuthorizationException` to `403` (`AccessRefusedExceptionMapper` and `AuthorizationExceptionMapper`, `AccessRefusedExceptionHandler` and `AuthorizationExceptionHandler`), and the `OwnerOrAdminQueries` bean. Neither app uses Bean Validation on its request records, so each has a `RequestChecks` class with the same constraints written out, and a handler runs them before it asks the rule: a malformed request is a `400` in all three apps before the caller is decided. Each of the three apps has an `OwnerOrAdminRestAccessIT` that walks the tables over HTTP with their callers and asserts exactly which rows a list returns; the rule itself is unit-tested as a table in `queries` (`OwnerOrAdminAccessTest`), and the rows of the lists in `projections` (`OwnerOrAdminQueriesTest`).

---

## What We Learned

- **`@RequireRole`** is a runtime-retained annotation you place on a command record to declare that the caller must hold at least one of the listed roles. When multiple roles are listed, any single match suffices (OR). The annotation lives in the `domain` module with no infrastructure imports.

- **`@RequirePermission`** works identically but checks fine-grained permissions rather than broad roles. You can combine both on one command: when both annotations are present, the caller must satisfy both (AND).

- **`UserRoleResolver`** is a `@FunctionalInterface` that maps a `UserId` to a `UserAuthority`. You implement it and register it as a bean; you own the data source. A resolver that reads the request, like the demo's, also overrides `requiresRequestContext()` to return `true`: the framework then calls it once at the request edge and never on a dead-letter replay, which runs an already-authorized command under the dead-letter-replay system principal instead.

- **`AnnotationAuthorizationInterceptor`** is a `CommandInterceptor` from `streamrune-runtime`. It reads both annotations from the command class via reflection, checks them against the caller's authority (the one captured at the request edge for a resolver that declares `requiresRequestContext()`, otherwise the answer of `UserRoleResolver.resolve()`), and throws `AuthorizationException` if the requirement is not met. If neither annotation is present it is a pure no-op with effectively zero overhead.

- **`ScopedValue`** (finalized in Java 25; preview in Java 21–24) replaces `ThreadLocal` for propagating request context through virtual threads. The framework's `ScopedValueFilter` (auto-registered by `StreamRuneAutoConfiguration`) wraps each request in `ScopedValue.where(StreamRuneContext.CURRENT, ctx).call(...)`, making the context available to all code that runs in that scope, including code dispatched onto virtual threads by the `CommandBus`. In the trusted-gateway mode the filter also reads `X-User-Role` and places it into the baggage map under the key `"role"`, which `HeaderUserRoleResolver` reads; in any other mode it ignores that header.

- **Identity and role in the demo are stand-ins.** The apps run in the trusted-gateway mode (`streamrune.security.trust-user-id-header: true`), so `X-User-Id` is the identity — acceptable only because the demo stands in for a gateway that would authenticate callers and set the header itself. `HeaderUserRoleResolver` goes further and authorizes on `X-User-Role`. The framework reads that header only in the trusted-gateway mode, where a real gateway would set it; in the demo nothing authenticates it: a demo shortcut a real deployment must replace with a resolver that derives the authority from the authenticated identity.

- **Ownership is checked where the id is known.** `@RequireRole` decides by the command's type; "an `ADMIN`, or the customer themself" depends on the id the request names. The demo writes that rule once (`OwnerOrAdminAccess`), asks it in every customer and order route — reads through `OwnerOrAdminQueries`, which also scopes a list to the caller's own rows — and answers `403` before anything is read or written. A refusal never tells a caller whether the thing exists. What has no owner is for an `ADMIN` alone, the role together with an identity: the product writes ask `OwnerOrAdminAccess.requireAdmin`.

- **Interceptor ordering** matters: authorization runs before validation, so an unauthorized caller cannot probe the validation rules. In the next chapter audit goes in front of authorization, so a refused command is still recorded.

---

## Next Up

We can control who does what. Next, let's add an audit trail.
