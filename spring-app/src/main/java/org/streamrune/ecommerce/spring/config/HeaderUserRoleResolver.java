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
