package org.streamrune.ecommerce.micronaut.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneContext.RequestContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.RequestEdgeAuthority;

/**
 * The demo's role resolver reads the request (the {@code role} baggage entry the filter copies from
 * {@code X-User-Role}), so it declares {@code requiresRequestContext()}. The request filter then
 * resolves it once, at the request edge, with the request's context bound — the capture every
 * authz-gated command of that request is decided against.
 */
class HeaderUserRoleResolverTest {

  private static RequestContext requestWithRole(String role) {
    return new RequestContext(
        null,
        UserId.of("admin-1"),
        CorrelationId.of("corr-resolver-test"),
        Instant.now(),
        Map.of("role", role));
  }

  private static UserAuthority captureAtTheRequestEdge(RequestContext request) {
    RequestContext captured =
        RequestEdgeAuthority.capture(
            request,
            new HeaderUserRoleResolver(),
            e -> {
              throw new AssertionError("the resolver must not fail at the request edge", e);
            });
    return captured.authority();
  }

  @Test
  void declaresThatItReadsTheRequest() {
    assertThat(new HeaderUserRoleResolver().requiresRequestContext()).isTrue();
  }

  @Test
  void anAdminRequestIsCapturedWithTheAdminRoleAndItsPermissions() {
    UserAuthority authority = captureAtTheRequestEdge(requestWithRole("ADMIN"));

    assertThat(authority).as("the request edge captured an authority").isNotNull();
    assertThat(authority.roles()).containsExactly("ADMIN");
    assertThat(authority.hasAnyPermission("ORDER_SHIP", "PRODUCT_MANAGE")).isTrue();
  }

  @Test
  void aCustomerRequestIsCapturedWithoutTheAdminRole() {
    UserAuthority authority = captureAtTheRequestEdge(requestWithRole("CUSTOMER"));

    assertThat(authority).isNotNull();
    assertThat(authority.hasAnyRole("ADMIN")).isFalse();
    assertThat(authority.roles()).containsExactly("CUSTOMER");
  }

  @Test
  void withNoRequestBoundTheAnswerIsGuestWithNoPermissions() {
    UserAuthority authority = new HeaderUserRoleResolver().resolve(UserId.of("admin-1"));

    assertThat(authority.roles()).containsExactly("GUEST");
    assertThat(authority.permissions()).isEmpty();
  }
}
