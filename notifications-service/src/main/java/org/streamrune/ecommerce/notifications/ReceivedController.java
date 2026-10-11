package org.streamrune.ecommerce.notifications;

import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * The service's two HTTP endpoints: what it received, for an operator, and whether it runs, for a
 * probe.
 *
 * <p><b>{@code GET /received} is ADMIN only.</b> It returns the body of every integration event the
 * service holds, and a body names an order, a payment or a product and can carry the reason a
 * customer typed when cancelling. The rule is the e-shop's definition of an {@code ADMIN}: the
 * {@code ADMIN} role together with an identity. Everyone else gets {@code 403}, a caller without
 * {@code X-User-Id} first of all.
 *
 * <p><b>DEMO SHORTCUT, the gateway stand-in the tutorial's chapter 9 describes.</b> The identity
 * and the role are whatever the client sent in {@code X-User-Id} and {@code X-User-Role}. The
 * service shares no code with the e-shop, so it reads the two headers itself. A real deployment
 * authenticates the caller and derives the role from that identity.
 *
 * <p><b>{@code GET /health} is open and carries no body.</b> A {@code 200} says the service runs; a
 * probe needs no headers and learns nothing else.
 */
@RestController
public class ReceivedController {

  private static final String ADMIN = "ADMIN";

  private final IntegrationEventListener listener;

  public ReceivedController(IntegrationEventListener listener) {
    this.listener = listener;
  }

  @GetMapping("/received")
  public List<IntegrationEventListener.Received> received(
      @RequestHeader(value = "X-User-Id", required = false) String userId,
      @RequestHeader(value = "X-User-Role", required = false) String role) {
    if (!isAdmin(userId, role)) {
      throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Required role: ADMIN");
    }
    return listener.recent();
  }

  @GetMapping("/health")
  public ResponseEntity<Void> health() {
    return ResponseEntity.ok().build();
  }

  /** An {@code ADMIN} is the role, spelled exactly, together with an identity. */
  private static boolean isAdmin(String userId, String role) {
    return userId != null && !userId.isBlank() && ADMIN.equals(role);
  }
}
