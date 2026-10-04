package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
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

  /** GDPR Article 20 request, for an ADMIN or the customer themself (403 otherwise). */
  @PostMapping("/{id}/export-data")
  public ResponseEntity<Void> exportData(@PathVariable String id) {
    requireAdminOrSelf(id);
    commandBus.execute(new CustomerCommand.RequestDataExport(id));
    return ResponseEntity.ok().build();
  }

  /**
   * GDPR Article 17 right to erasure, for an ADMIN or the customer themself. Three explicit steps,
   * in this order:
   *
   * <ol>
   *   <li>{@link #requireAdminOrSelf} answers {@code 403} to anyone else, before anything is
   *       written.
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
    requireAdminOrSelf(id);
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
   * Answers {@code 403} unless the caller holds the ADMIN role or is the customer themself (the
   * request identity equals the customer id). DEMO SHORTCUT, as in {@code AdminController}: the
   * role and the identity are what the trusted-gateway headers say (ch. 9); a real deployment
   * authenticates the caller.
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

  /**
   * Requester id for the GDPR audit trail, read from the request-scoped context; null if unbound.
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
