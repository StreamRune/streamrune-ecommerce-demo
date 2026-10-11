package org.streamrune.ecommerce.micronaut.controller;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Put;
import io.micronaut.serde.annotation.Serdeable;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.runtime.gdpr.ForgetResult;
import org.streamrune.runtime.gdpr.ForgetSubjectService;

@Controller("/api/customers")
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
  @Post
  public HttpResponse<Void> registerCustomer(@Body RegisterRequest req) {
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), req.customerId());
    commandBus.execute(
        new CustomerCommand.RegisterCustomer(
            req.customerId(), req.name(), req.email(), req.address(), req.phone()));
    return HttpResponse.ok();
  }

  /** For an ADMIN or the customer themself (403 otherwise). */
  @Put("/{id}")
  public HttpResponse<Void> updateProfile(@PathVariable String id, @Body UpdateProfileRequest req) {
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), id);
    commandBus.execute(
        new CustomerCommand.UpdateProfile(id, req.name(), req.email(), req.address(), req.phone()));
    return HttpResponse.ok();
  }

  /** GDPR Article 20 request, for an ADMIN or the customer themself (403 otherwise). */
  @Post("/{id}/export-data")
  public HttpResponse<Void> exportData(@PathVariable String id) {
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), id);
    commandBus.execute(new CustomerCommand.RequestDataExport(id));
    return HttpResponse.ok();
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
  @Post("/{id}/forget")
  public HttpResponse<ForgetResponse> forgetCustomer(@PathVariable String id) {
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
      return HttpResponse.<ForgetResponse>serverError().body(ForgetResponse.keyNotDeleted());
    }
    ForgetResponse body = ForgetResponse.of(result);
    return result.fullyErased()
        ? HttpResponse.ok(body)
        : HttpResponse.<ForgetResponse>serverError().body(body);
  }

  /**
   * What a forget did. {@code keyDeleted}: the subject's encryption key is gone. {@code
   * fullyErased}: the key is gone and every read-model purger succeeded. {@code failedPurgers}: the
   * read models that may still hold the customer's data. Written even when empty, as in the Spring
   * and Quarkus apps: micronaut-serde leaves an empty list out unless told otherwise.
   */
  @Serdeable
  @JsonInclude(JsonInclude.Include.ALWAYS)
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

  @Serdeable
  public record RegisterRequest(
      String customerId, String name, String email, String address, String phone) {}

  @Serdeable
  public record UpdateProfileRequest(String name, String email, String address, String phone) {}
}
