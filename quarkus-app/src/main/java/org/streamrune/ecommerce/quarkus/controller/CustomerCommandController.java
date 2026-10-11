package org.streamrune.ecommerce.quarkus.controller;

import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.quarkus.StreamRuneRequestContextHolder;
import org.streamrune.quarkus.StreamRuneRequestFilter;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.runtime.gdpr.ForgetResult;
import org.streamrune.runtime.gdpr.ForgetSubjectService;

@Path("/api/customers")
@Produces(MediaType.APPLICATION_JSON)
public class CustomerCommandController {

  private static final Logger LOG = LoggerFactory.getLogger(CustomerCommandController.class);

  @Inject VirtualThreadCommandBus commandBus;
  @Inject ForgetSubjectService forgetSubjectService;
  @Inject StreamRuneRequestContextHolder requestContext;

  /**
   * Executes a command with the request's context bound to {@code StreamRuneContext.CURRENT}, as
   * {@code OrderCommandController} does: a JAX-RS filter cannot wrap the resource method, so
   * without this bind the command, its audit row and its event metadata carry no user.
   */
  private void executeInContext(CustomerCommand command) {
    StreamRuneRequestFilter.withContext(context(), () -> commandBus.execute(command));
  }

  /** A customer registers themself; an {@code ADMIN} registers anyone ({@code 403} otherwise). */
  // @Consumes is per-method: only the body-carrying endpoints require a JSON content-type. A
  // class-level @Consumes(JSON) would make the bodiless POSTs (/forget, /export-data) reject a
  // content-type-less request with 415.
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response registerCustomer(RegisterRequest req) {
    OwnerOrAdminAccess.requireCustomerAccess(context(), req.customerId());
    executeInContext(
        new CustomerCommand.RegisterCustomer(
            req.customerId(), req.name(), req.email(), req.address(), req.phone()));
    return Response.ok().build();
  }

  /** For an ADMIN or the customer themself (403 otherwise). */
  @PUT
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response updateProfile(@PathParam("id") String id, UpdateProfileRequest req) {
    OwnerOrAdminAccess.requireCustomerAccess(context(), id);
    executeInContext(
        new CustomerCommand.UpdateProfile(id, req.name(), req.email(), req.address(), req.phone()));
    return Response.ok().build();
  }

  /** GDPR Article 20 request, for an ADMIN or the customer themself (403 otherwise). */
  @POST
  @Path("/{id}/export-data")
  public Response exportData(@PathParam("id") String id) {
    OwnerOrAdminAccess.requireCustomerAccess(context(), id);
    executeInContext(new CustomerCommand.RequestDataExport(id));
    return Response.ok().build();
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
   * <p>The requester id for the GDPR audit is taken from the Quarkus-bound request context ({@code
   * X-User-Id}).
   */
  @POST
  @Path("/{id}/forget")
  public Response forgetCustomer(@PathParam("id") String id) {
    OwnerOrAdminAccess.requireCustomerAccess(context(), id);
    executeInContext(new CustomerCommand.ForgetCustomer(id));
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
      return Response.serverError().entity(ForgetResponse.keyNotDeleted()).build();
    }
    ForgetResponse body = ForgetResponse.of(result);
    return (result.fullyErased() ? Response.ok() : Response.serverError()).entity(body).build();
  }

  /**
   * What a forget did. {@code keyDeleted}: the subject's encryption key is gone. {@code
   * fullyErased}: the key is gone and every read-model purger succeeded. {@code failedPurgers}: the
   * read models that may still hold the customer's data. Registered for reflection because the
   * resource returns it inside a {@link Response}, where the native build cannot see its type.
   */
  @RegisterForReflection
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
   * The request context of the current request, from the filter's request-scoped holder (see {@link
   * RequestCaller}).
   */
  private StreamRuneContext.RequestContext context() {
    return RequestCaller.of(requestContext);
  }

  /**
   * Requester id for the GDPR audit trail: the request identity, or {@code null} when none is
   * bound.
   */
  private String currentRequesterId() {
    return OwnerOrAdminAccess.identityOf(context());
  }

  public record RegisterRequest(
      String customerId, String name, String email, String address, String phone) {}

  public record UpdateProfileRequest(String name, String email, String address, String phone) {}
}
