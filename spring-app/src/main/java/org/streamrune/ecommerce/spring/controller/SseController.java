package org.streamrune.ecommerce.spring.controller;

import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.SseEventPublisher;
import org.streamrune.spring.ScopedValueFilter;

/**
 * Live Server-Sent Events endpoint for the demo: streams every event appended to a given aggregate
 * stream to subscribed browser clients via the {@link SseEventPublisher} fed by the {@code
 * sse-fanout} subscription wired in {@code StreamRuneConfig}.
 *
 * <p>Endpoint: {@code GET /api/sse/{aggregateType}/{aggregateId}} (media type {@code
 * text/event-stream}). Each frame's {@code data} is the domain event serialized as JSON and its
 * {@code id} is the global offset.
 *
 * <p><b>Why this subclasses the framework controller.</b> {@code StreamRuneAutoConfiguration} ships
 * its own {@code org.streamrune.spring.SseController} guarded by
 * {@code @ConditionalOnMissingBean(SseController.class)}. Extending that type makes this bean
 * satisfy the condition, so the framework's controller backs off and there is no ambiguous {@code
 * /api/sse/{aggregateType}/{aggregateId}} mapping. We override {@code stream} only to bind the path
 * variables explicitly ({@code @PathVariable("aggregateType")},
 * {@code @PathVariable("aggregateId")}): the shipped {@code streamrune-spring} jar is compiled
 * without javac's {@code -parameters} flag, so Spring MVC cannot infer the variable name from the
 * framework method and rejects every request with 400.
 *
 * <p><b>Authorization.</b> The framework controller requires an {@link SseAuthorizer} to gate
 * access before subscribing a caller. This override replicates that check explicitly (see {@link
 * StreamRuneConfig#sseAuthorizer}) since it does not delegate to {@code super.stream(...)}.
 *
 * <p><b>The caller.</b> The caller handed to the authorizer is resolved by the framework's {@link
 * RequestIdentityPolicy} bean — the same rule the framework's {@code ScopedValueFilter} binds
 * {@code RequestContext.userId} with — so a subscription and a command from the same request see
 * the same user. The demo runs in the trusted-gateway mode ({@code
 * streamrune.security.trust-user-id-header: true} in {@code application.yml}), so that user is the
 * {@code X-User-Id} header. The header is read with {@link ScopedValueFilter#userIdHeaderValues},
 * every value as received, exactly as the filter reads it: a request that repeats {@code X-User-Id}
 * is anonymous at both, rather than one caller here and another there.
 */
@RestController
@RequestMapping("/api/sse")
@Primary
public class SseController extends org.streamrune.spring.SseController {

  private final SseEventPublisher publisher;
  private final SseAuthorizer authorizer;
  private final RequestIdentityPolicy identityPolicy;

  public SseController(
      SseEventPublisher publisher, SseAuthorizer authorizer, RequestIdentityPolicy identityPolicy) {
    super(publisher, authorizer, identityPolicy);
    this.publisher = publisher;
    this.authorizer = authorizer;
    this.identityPolicy = identityPolicy;
  }

  @Override
  @GetMapping(
      value = "/{aggregateType}/{aggregateId}",
      produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public SseEmitter stream(
      @PathVariable("aggregateType") String aggregateType,
      @PathVariable("aggregateId") String aggregateId,
      HttpServletRequest request) {
    StreamId sid;
    try {
      sid = StreamId.of(AggregateType.of(aggregateType), AggregateId.of(aggregateId));
    } catch (IllegalArgumentException e) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "Invalid aggregate type or aggregate id");
    }
    // The mismatch listener only fires in the authenticated-principal mode, which this demo (a
    // trusted-gateway deployment without real authentication) never runs in.
    UserId principal =
        identityPolicy.resolve(
            ScopedValueFilter.userIdHeaderValues(request), (claimed, authenticated) -> {});
    if (!authorizer.isAuthorized(principal, sid)) {
      throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Not authorized for this stream");
    }

    SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);

    // Flush the response (status + headers) immediately with a comment frame, so clients — and the
    // E2E test's HTTP client — see the open 200 stream right away instead of blocking until the
    // first domain event. Comment lines (":...") are ignored by SSE parsers, so they carry no data.
    try {
      emitter.send(SseEmitter.event().comment("connected"));
    } catch (IOException e) {
      emitter.completeWithError(e);
      return emitter;
    }

    SseEventPublisher.SseSubscriber subscriber =
        envelope -> {
          try {
            emitter.send(
                SseEmitter.event()
                    .data(envelope.event())
                    .id(String.valueOf(envelope.globalOffset().value()))
                    .build());
          } catch (IOException e) {
            emitter.completeWithError(e);
          }
        };
    publisher.subscribe(sid, subscriber);

    emitter.onCompletion(() -> publisher.unsubscribe(sid, subscriber));
    emitter.onTimeout(() -> publisher.unsubscribe(sid, subscriber));

    return emitter;
  }
}
