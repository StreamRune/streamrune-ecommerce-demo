package org.streamrune.ecommerce.quarkus;

import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps the spans the application ends, for {@link PlaceOrderTraceIT}. quarkus-opentelemetry adds
 * every {@code SpanProcessor} bean to the tracer provider of its SDK, the one the framework's
 * {@code StreamRuneTracing} traces through, once the SDK is on ({@code
 * quarkus.otel.sdk.disabled=false}, which only that test's profile sets). It keeps the latest
 * {@value #CAPACITY} spans and drops the oldest. It records nothing until a test calls {@link
 * #clear()}, so the spans of the other tests are not converted.
 */
@Singleton
public class RecordedSpans implements SpanProcessor {

  static final int CAPACITY = 20_000;

  private final ConcurrentLinkedDeque<SpanData> spans = new ConcurrentLinkedDeque<>();

  private final AtomicInteger size = new AtomicInteger();

  /** Whether a test has called {@link #clear()}: until then the spans are not kept. */
  private volatile boolean recording;

  /** The spans ended since the last {@link #clear()}, oldest first. */
  public List<SpanData> all() {
    return List.copyOf(spans);
  }

  /** Forgets every span ended so far, and keeps the spans ended from now on. */
  public void clear() {
    spans.clear();
    size.set(0);
    recording = true;
  }

  @Override
  public void onStart(Context parentContext, ReadWriteSpan span) {}

  @Override
  public boolean isStartRequired() {
    return false;
  }

  @Override
  public void onEnd(ReadableSpan span) {
    if (!recording) {
      return;
    }
    spans.addLast(span.toSpanData());
    if (size.incrementAndGet() > CAPACITY && spans.pollFirst() != null) {
      size.decrementAndGet();
    }
  }

  @Override
  public boolean isEndRequired() {
    return true;
  }
}
