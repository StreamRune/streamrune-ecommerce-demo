package org.streamrune.ecommerce.spring;

import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps the spans the application ends, for {@link PlaceOrderTraceIT}. Spring Boot hands every
 * {@code SpanProcessor} bean to the tracer provider of its OpenTelemetry SDK, the one the
 * framework's {@code StreamRuneTracing} traces through. The suite shares one application context,
 * so this processor sees the spans of every test class: it keeps the latest {@value #CAPACITY} and
 * drops the oldest.
 */
public class RecordedSpans implements SpanProcessor {

  static final int CAPACITY = 20_000;

  private final ConcurrentLinkedDeque<SpanData> spans = new ConcurrentLinkedDeque<>();

  private final AtomicInteger size = new AtomicInteger();

  /** The spans ended since the last {@link #clear()}, oldest first. */
  public List<SpanData> all() {
    return List.copyOf(spans);
  }

  /** Forgets every span ended so far. */
  public void clear() {
    spans.clear();
    size.set(0);
  }

  @Override
  public void onStart(Context parentContext, ReadWriteSpan span) {}

  @Override
  public boolean isStartRequired() {
    return false;
  }

  @Override
  public void onEnd(ReadableSpan span) {
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
