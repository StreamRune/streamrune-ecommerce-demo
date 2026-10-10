package org.streamrune.ecommerce.quarkus;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * One client of {@code GET /api/sse/{aggregateType}/{aggregateId}} over real HTTP, read on a
 * virtual thread. The request names its caller the way every request of the demo does: the {@code
 * X-User-Id} and {@code X-User-Role} headers of the gateway the demo stands in for.
 */
final class SseTestStream implements AutoCloseable {

  /** One frame of an SSE stream: its {@code id} and {@code data} fields. */
  record Frame(String id, String data) {}

  /**
   * The line every stream of the endpoint opens with, and the one it repeats as its keepalive: a
   * comment, which an SSE client ignores.
   */
  private static final String OPENING_COMMENT = ": keepalive";

  private final HttpClient http = HttpClient.newHttpClient();
  private final List<Frame> frames = new CopyOnWriteArrayList<>();
  private final CompletableFuture<Integer> status = new CompletableFuture<>();
  private final CompletableFuture<Void> subscribed = new CompletableFuture<>();

  /**
   * Sends the request; the answer is read in the background.
   *
   * @param server the base URI of the running application
   * @param aggregateType the first path segment of the stream
   * @param aggregateId the second path segment of the stream
   * @param userId the {@code X-User-Id} header, or {@code null} to send none
   * @param role the {@code X-User-Role} header, or {@code null} to send none
   */
  SseTestStream(URI server, String aggregateType, String aggregateId, String userId, String role) {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(server.resolve("/api/sse/" + aggregateType + "/" + aggregateId))
            .header("Accept", "text/event-stream")
            .GET();
    if (userId != null) {
      request.header("X-User-Id", userId);
    }
    if (role != null) {
      request.header("X-User-Role", role);
    }
    HttpRequest built = request.build();
    Thread.ofVirtual().start(() -> read(built));
  }

  private void read(HttpRequest request) {
    try {
      HttpResponse<Stream<String>> response =
          http.send(request, HttpResponse.BodyHandlers.ofLines());
      status.complete(response.statusCode());
      if (response.statusCode() != 200) {
        subscribed.completeExceptionally(
            new AssertionError("the stream answered " + response.statusCode()));
        response.body().close();
        return;
      }
      String id = null;
      String data = null;
      try (Stream<String> lines = response.body()) {
        for (String line : (Iterable<String>) lines::iterator) {
          if (line.equals(OPENING_COMMENT)) {
            subscribed.complete(null);
          }
          if (line.startsWith("id:")) {
            id = line.substring(3).trim();
          } else if (line.startsWith("data:")) {
            data = line.substring(5).trim();
          } else if (line.isEmpty()) {
            if (data != null) {
              frames.add(new Frame(id, data));
            }
            id = null;
            data = null;
          }
        }
      }
    } catch (IOException | UncheckedIOException e) {
      // The test closed the stream, or it never opened: status() and awaitSubscribed() report the
      // latter.
      status.completeExceptionally(e);
      subscribed.completeExceptionally(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      status.completeExceptionally(e);
      subscribed.completeExceptionally(e);
    }
  }

  /** The HTTP status the endpoint answered with. */
  int status() throws Exception {
    return status.get(15, TimeUnit.SECONDS);
  }

  /**
   * Returns once the client has read the stream's opening comment. The endpoint writes that comment
   * as the last step of a subscription, whatever {@code streamrune.sse.keep-alive-interval} is, so
   * an event stored from then on reaches this client. The applications run their tests with the
   * default interval of 30 seconds, twice this wait: a stream that opened without the comment would
   * fail here.
   */
  void awaitSubscribed() throws Exception {
    subscribed.get(15, TimeUnit.SECONDS);
  }

  /** The frames read so far. */
  List<Frame> frames() {
    return frames;
  }

  @Override
  public void close() {
    http.shutdownNow();
  }
}
