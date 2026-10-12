package org.streamrune.ecommerce.quarkus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * One client of the open live feed, {@code GET /api/events/sse}, over real HTTP, read on a virtual
 * thread. It keeps every block of lines the feed sent as it arrived: the comment that opens the
 * stream, and then the frames.
 */
final class LiveFeedTestClient implements AutoCloseable {

  private static final ObjectMapper JSON = new ObjectMapper();

  /** The comment the feed sends once the connection stands at the head of the global stream. */
  private static final String CONNECTED_COMMENT = ": connected";

  private final HttpClient http = HttpClient.newHttpClient();
  private final List<List<String>> frames = new CopyOnWriteArrayList<>();
  private final CompletableFuture<Void> connected = new CompletableFuture<>();
  private final CompletableFuture<Void> ended = new CompletableFuture<>();

  /**
   * Sends the request; the answer is read in the background.
   *
   * @param server the base URI of the running application
   * @param userId the {@code X-User-Id} header, or {@code null} to send none
   * @param role the {@code X-User-Role} header, or {@code null} to send none
   */
  LiveFeedTestClient(URI server, String userId, String role) {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(server.resolve("/api/events/sse"))
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
      if (response.statusCode() != 200) {
        connected.completeExceptionally(
            new AssertionError("the feed answered " + response.statusCode()));
        response.body().close();
        return;
      }
      List<String> block = new ArrayList<>();
      try (Stream<String> lines = response.body()) {
        for (String line : (Iterable<String>) lines::iterator) {
          if (!line.isEmpty()) {
            block.add(line);
            continue;
          }
          if (block.contains(CONNECTED_COMMENT)) {
            connected.complete(null);
          } else if (block.stream().anyMatch(l -> l.startsWith("data:"))) {
            frames.add(List.copyOf(block));
          }
          block.clear();
        }
      }
      ended.complete(null);
    } catch (IOException | UncheckedIOException e) {
      // The test closed the stream, or it never opened: awaitConnected() reports the latter.
      connected.completeExceptionally(e);
      ended.complete(null);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      connected.completeExceptionally(e);
    }
  }

  /**
   * Returns once the client has read the feed's opening comment. The feed sends it when the
   * connection stands at the head of the global stream, so an event stored from then on reaches
   * this client.
   */
  void awaitConnected() throws Exception {
    connected.get(15, TimeUnit.SECONDS);
  }

  /** Returns once the server has ended the stream. */
  void awaitEnded(Duration atMost) throws Exception {
    ended.get(atMost.toMillis(), TimeUnit.MILLISECONDS);
  }

  /** The lines of every frame read so far, in the order the feed sent them. */
  List<List<String>> frames() {
    return frames;
  }

  /** The global offsets of the frames read so far, in the order the feed sent them. */
  List<Long> offsets() throws Exception {
    List<Long> offsets = new ArrayList<>();
    for (List<String> frame : frames) {
      offsets.add(data(frame).path("globalOffset").asLong());
    }
    return offsets;
  }

  /** The frame's data line as JSON. */
  static JsonNode data(List<String> frame) throws Exception {
    String data =
        frame.stream().filter(l -> l.startsWith("data:")).findFirst().orElseThrow().substring(5);
    return JSON.readTree(data);
  }

  /** Closes the connection, as a browser that leaves the page does. */
  @Override
  public void close() {
    http.shutdownNow();
  }
}
