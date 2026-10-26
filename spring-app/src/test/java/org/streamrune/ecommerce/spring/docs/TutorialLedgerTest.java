package org.streamrune.ecommerce.spring.docs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Every step of a chapter works with what the chapters up to it built: each product, customer and
 * order id a request names, each endpoint it calls, each port and each request header it uses was
 * created or introduced in that chapter or an earlier one. A walkthrough that orders a product only
 * a later chapter seeds, or calls an endpoint a later chapter adds, cannot be followed; it fails
 * here.
 *
 * <p><b>What is scanned.</b> The {@code curl} commands of a chapter's shell blocks (continuation
 * lines and multi-line bodies joined) and the {@code http://localhost} addresses of its text and
 * shell blocks, the addresses a reader opens in a browser. A {@code curl} command without an {@code
 * http://} or {@code https://} address the test can read fails, rather than going unchecked; {@code
 * --json} sends its body as {@code -d} does. A request's ids are the values of its path parameters,
 * of its query parameters whose names end in {@code Id}, and of the {@code productId}, {@code
 * customerId} and {@code orderId} fields of its JSON body. A value with a shell variable or a
 * {@code <placeholder>} in it is not an id.
 *
 * <p><b>The ledger</b> ({@link #LEDGER}) says, for each of them, the chapter that introduces it,
 * and how. The test holds the ledger to it: a {@code created} id has a request in its chapter that
 * creates it ({@code POST /api/products}, {@code /api/customers} or {@code /api/orders} with the id
 * in its body), a {@code listed} one is named by a Java listing of its chapter (an aggregate type,
 * a product the seed data creates), a {@code controller} endpoint is mapped by the controllers the
 * reader has at the end of its chapter and not before, and every other entry (an id named on
 * purpose, a framework endpoint, a port, a header) is used in its chapter.
 */
class TutorialLedgerTest {

  private static final Path TUTORIAL = Path.of("../docs/tutorial");

  /** What each chapter introduces that its steps and the later chapters' steps may use. */
  private static final List<Entry> LEDGER =
      List.of(
          // Ports
          port(8080, 1, "the application"),
          port(15672, 13, "the RabbitMQ management UI"),
          port(8090, 13, "the notifications service"),
          port(3000, 16, "the frontend"),
          port(8091, 17, "the second backend of the two-backends profile"),
          // Request headers
          header("X-User-Id", 9, "the trusted-gateway mode switches the request identity on"),
          header("X-User-Role", 9, "the role resolver reads it"),
          header("X-Correlation-Id", 11, "the saga's correlation id"),
          header("Idempotency-Key", 14, "the keyed stock adjustment"),
          // Endpoints the framework, Spring Boot or a service of the stack serves
          framework("GET /actuator/health", 1, "Spring Boot Actuator"),
          framework("GET /actuator/metrics/{*}", 15, "Spring Boot Actuator"),
          framework("GET /api/sse/{type}/{id}", 15, "streamrune.sse.enabled"),
          // Endpoints of the demo's controllers
          controller("POST /api/products", 2),
          controller("GET /api/products", 2),
          controller("GET /api/products/{id}", 2),
          controller("PUT /api/products/{id}/stock", 2),
          controller("PUT /api/products/{id}/price", 2),
          controller("POST /api/products/{id}/discontinue", 2),
          controller("POST /api/customers", 6),
          controller("GET /api/customers/{id}", 6),
          controller("PUT /api/customers/{id}", 6),
          controller("POST /api/customers/{id}/export-data", 6),
          controller("POST /api/customers/{id}/forget", 6),
          controller("GET /api/customers", 6),
          controller("POST /api/orders", 8),
          controller("GET /api/orders", 8),
          controller("GET /api/orders/{id}", 8),
          controller("POST /api/orders/{id}/ship", 8),
          controller("POST /api/orders/{id}/cancel", 8),
          controller("GET /api/audit/commands", 10),
          controller("GET /api/saga/fulfillments", 11),
          controller("GET /api/saga/fulfillments/{id}", 11),
          controller("POST /api/saga/fulfillments/{id}/inject-failure", 11),
          controller("GET /api/admin/circuit-breaker", 12),
          controller("POST /api/admin/payment-failure/toggle", 12),
          controller("POST /api/inventory/{id}/receive", 11),
          controller("GET /received", 13),
          controller("GET /health", 13),
          controller("GET /api/admin/outbox", 14),
          controller("GET /api/admin/outbox/failed", 14),
          controller("POST /api/admin/outbox/failed/{id}/skip", 14),
          controller("GET /api/admin/dead-letters", 14),
          controller("POST /api/admin/dead-letters/{id}/retry", 14),
          controller("DELETE /api/admin/dead-letters/{id}", 14),
          controller("GET /api/admin/saga-dead-letters", 14),
          controller("POST /api/admin/saga-dead-letters/replay-all", 14),
          controller("DELETE /api/admin/saga-dead-letters", 14),
          controller("GET /api/admin/sagas/faulted", 14),
          controller("POST /api/admin/sagas/faulted/resume", 14),
          controller("GET /api/events", 15),
          controller("GET /api/events/{type}/{id}", 15),
          controller("GET /api/events/sse", 15),
          // Aggregate types, the first segment of an event or SSE address
          listed("product", 2, "the product aggregate's type"),
          listed("customer", 6, "the customer aggregate's type"),
          listed("order", 8, "the order aggregate's type"),
          listed("payment", 11, "the payment aggregate's type"),
          listed("inventory", 11, "the inventory aggregate's type"),
          // Products
          created("p-1", 2),
          created("p-valid", 3),
          named("p-2", 3, "refused by validation here; chapter 5 creates it"),
          named("p-3", 3, "refused by validation"),
          named("p-4", 3, "refused by validation"),
          named("p-legacy", 4, "written as a raw version-1 event with psql"),
          created("p-mug", 8),
          named("p-nobody", 8, "a product nobody created: the order is refused"),
          created("p-retired", 9),
          listed("prod-widget", 17, "SeedDataRunner creates it at startup"),
          created("prod-sprocket", 17),
          created("prod-gizmo", 17),
          // Customers
          created("c-1", 6),
          created("c-alice", 7),
          created("c-bob", 7),
          named("c-carol", 9, "registered by the loop of the access-rule step"),
          named("customer-1", 9, "the customer an order names: placing one looks no customer up"),
          created("c-pii-test", 13),
          created("cust-walkthrough", 17),
          // Orders and what an order starts
          created("order-1", 8),
          created("order-2", 8),
          named("order-3", 8, "refused: its product id holds a line feed"),
          named("order-4", 8, "refused: its product does not exist"),
          created("order-5", 8),
          created("order-auth-test", 9),
          created("order-acl-1", 9),
          created("order-audit-1", 10),
          created("o-100", 11),
          named("fulfillment-o-100", 11, "the fulfillment saga of order o-100"),
          created("o-101", 11),
          named("fulfillment-o-101", 11, "the fulfillment saga of order o-101"),
          named("fulfillment-cb-1", 12, "the fulfillment saga of the first breaker order"),
          created("o-test-1", 13),
          created("o-fail-1", 14),
          created("o-obs-1", 15),
          created("o-obs-2", 15),
          created("ord-1", 17),
          named("fulfillment-ord-1", 17, "the fulfillment saga of order ord-1"),
          created("ord-2", 17),
          named("pay-ord-2", 17, "the payment of order ord-2"),
          named("fulfillment-walk-cb-1", 17, "the fulfillment saga of the first breaker order"));

  /** The {@code POST} that creates an aggregate, and the body field that names the new one. */
  private static final Map<String, String> CREATES =
      Map.of(
          "/api/products", "productId", "/api/customers", "customerId", "/api/orders", "orderId");

  private static final Pattern BODY_ID =
      Pattern.compile("\"(productId|customerId|orderId)\"\\s*:\\s*\"([^\"]*)\"");

  private static final Pattern LOCALHOST_URL =
      Pattern.compile("https?://localhost:(\\d+)(/[^\\s`'\")\\]>,]*)?");

  private static final Pattern CLASS_MAPPING =
      Pattern.compile("^@RequestMapping\\(\"([^\"]*)\"\\)");

  private static final Pattern METHOD_MAPPING =
      Pattern.compile(
          "^\\s+@(Get|Post|Put|Delete|Patch)Mapping"
              + "(?:\\((?:(?:value|path)\\s*=\\s*)?\"([^\"]*)\"[^)]*\\))?\\s*$");

  /** The languages of the fenced blocks that hold shell steps or their output. */
  private static final Set<String> SHELL = Set.of("bash", "sh", "shell", "console", "text", "");

  @Test
  void everyStepUsesOnlyWhatTheChaptersUpToItIntroduce() throws IOException {
    Map<String, Entry> ledger = new LinkedHashMap<>();
    LEDGER.forEach(entry -> ledger.put(entry.key(), entry));

    List<String> problems = new ArrayList<>();
    int requests = 0;
    for (Chapter chapter : chapters()) {
      for (int line : chapter.unreadCommands()) {
        problems.add(chapter.name() + ":" + line + " a curl command without an address to read");
      }
      for (Request request : chapter.requests()) {
        requests++;
        for (String key : uses(request, ledger)) {
          Entry entry = ledger.get(key);
          String where = chapter.name() + ":" + request.line() + " ";
          if (entry == null) {
            problems.add(where + key + " is not in the ledger");
          } else if (entry.chapter() > chapter.number()) {
            problems.add(
                where
                    + key
                    + " is used in chapter "
                    + chapter.number()
                    + " but introduced in "
                    + entry.chapter());
          }
        }
      }
    }

    assertThat(requests).as("requests the tutorial's steps send").isGreaterThan(100);
    assertThat(problems).as("steps that use what no chapter up to theirs introduced").isEmpty();
  }

  @Test
  void everyLedgerEntryIsIntroducedWhereTheLedgerSays() throws IOException {
    List<Chapter> chapters = chapters();
    Map<String, Entry> ledger = new LinkedHashMap<>();
    LEDGER.forEach(entry -> ledger.put(entry.key(), entry));
    List<Map<String, Integer>> mappedByChapter = controllerEndpointsByChapter(chapters);

    List<String> problems = new ArrayList<>();
    for (Entry entry : LEDGER) {
      Chapter chapter = chapters.get(entry.chapter() - 1);
      boolean introduced =
          switch (entry.how()) {
            case CREATED ->
                chapter.requests().stream()
                    .anyMatch(r -> r.method().equals("POST") && createdId(r).equals(entry.key()));
            case LISTED -> chapter.javaListings().contains("\"" + entry.key() + "\"");
            case CONTROLLER ->
                mappedByChapter.get(entry.chapter() - 1).containsKey(normalized(entry.key()))
                    && (entry.chapter() == 1
                        || !mappedByChapter
                            .get(entry.chapter() - 2)
                            .containsKey(normalized(entry.key())));
            case PORT ->
                chapter.requests().stream().anyMatch(r -> ("port " + r.port()).equals(entry.key()));
            case NAMED, FRAMEWORK, HEADER ->
                chapter.requests().stream().anyMatch(r -> uses(r, ledger).contains(entry.key()));
          };
      if (!introduced) {
        problems.add(entry + ": not introduced in chapter " + entry.chapter());
      }
    }

    Set<String> keys = new LinkedHashSet<>();
    List<String> duplicates =
        LEDGER.stream().map(Entry::key).filter(key -> !keys.add(key)).toList();
    assertThat(duplicates).as("keys the ledger lists twice").isEmpty();
    assertThat(problems).as("ledger entries their chapter does not introduce").isEmpty();
  }

  /** The ledger keys a request uses: its port, endpoint, headers and ids. */
  private static List<String> uses(Request request, Map<String, Entry> ledger) {
    List<String> keys = new ArrayList<>();
    keys.add("port " + request.port());
    if (request.path().isEmpty() || request.path().equals("/")) {
      return keys;
    }
    Optional<Entry> endpoint =
        ledger.values().stream()
            .filter(e -> e.how() == How.CONTROLLER || e.how() == How.FRAMEWORK)
            .filter(e -> matches(e.key(), request.method(), request.path()))
            .max(
                Comparator.comparingLong(
                    e -> segments(e.key()).stream().filter(s -> !s.startsWith("{")).count()));
    if (endpoint.isEmpty()) {
      keys.add(request.method() + " " + request.path());
      return keys;
    }
    keys.add(endpoint.get().key());
    List<String> template = segments(endpoint.get().key().split(" ", 2)[1]);
    List<String> actual = segments(request.path());
    for (int i = 0; i < template.size(); i++) {
      if (template.get(i).startsWith("{") && !template.get(i).equals("{*}")) {
        addId(keys, actual.get(i));
      }
    }
    for (String parameter : request.query().split("&")) {
      String[] pair = parameter.split("=", 2);
      if (pair.length == 2 && pair[0].endsWith("Id")) {
        addId(keys, pair[1]);
      }
    }
    for (String header : request.headers()) {
      String name = header.split(":", 2)[0].strip();
      if (!name.equalsIgnoreCase("Content-Type") && !name.equalsIgnoreCase("Accept")) {
        keys.add("header " + name);
      }
    }
    Matcher body = BODY_ID.matcher(request.body());
    while (body.find()) {
      addId(keys, body.group(2));
    }
    return keys;
  }

  private static void addId(List<String> keys, String value) {
    if (!value.isEmpty() && !value.matches(".*[$<>\\\\{}].*")) {
      keys.add(value);
    }
  }

  /** The id a {@code POST} to a collection creates, or the empty string. */
  private static String createdId(Request request) {
    String field = CREATES.get(request.path());
    if (field == null) {
      return "";
    }
    Matcher body = BODY_ID.matcher(request.body());
    while (body.find()) {
      if (body.group(1).equals(field)) {
        return body.group(2);
      }
    }
    return "";
  }

  /** Whether the endpoint {@code key} ({@code METHOD /template}) serves the request. */
  private static boolean matches(String key, String method, String path) {
    String[] parts = key.split(" ", 2);
    if (!parts[0].equals(method)) {
      return false;
    }
    List<String> template = segments(parts[1]);
    List<String> actual = segments(path);
    if (template.size() != actual.size()) {
      return false;
    }
    for (int i = 0; i < template.size(); i++) {
      if (!template.get(i).startsWith("{") && !template.get(i).equals(actual.get(i))) {
        return false;
      }
    }
    return true;
  }

  private static List<String> segments(String path) {
    return Stream.of(path.split("/")).filter(s -> !s.isEmpty()).toList();
  }

  /** {@code METHOD /template} with every path parameter written {@code {}}. */
  private static String normalized(String key) {
    return key.replaceAll("\\{[^}]*}", "{}");
  }

  /**
   * For each chapter, the endpoints the controllers of the reader's project map at its end, as
   * {@link #normalized} keys: the project is the last listing of every file up to the chapter.
   */
  private static List<Map<String, Integer>> controllerEndpointsByChapter(List<Chapter> chapters) {
    Map<String, String> project = new LinkedHashMap<>();
    List<Map<String, Integer>> byChapter = new ArrayList<>();
    for (Chapter chapter : chapters) {
      project.putAll(chapter.files());
      Map<String, Integer> endpoints = new TreeMap<>();
      for (String source : project.values()) {
        if (!source.contains("@RestController")) {
          continue;
        }
        String prefix = "";
        boolean inClass = false;
        for (String line : source.split("\n")) {
          Matcher type = CLASS_MAPPING.matcher(line);
          if (!inClass && type.find()) {
            prefix = type.group(1);
          }
          if (line.startsWith("public class ")) {
            inClass = true;
          }
          Matcher method = METHOD_MAPPING.matcher(line);
          if (inClass && method.find()) {
            String path = prefix + (method.group(2) == null ? "" : method.group(2));
            endpoints.put(normalized(method.group(1).toUpperCase() + " " + path), chapter.number());
          }
        }
      }
      byChapter.add(endpoints);
    }
    return byChapter;
  }

  private static List<Chapter> chapters() throws IOException {
    List<Chapter> chapters = new ArrayList<>();
    try (Stream<Path> files = Files.list(TUTORIAL)) {
      for (Path file : files.filter(p -> p.toString().endsWith(".md")).sorted().toList()) {
        chapters.add(Chapter.read(file, chapters.size() + 1));
      }
    }
    return chapters;
  }

  /** How a ledger entry comes into the tutorial. */
  private enum How {
    CREATED,
    LISTED,
    NAMED,
    CONTROLLER,
    FRAMEWORK,
    PORT,
    HEADER
  }

  /** One ledger line: the key a step uses, the chapter that introduces it, how, and why. */
  private record Entry(String key, int chapter, How how, String why) {
    @Override
    public String toString() {
      return key + " (" + how.name().toLowerCase() + ", chapter " + chapter + ")";
    }
  }

  private static Entry created(String id, int chapter) {
    return new Entry(id, chapter, How.CREATED, "created by a request of its chapter");
  }

  private static Entry listed(String id, int chapter, String why) {
    return new Entry(id, chapter, How.LISTED, why);
  }

  private static Entry named(String id, int chapter, String why) {
    return new Entry(id, chapter, How.NAMED, why);
  }

  private static Entry controller(String endpoint, int chapter) {
    return new Entry(endpoint, chapter, How.CONTROLLER, "mapped by a controller of its chapter");
  }

  private static Entry framework(String endpoint, int chapter, String why) {
    return new Entry(endpoint, chapter, How.FRAMEWORK, why);
  }

  private static Entry port(int port, int chapter, String why) {
    return new Entry("port " + port, chapter, How.PORT, why);
  }

  private static Entry header(String name, int chapter, String why) {
    return new Entry("header " + name, chapter, How.HEADER, why);
  }

  /** One request a step sends: where, the method, the port, the path and query, headers, body. */
  private record Request(
      int line,
      String method,
      int port,
      String path,
      String query,
      List<String> headers,
      String body) {}

  /**
   * One chapter: its number, file name, the requests of its steps, the lines of the {@code curl}
   * commands it has no address of, and its Java listings.
   */
  private record Chapter(
      int number,
      String name,
      List<Request> requests,
      List<Integer> unreadCommands,
      String javaListings,
      Map<String, String> files) {

    static Chapter read(Path file, int number) throws IOException {
      List<String> lines = Files.readAllLines(file);
      List<Request> requests = new ArrayList<>();
      List<Integer> unread = new ArrayList<>();
      StringBuilder java = new StringBuilder();
      Map<String, String> files = new LinkedHashMap<>();
      String language = null;
      List<String> block = new ArrayList<>();
      int blockStart = 0;
      for (int i = 0; i < lines.size(); i++) {
        String line = lines.get(i);
        if (line.strip().startsWith("```")) {
          if (language == null) {
            language = line.strip().substring(3).strip();
            block = new ArrayList<>();
            blockStart = i + 2;
          } else {
            if (SHELL.contains(language)) {
              requests.addAll(Shell.requests(block, blockStart, unread));
            } else if (language.equals("java")) {
              String source = String.join("\n", block);
              java.append(source).append('\n');
              if (!block.isEmpty() && block.getFirst().startsWith("package ")) {
                files.put(fileOf(block), source);
              }
            }
            language = null;
          }
          continue;
        }
        if (language != null) {
          block.add(line);
        } else {
          Matcher url = LOCALHOST_URL.matcher(line);
          while (url.find()) {
            requests.add(Shell.request(i + 1, "GET", url.group(), List.of(), ""));
          }
        }
      }
      return new Chapter(
          number, file.getFileName().toString(), requests, unread, java.toString(), files);
    }

    private static String fileOf(List<String> block) {
      String pkg = block.getFirst().replaceAll("^package\\s+([\\w.]+)\\s*;.*", "$1");
      for (String line : block) {
        Matcher type =
            Pattern.compile(
                    "^(?:(?:public|final|abstract|sealed|non-sealed)\\s+)*"
                        + "(?:class|interface|record|enum|@interface)\\s+(\\w+)")
                .matcher(line);
        if (type.find()) {
          return pkg + "." + type.group(1);
        }
      }
      return pkg;
    }
  }

  /** Reads the requests out of a shell block: {@code curl} commands and localhost addresses. */
  private static final class Shell {

    /**
     * The requests of a block; the first line of a curl command without an address joins unread.
     */
    static List<Request> requests(List<String> block, int firstLine, List<Integer> unread) {
      List<Request> requests = new ArrayList<>();
      StringBuilder command = new StringBuilder();
      int start = -1;
      for (int i = 0; i < block.size(); i++) {
        String line = block.get(i);
        if (command.isEmpty() && !line.contains("curl ")) {
          Matcher url = LOCALHOST_URL.matcher(line);
          while (url.find()) {
            requests.add(request(firstLine + i, "GET", url.group(), List.of(), ""));
          }
          continue;
        }
        if (start < 0) {
          start = firstLine + i;
        }
        command.append(line.endsWith("\\") ? line.substring(0, line.length() - 1) : line);
        if (line.endsWith("\\") || openQuote(command)) {
          command.append(line.endsWith("\\") ? " " : "\n");
          continue;
        }
        List<String> words = words(command.toString());
        int curl = words.indexOf("curl");
        if (curl >= 0) {
          Request request = curl(start, words.subList(curl + 1, words.size()));
          if (request.port() > 0) {
            requests.add(request);
          } else {
            unread.add(start);
          }
        }
        command.setLength(0);
        start = -1;
      }
      return requests;
    }

    private static Request curl(int line, List<String> args) {
      String method = null;
      String url = null;
      List<String> headers = new ArrayList<>();
      String body = "";
      for (int i = 0; i < args.size(); i++) {
        String arg = args.get(i);
        if (Set.of("|", ";", "&&", ">", "||").contains(arg)) {
          break;
        }
        String next = i + 1 < args.size() ? args.get(i + 1) : "";
        switch (arg) {
          case "-X", "--request" -> {
            method = next;
            i++;
          }
          case "-H", "--header" -> {
            headers.add(next);
            i++;
          }
          case "-d", "--data", "--data-raw", "--data-binary", "--json" -> {
            body = next;
            i++;
          }
          case "-o", "-w", "--output", "--write-out", "-u", "--user" -> i++;
          default -> {
            if (url == null && arg.matches("https?://.*")) {
              url = arg;
            }
          }
        }
      }
      if (method == null) {
        method = body.isEmpty() ? "GET" : "POST";
      }
      return request(line, method, url == null ? "" : url, headers, body);
    }

    static Request request(int line, String method, String url, List<String> headers, String body) {
      Matcher m =
          Pattern.compile("^https?://[^/:]+(?::(\\d+))?([^?#]*)(?:\\?([^#]*))?").matcher(url);
      if (!m.find()) {
        return new Request(line, method, 0, "", "", headers, body);
      }
      int port = m.group(1) == null ? 80 : Integer.parseInt(m.group(1));
      String path = m.group(2) == null ? "" : m.group(2);
      return new Request(
          line, method, port, path, m.group(3) == null ? "" : m.group(3), headers, body);
    }

    /** Whether a single or a double quote of the text is still open. */
    private static boolean openQuote(CharSequence text) {
      boolean single = false;
      boolean dbl = false;
      for (int i = 0; i < text.length(); i++) {
        char c = text.charAt(i);
        if (c == '\\' && !single) {
          i++;
        } else if (c == '\'' && !dbl) {
          single = !single;
        } else if (c == '"' && !single) {
          dbl = !dbl;
        }
      }
      return single || dbl;
    }

    /** The words of a shell command, quotes removed, as the shell splits them. */
    private static List<String> words(String command) {
      List<String> words = new ArrayList<>();
      StringBuilder word = new StringBuilder();
      boolean inWord = false;
      for (int i = 0; i < command.length(); i++) {
        char c = command.charAt(i);
        if (c == '\'') {
          int end = command.indexOf('\'', i + 1);
          end = end < 0 ? command.length() : end;
          word.append(command, i + 1, end);
          i = end;
          inWord = true;
        } else if (c == '"') {
          i++;
          while (i < command.length() && command.charAt(i) != '"') {
            if (command.charAt(i) == '\\' && i + 1 < command.length()) {
              i++;
            }
            word.append(command.charAt(i));
            i++;
          }
          inWord = true;
        } else if (Character.isWhitespace(c)) {
          if (inWord) {
            words.add(word.toString());
            word.setLength(0);
            inWord = false;
          }
        } else {
          word.append(c);
          inWord = true;
        }
      }
      if (inWord) {
        words.add(word.toString());
      }
      return words;
    }
  }
}
