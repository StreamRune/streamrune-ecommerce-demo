package org.streamrune.ecommerce.spring.docs;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;
import org.streamrune.ecommerce.spring.docs.TutorialListingsCompileTest.Listing;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/**
 * The project a reader has at the end of each chapter starts. {@link TutorialListingsCompileTest}
 * proves that it compiles; a project that compiles can still fail to start — a bean a controller
 * injects that no chapter declares yet, two beans of one injected type, a builder that refuses its
 * configuration — and only starting it shows that.
 *
 * <p>For every chapter with complete Java files, the test builds the reader's project as the
 * compile test does, from the last listing of every file of chapters 1 to N, compiles its main
 * sources (the listed tests stay out: they are not part of the application), and starts {@code
 * SpringEcommerceApplication} in a JVM of its own, with the compiled classes before the framework
 * and the third-party libraries on its classpath, as the reader's {@code bootRun} does. Each
 * chapter gets a database of its own on the test PostgreSQL ({@code tutorial_NN}) and the test
 * RabbitMQ. The project starts when the log says {@code Started SpringEcommerceApplication}; it
 * fails when the process ends first or does not get there within {@value #START_TIMEOUT_SECONDS}
 * seconds, and the failure names the chapter and quotes Spring's report.
 *
 * <p><b>Started is not enough.</b> A bean can start and its thread die after it: a subscription
 * whose first poll fails, a runner that refuses its configuration at its first use. So a started
 * project runs on for one polling interval ({@link #POLLING_INTERVAL}), and then its health is
 * asked once: the chapter fails when the answer is not {@code UP}, or when the log shows a line at
 * {@code ERROR} or Spring's {@code APPLICATION FAILED} report by then, the lines up to the started
 * line included ({@link #errors}).
 *
 * <p><b>The reader's {@code application.yml}.</b> Chapter 1 lists it in full, and later chapters
 * add to it with fragments: a {@code yaml} block whose introducing line names {@value
 * #APPLICATION_YML}. The test merges every fragment up to the chapter into the file, key by key, a
 * later value replacing an earlier one, and puts the result on the started project's classpath.
 * After the last chapter the merged file is the repository's, key for key: a fragment whose keys or
 * values drifted from the file, or a key of the file that no chapter gives, fails the test.
 */
class TutorialChaptersStartIT {

  private static final String APPLICATION =
      "org.streamrune.ecommerce.spring.SpringEcommerceApplication";

  /** The file the configuration fragments of the chapters belong to. */
  private static final String APPLICATION_YML = "spring-app/src/main/resources/application.yml";

  private static final long START_TIMEOUT_SECONDS = 120;

  /** The line of the log that says the project started. */
  private static final String STARTED = "Started SpringEcommerceApplication in ";

  /**
   * How long a started project runs before its health is asked: the polling interval of the
   * chapters' subscriptions ({@code SubscriptionConfig.pollingOnly(Duration.ofMillis(500))}), by
   * when each of them has polled once.
   */
  private static final Duration POLLING_INTERVAL = Duration.ofMillis(500);

  /** A line of the log at level {@code ERROR}, in Spring Boot's console format. */
  private static final Pattern ERROR_LINE = Pattern.compile("^\\S+\\s+ERROR\\s+\\d+\\s+---");

  /** The port of the started project's web server, from its log. */
  private static final Pattern PORT = Pattern.compile("Tomcat started on port (\\d+)");

  private static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  private static final Path REPOSITORY = Path.of("..").toAbsolutePath().normalize();

  @Test
  void theProjectOfEveryChapterStarts(@TempDir Path out) throws Exception {
    JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
    assertThat(javac).as("the tests run on a JDK with a Java compiler").isNotNull();
    String classpath = TutorialListingsCompileTest.frameworkAndLibraries();

    Map<String, Listing> project = new LinkedHashMap<>();
    Map<String, Object> configuration = new LinkedHashMap<>();
    int fragments = 0;
    int started = 0;
    List<String> failures = new ArrayList<>();
    for (Path chapter : TutorialListingsCompileTest.chapters()) {
      for (Map<String, Object> fragment : configurationFragments(chapter)) {
        merge(configuration, fragment);
        fragments++;
      }
      List<Listing> listings = TutorialListingsCompileTest.completeFiles(chapter);
      if (listings.isEmpty()) {
        continue;
      }
      listings.forEach(listing -> project.put(listing.className(), listing));
      String name = chapter.getFileName().toString();
      Path classes = Files.createDirectories(out.resolve(name));
      List<String> errors =
          TutorialListingsCompileTest.compile(
              javac,
              project.values().stream().filter(l -> isMainSource(l.className())).toList(),
              classpath,
              classes);
      if (!errors.isEmpty()) {
        failures.add(name + ": the project does not compile, " + errors.getFirst());
        continue;
      }
      Files.writeString(classes.resolve("application.yml"), yaml().dump(configuration));
      String failure = start(chapter, classes, classpath, out);
      if (failure != null) {
        failures.add(name + ": " + failure);
      }
      started++;
    }

    assertThat(fragments)
        .as("configuration the chapters give, the listing of chapter 1 and the fragments after it")
        .isGreaterThanOrEqualTo(9);
    Map<String, Object> repository =
        yaml().load(Files.readString(REPOSITORY.resolve(APPLICATION_YML)));
    assertThat(keys("", configuration))
        .as("%s as the chapters build it, key by key, against the repository's", APPLICATION_YML)
        .containsExactlyInAnyOrderEntriesOf(keys("", repository));
    assertThat(started).as("chapters whose project was started").isGreaterThan(10);
    assertThat(failures).as("chapters whose project does not start").isEmpty();
  }

  /**
   * Starts the project of {@code chapter} and stops it again.
   *
   * @return {@code null} when it started and, one polling interval later, is {@code UP} with no
   *     error in its log, otherwise what is wrong
   */
  private static String start(Path chapter, Path classes, String classpath, Path out)
      throws Exception {
    String name = chapter.getFileName().toString();
    String database = "tutorial_" + name.substring(0, 2);
    createDatabase(database);
    var postgres = AbstractIntegrationTest.POSTGRES;
    var rabbit = AbstractIntegrationTest.RABBIT;
    Path arguments = out.resolve(name + ".args");
    Files.write(
        arguments,
        Stream.of(
                "--enable-preview",
                "-cp",
                classes + File.pathSeparator + classpath,
                APPLICATION,
                "--server.port=0",
                "--management.endpoint.health.show-details=always",
                "--spring.datasource.url=" + databaseUrl(database),
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--spring.rabbitmq.host=" + rabbit.getHost(),
                "--spring.rabbitmq.port=" + rabbit.getAmqpPort(),
                "--spring.rabbitmq.username=" + rabbit.getAdminUsername(),
                "--spring.rabbitmq.password=" + rabbit.getAdminPassword())
            .map(TutorialChaptersStartIT::quoted)
            .toList());
    Path log = out.resolve(name + ".log");
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(), "@" + arguments)
            .redirectErrorStream(true)
            .redirectOutput(log.toFile())
            .start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(START_TIMEOUT_SECONDS);
      while (System.nanoTime() < deadline) {
        String text = Files.readString(log);
        if (text.contains(STARTED)) {
          return afterTheStart(text, log);
        }
        if (!process.isAlive()) {
          return "the application stopped: " + report(Files.readString(log));
        }
        Thread.sleep(200);
      }
      return "not started within " + START_TIMEOUT_SECONDS + " s: " + report(Files.readString(log));
    } finally {
      process.destroy();
      if (!process.waitFor(20, TimeUnit.SECONDS)) {
        process.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
      }
    }
  }

  /**
   * Lets a started project run one polling interval, asks its health once and reads its log again.
   *
   * @param started the log as it stood when it showed the started line
   * @return {@code null} when the health is {@code UP} and the log shows no error, otherwise what
   *     is wrong
   */
  private static String afterTheStart(String started, Path log) throws Exception {
    Matcher port = PORT.matcher(started);
    if (!port.find()) {
      return "the log names no port of the web server: " + report(started);
    }
    Thread.sleep(POLLING_INTERVAL.toMillis());
    HttpResponse<String> health;
    try {
      health =
          HTTP.send(
              HttpRequest.newBuilder(
                      URI.create("http://localhost:" + port.group(1) + "/actuator/health"))
                  .timeout(Duration.ofSeconds(10))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
    } catch (IOException e) {
      return "the health check got no answer (" + e + "): " + report(Files.readString(log));
    }
    String errors = errors(Files.readString(log));
    if (errors != null) {
      return "the log shows errors: " + errors;
    }
    String status;
    try {
      status = new ObjectMapper().readTree(health.body()).path("status").asText();
    } catch (IOException e) {
      status = "unreadable";
    }
    if (health.statusCode() != 200 || !status.equals("UP")) {
      return "the health is "
          + status
          + " ("
          + health.statusCode()
          + ") one polling interval after the start: "
          + health.body();
    }
    return null;
  }

  /**
   * The lines of {@code log} at {@code ERROR} and the start of Spring's {@code APPLICATION FAILED}
   * report, the first five of them, or {@code null} when it has none. A message that contains the
   * word is no error: the level is read where Spring Boot's console format writes it.
   */
  static String errors(String log) {
    List<String> lines =
        log.lines()
            .filter(line -> ERROR_LINE.matcher(line).find() || line.contains("APPLICATION FAILED"))
            .map(String::strip)
            .limit(5)
            .toList();
    return lines.isEmpty() ? null : String.join(" | ", lines);
  }

  @Test
  void aLineAtErrorOrSpringsFailureReportIsAnError() {
    String info =
        "2026-10-27T12:00:00.000+01:00  INFO 4242 --- [           main] o.s.b.SpringApplication"
            + "                  : Starting SpringEcommerceApplication";
    String warn =
        "2026-10-27T12:00:00.001+01:00  WARN 4242 --- [           main] o.s.r.PollingEventSubscription"
            + "           : an ERROR in a message is no error";
    String error =
        "2026-10-27T12:00:00.002+01:00 ERROR 4242 --- [       poll-1] o.s.r.PollingEventSubscription"
            + "           : poll failed";

    assertThat(errors(info + "\n" + warn)).isNull();
    assertThat(errors(info + "\n" + error + "\n" + warn)).endsWith(": poll failed");
    assertThat(errors(info + "\n***************************\nAPPLICATION FAILED TO START"))
        .isEqualTo("APPLICATION FAILED TO START");
  }

  /** Spring's failure report when the log has one, otherwise the end of the log. */
  private static String report(String log) {
    int failed = log.indexOf("APPLICATION FAILED TO START");
    List<String> lines = (failed >= 0 ? log.substring(failed) : log).lines().toList();
    List<String> shown =
        failed >= 0
            ? lines.subList(0, Math.min(lines.size(), 12))
            : lines.subList(Math.max(0, lines.size() - 20), lines.size());
    return String.join(" | ", shown.stream().map(String::strip).filter(l -> !l.isEmpty()).toList());
  }

  /**
   * The {@code yaml} blocks of one chapter that belong to {@value #APPLICATION_YML}: the line that
   * introduces the block, the last line of text before it, names the file.
   */
  private static List<Map<String, Object>> configurationFragments(Path chapter) throws IOException {
    List<String> lines = Files.readAllLines(chapter);
    List<Map<String, Object>> fragments = new ArrayList<>();
    for (int i = 0; i < lines.size(); i++) {
      if (!lines.get(i).strip().equals("```yaml")) {
        continue;
      }
      int intro = i - 1;
      while (intro >= 0 && lines.get(intro).isBlank()) {
        intro--;
      }
      int end = i + 1;
      while (end < lines.size() && !lines.get(end).strip().startsWith("```")) {
        end++;
      }
      if (intro >= 0 && lines.get(intro).contains(APPLICATION_YML)) {
        Map<String, Object> fragment = yaml().load(String.join("\n", lines.subList(i + 1, end)));
        fragments.add(fragment);
      }
      i = end;
    }
    return fragments;
  }

  /** Every key of {@code section} below {@code prefix} as a dotted path, with its value. */
  private static Map<String, Object> keys(String prefix, Map<?, ?> section) {
    Map<String, Object> keys = new TreeMap<>();
    section.forEach(
        (key, value) -> {
          String path = prefix.isEmpty() ? String.valueOf(key) : prefix + "." + key;
          if (value instanceof Map<?, ?> nested) {
            keys.putAll(keys(path, nested));
          } else {
            keys.put(path, value);
          }
        });
    return keys;
  }

  /** Puts every key of {@code fragment} into {@code configuration}, a section into its section. */
  @SuppressWarnings("unchecked")
  private static void merge(Map<String, Object> configuration, Map<String, Object> fragment) {
    fragment.forEach(
        (key, value) -> {
          if (value instanceof Map<?, ?> section
              && configuration.get(key) instanceof Map<?, ?> existing) {
            merge((Map<String, Object>) existing, (Map<String, Object>) section);
          } else {
            configuration.put(key, value);
          }
        });
  }

  private static Yaml yaml() {
    DumperOptions options = new DumperOptions();
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    return new Yaml(options);
  }

  /** Whether a module of the demo holds {@code className} in its main sources. */
  private static boolean isMainSource(String className) {
    String relative = "src/main/java/" + className.replace('.', '/') + ".java";
    try (Stream<Path> modules = Files.list(REPOSITORY)) {
      return modules.anyMatch(module -> Files.isRegularFile(module.resolve(relative)));
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** One argument of a {@code java @file}: quoted, with its backslashes and quotes escaped. */
  private static String quoted(String argument) {
    return '"' + argument.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
  }

  private static void createDatabase(String database) throws Exception {
    var postgres = AbstractIntegrationTest.POSTGRES;
    try (Connection conn =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement st = conn.createStatement()) {
      st.execute("DROP DATABASE IF EXISTS " + database + " WITH (FORCE)");
      st.execute("CREATE DATABASE " + database);
    }
  }

  private static String databaseUrl(String database) {
    var postgres = AbstractIntegrationTest.POSTGRES;
    return "jdbc:postgresql://"
        + postgres.getHost()
        + ":"
        + postgres.getMappedPort(5432)
        + "/"
        + database;
  }
}
