package org.streamrune.ecommerce.spring.docs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
 * <p><b>The reader's {@code application.yml}.</b> Chapter 1 lists it in full, and later chapters
 * add to it with fragments: a {@code yaml} block whose introducing line names {@value
 * #APPLICATION_YML}. The test merges every fragment up to the chapter into the file, key by key, a
 * later value replacing an earlier one, and puts the result on the started project's classpath.
 */
class TutorialChaptersStartIT {

  private static final String APPLICATION =
      "org.streamrune.ecommerce.spring.SpringEcommerceApplication";

  /** The file the configuration fragments of the chapters belong to. */
  private static final String APPLICATION_YML = "spring-app/src/main/resources/application.yml";

  private static final long START_TIMEOUT_SECONDS = 120;

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
    assertThat(started).as("chapters whose project was started").isGreaterThan(10);
    assertThat(failures).as("chapters whose project does not start").isEmpty();
  }

  /**
   * Starts the project of {@code chapter} and stops it again.
   *
   * @return {@code null} when it started, otherwise what Spring reported
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
        if (text.contains("Started SpringEcommerceApplication in ")) {
          return null;
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
