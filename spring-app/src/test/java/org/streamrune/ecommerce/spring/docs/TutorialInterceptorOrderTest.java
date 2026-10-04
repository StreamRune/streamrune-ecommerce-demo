package org.streamrune.ecommerce.spring.docs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.streamrune.runtime.CommandInterceptorOrdering;

/**
 * Every interceptor chain the tutorial shows, and the one the Spring app builds, lists the
 * interceptors in the framework's canonical order ({@link CommandInterceptorOrdering}), the order
 * the Quarkus and Micronaut integrations impose on the chains they assemble: audit outside
 * authorization, so a refused command still reaches the audit interceptor's {@code onError()} and
 * is recorded; authorization before Bean Validation, so a caller without the role learns nothing
 * about the validation rules; the circuit breaker innermost, so it counts only execution failures.
 * {@code VirtualThreadCommandBus.Builder.interceptors(...)} runs {@code before()} in the order the
 * chain lists, so a chain written in another order changes what is audited.
 */
class TutorialInterceptorOrderTest {

  private static final Path TUTORIAL = Path.of("../docs/tutorial");

  private static final Path SPRING_CONFIG =
      Path.of("src/main/java/org/streamrune/ecommerce/spring/config/StreamRuneConfig.java");

  /** The variable name each interceptor has in the tutorial and the Spring app, and its slot. */
  private static final Map<String, Integer> ORDER_BY_NAME =
      Map.of(
          "otel", CommandInterceptorOrdering.ORDER_OPEN_TELEMETRY,
          "audit", CommandInterceptorOrdering.ORDER_AUDIT,
          "auth", CommandInterceptorOrdering.ORDER_ANNOTATION_AUTHORIZATION,
          "validation", CommandInterceptorOrdering.ORDER_VALIDATION,
          "circuitBreaker", CommandInterceptorOrdering.ORDER_CIRCUIT_BREAKER);

  private static final Pattern CHAIN = Pattern.compile("\\.interceptors\\(([^)]*)\\)");

  @Test
  void everyInterceptorChainIsInTheCanonicalOrder() throws IOException {
    List<Path> files = new ArrayList<>();
    try (Stream<Path> chapters = Files.list(TUTORIAL)) {
      chapters.filter(p -> p.toString().endsWith(".md")).sorted().forEach(files::add);
    }
    files.add(SPRING_CONFIG);

    List<String> chains = new ArrayList<>();
    List<String> outOfOrder = new ArrayList<>();
    for (Path file : files) {
      List<String> lines = Files.readAllLines(file);
      for (int i = 0; i < lines.size(); i++) {
        Matcher m = CHAIN.matcher(lines.get(i));
        while (m.find()) {
          String args = m.group(1).strip();
          if (args.isEmpty() || args.equals("...")) {
            continue; // a prose mention of the builder call, not a chain
          }
          String where = file.getFileName() + ":" + (i + 1) + " .interceptors(" + args + ")";
          chains.add(where);
          int previous = Integer.MIN_VALUE;
          for (String name : Arrays.stream(args.split(",")).map(String::strip).toList()) {
            Integer order = ORDER_BY_NAME.get(name);
            assertThat(order)
                .as("%s names an interceptor this test does not map", where)
                .isNotNull();
            if (order < previous) {
              outOfOrder.add(where);
              break;
            }
            previous = order;
          }
        }
      }
    }

    assertThat(chains).anyMatch(c -> c.startsWith("StreamRuneConfig.java:"));
    assertThat(chains).anyMatch(c -> c.startsWith("10-audit-logging.md:"));
    assertThat(outOfOrder).as("interceptor chains out of the canonical order").isEmpty();
  }
}
