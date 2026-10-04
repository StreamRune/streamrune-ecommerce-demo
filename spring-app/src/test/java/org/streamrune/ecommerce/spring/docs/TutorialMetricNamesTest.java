package org.streamrune.ecommerce.spring.docs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.streamrune.core.metrics.MetricNames;

/**
 * Every StreamRune meter name the tutorial and the README give, dotted ({@code
 * streamrune.events.duration}) or in the form Prometheus exports it ({@code
 * streamrune_events_duration_seconds}), is a meter the framework registers: a {@link MetricNames}
 * constant. An alert or a dashboard written against a name the framework never registers stays
 * silent instead of failing, so a wrong name in the docs is found here rather than in production.
 *
 * <p>A dotted name counts as a meter name when its second segment is one a {@link MetricNames}
 * constant uses ({@code commands}, {@code events}, {@code outbox}, ...). Configuration properties
 * share some of those namespaces; they are told apart by their shape: a property segment contains a
 * hyphen ({@code streamrune.outbox.retention-max-age}) or the key ends in {@code .enabled}.
 */
class TutorialMetricNamesTest {

  private static final Path TUTORIAL = Path.of("../docs/tutorial");

  private static final Path README = Path.of("../README.md");

  /** {@code streamrune.} and at least two more segments, not inside a longer name or a property. */
  private static final Pattern DOTTED =
      Pattern.compile("(?<![\\w.-])streamrune(?:\\.[a-z0-9_]+){2,}(?![\\w-])");

  /** A Prometheus series name: the dotted name with underscores, plus a unit or type suffix. */
  private static final Pattern PROMETHEUS =
      Pattern.compile("(?<![\\w.-])streamrune(?:_[a-z0-9]+)+(?![\\w-])");

  /** What the Prometheus registry appends to a meter name: base unit, then type suffix. */
  private static final Set<String> PROMETHEUS_SUFFIXES =
      Set.of(
          "",
          "_total",
          "_seconds",
          "_seconds_count",
          "_seconds_sum",
          "_seconds_max",
          "_seconds_bucket",
          "_count",
          "_sum",
          "_max",
          "_bucket");

  @Test
  void everyMeterNameInTheDocsIsOneTheFrameworkRegisters() throws IOException {
    Set<String> meters = meterNames();
    Set<String> families =
        meters.stream().map(n -> n.split("\\.")[1]).collect(Collectors.toCollection(TreeSet::new));

    List<String> checked = new ArrayList<>();
    List<String> unknown = new ArrayList<>();
    for (Path file : docs()) {
      List<String> lines = Files.readAllLines(file);
      for (int i = 0; i < lines.size(); i++) {
        String where = file.getFileName() + ":" + (i + 1) + " ";
        Matcher dotted = DOTTED.matcher(lines.get(i));
        while (dotted.find()) {
          String name = dotted.group();
          if (!families.contains(name.split("\\.")[1]) || name.endsWith(".enabled")) {
            continue;
          }
          checked.add(where + name);
          if (!meters.contains(name)) {
            unknown.add(where + name);
          }
        }
        Matcher prometheus = PROMETHEUS.matcher(lines.get(i));
        while (prometheus.find()) {
          String name = prometheus.group();
          if (!families.contains(name.split("_")[1])) {
            continue;
          }
          checked.add(where + name);
          boolean known =
              meters.stream()
                  .map(m -> m.replace('.', '_'))
                  .anyMatch(
                      m ->
                          name.startsWith(m)
                              && PROMETHEUS_SUFFIXES.contains(name.substring(m.length())));
          if (!known) {
            unknown.add(where + name);
          }
        }
      }
    }

    assertThat(checked).anyMatch(c -> c.startsWith("15-observability.md:"));
    assertThat(unknown).as("meter names the framework does not register").isEmpty();
  }

  /** The meter names {@link MetricNames} publishes; its other constants are tag keys. */
  private static Set<String> meterNames() {
    Set<String> names = new TreeSet<>();
    for (Field field : MetricNames.class.getFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
        try {
          String value = (String) field.get(null);
          if (value.startsWith("streamrune.")) {
            names.add(value);
          }
        } catch (IllegalAccessException e) {
          throw new AssertionError(e);
        }
      }
    }
    return names;
  }

  private static List<Path> docs() throws IOException {
    List<Path> files = new ArrayList<>();
    try (Stream<Path> chapters = Files.list(TUTORIAL)) {
      chapters.filter(p -> p.toString().endsWith(".md")).sorted().forEach(files::add);
    }
    files.add(README);
    return files;
  }
}
