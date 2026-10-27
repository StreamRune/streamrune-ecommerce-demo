package org.streamrune.ecommerce.spring.docs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Every entry of the version catalog, {@code gradle/libs.versions.toml}, is used. A library, plugin
 * or bundle alias is referenced through its accessor ({@code libs.spring.boot.starter.test}, {@code
 * libs.plugins.spotless}, {@code libs.findLibrary("junit-bom")}) by a build file of the demo or by
 * a build listing of the tutorial, whose reader copies the whole catalog; a version is the {@code
 * version.ref} of a used alias or is read through {@code libs.versions}. An alias nothing resolves
 * looks maintained, and its version falls behind the framework's without anyone noticing. The
 * Flyway pair is kept on purpose, in step with the framework build, as the catalog says ({@link
 * #KEPT}).
 */
class VersionCatalogAliasesTest {

  private static final Path REPOSITORY = Path.of("..").toAbsolutePath().normalize();

  private static final Path CATALOG = REPOSITORY.resolve("gradle/libs.versions.toml");

  private static final Path TUTORIAL = REPOSITORY.resolve("docs/tutorial");

  /** Entries kept although nothing resolves them; the catalog's comment says why. */
  private static final Set<String> KEPT = Set.of("flyway-core", "flyway-postgresql", "flyway");

  /**
   * Directories that hold no build file of the demo: output, dependencies, tooling, other
   * checkouts.
   */
  private static final Set<String> PRUNED =
      Set.of(
          "build",
          ".gradle",
          ".git",
          ".kotlin",
          "node_modules",
          ".next",
          "out",
          "bin",
          ".worktrees",
          ".claude",
          "scratchpad");

  /** An entry of a catalog section: its alias and its value. */
  private static final Pattern ENTRY = Pattern.compile("^([a-z0-9-]+)\\s*=\\s*(.+)$");

  /** The {@code version.ref} of a library or plugin entry. */
  private static final Pattern VERSION_REF = Pattern.compile("version\\.ref\\s*=\\s*\"([^\"]+)\"");

  /** An accessor chain of the catalog, {@code libs.} and its dotted path. */
  private static final Pattern ACCESSOR =
      Pattern.compile("\\blibs\\.([a-zA-Z0-9]+(?:\\.[a-zA-Z0-9]+)*)");

  /** A lookup of the catalog by alias, {@code libs.findLibrary("junit-bom")}. */
  private static final Pattern LOOKUP =
      Pattern.compile("\\bfind(?:Library|Plugin|Bundle|Version)\\(\"([a-z0-9-]+)\"\\)");

  @Test
  void everyAliasAndVersionOfTheCatalogIsUsed() throws IOException {
    Map<String, Map<String, String>> catalog = sections(Files.readAllLines(CATALOG));
    Set<String> accessors = new TreeSet<>();
    Set<String> lookups = new TreeSet<>();
    for (Path file : buildFilesAndChapters()) {
      String text = Files.readString(file);
      Matcher accessor = ACCESSOR.matcher(text);
      while (accessor.find()) {
        accessors.add(accessor.group(1));
      }
      Matcher lookup = LOOKUP.matcher(text);
      while (lookup.find()) {
        lookups.add(lookup.group(1));
      }
    }
    assertThat(accessors).as("the accessors of the build files").isNotEmpty();

    List<String> unused = new ArrayList<>();
    Set<String> usedVersions = new TreeSet<>();
    for (String section : List.of("libraries", "plugins", "bundles")) {
      String prefix = section.equals("libraries") ? "" : section + ".";
      catalog
          .getOrDefault(section, Map.of())
          .forEach(
              (alias, value) -> {
                String path = prefix + alias.replace('-', '.');
                boolean used =
                    lookups.contains(alias)
                        || accessors.contains(path)
                        || accessors.contains(path + ".get");
                if (used) {
                  Matcher ref = VERSION_REF.matcher(value);
                  if (ref.find()) {
                    usedVersions.add(ref.group(1));
                  }
                } else if (!KEPT.contains(alias)) {
                  unused.add("[" + section + "] " + alias);
                }
              });
    }
    for (String version : catalog.getOrDefault("versions", Map.of()).keySet()) {
      String path = "versions." + version.replace('-', '.');
      boolean read =
          lookups.contains(version)
              || accessors.contains(path)
              || accessors.stream().anyMatch(a -> a.startsWith(path + "."));
      if (!usedVersions.contains(version) && !read && !KEPT.contains(version)) {
        unused.add("[versions] " + version);
      }
    }
    assertThat(unused)
        .as(
            "catalog entries no build file of the demo and no build listing of the tutorial uses;"
                + " delete them, or say in the catalog why one is kept and add it to KEPT")
        .isEmpty();
  }

  /** The entries of each {@code [section]} of the catalog, alias to value, in order. */
  private static Map<String, Map<String, String>> sections(List<String> lines) {
    Map<String, Map<String, String>> sections = new LinkedHashMap<>();
    Map<String, String> current = null;
    for (String line : lines) {
      if (line.startsWith("[") && line.endsWith("]")) {
        current = new LinkedHashMap<>();
        sections.put(line.substring(1, line.length() - 1), current);
      } else if (current != null) {
        Matcher entry = ENTRY.matcher(line);
        if (entry.matches()) {
          current.put(entry.group(1), entry.group(2));
        }
      }
    }
    return sections;
  }

  /** The demo's Gradle build files and the tutorial's chapters, whose build listings count. */
  private static List<Path> buildFilesAndChapters() throws IOException {
    List<Path> files = new ArrayList<>();
    Files.walkFileTree(
        REPOSITORY,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            return PRUNED.contains(dir.getFileName().toString())
                ? FileVisitResult.SKIP_SUBTREE
                : FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            if (file.getFileName().toString().endsWith(".kts")) {
              files.add(file);
            }
            return FileVisitResult.CONTINUE;
          }
        });
    try (Stream<Path> chapters = Files.list(TUTORIAL)) {
      chapters.filter(path -> path.toString().endsWith(".md")).sorted().forEach(files::add);
    }
    return files;
  }
}
