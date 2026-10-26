package org.streamrune.ecommerce.spring.docs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Chapter 1 shows an excerpt of the demo's version catalog, {@code gradle/libs.versions.toml}, and
 * tells the reader to copy the whole file: the excerpt names the versions and aliases the chapter's
 * build files use. This test holds the excerpt to the file, so a version bump in the catalog shows
 * up as a failure here until the chapter follows it: every line the excerpt shows is a line of the
 * same section of the catalog, in the catalog's order, apart from the one line that says the
 * excerpt leaves the rest out.
 */
class TutorialCatalogListingTest {

  private static final Path CHAPTER = Path.of("../docs/tutorial/01-project-setup.md");

  private static final Path CATALOG = Path.of("../gradle/libs.versions.toml");

  private static final String INTRO =
      "Create `gradle/libs.versions.toml` (copy from the real demo — only the key aliases used in"
          + " this tutorial are shown here; copy the full file to get all aliases):";

  /** The line of the excerpt that stands for the lines it leaves out. */
  private static final String ELLIPSIS =
      "# ... (see the full catalog in the real demo for all aliases)";

  @Test
  void everyLineOfTheCatalogExcerptIsALineOfTheCatalogInItsSection() throws IOException {
    Map<String, List<String>> excerpt = sections(listingAfter(INTRO));
    Map<String, List<String>> catalog = sections(Files.readAllLines(CATALOG));

    assertThat(excerpt.keySet())
        .as("the sections of chapter 1's catalog excerpt")
        .containsExactly("[versions]", "[libraries]", "[plugins]");
    for (Map.Entry<String, List<String>> section : excerpt.entrySet()) {
      List<String> lines = new ArrayList<>(section.getValue());
      lines.removeIf(line -> line.isBlank() || line.equals(ELLIPSIS));
      assertThat(catalog.get(section.getKey()))
          .as("the lines chapter 1 shows in %s, in the catalog's order", section.getKey())
          .containsSubsequence(lines);
    }
  }

  @Test
  void theExcerptStatesTheFrameworkAndSpringBootVersions() throws IOException {
    List<String> versions = sections(listingAfter(INTRO)).get("[versions]");

    assertThat(versions)
        .as("the versions chapter 1 states")
        .anyMatch(line -> line.startsWith("streamrune = "))
        .anyMatch(line -> line.startsWith("spring-boot = "));
  }

  /** The lines of the first fenced block after the line {@code intro}. */
  private static List<String> listingAfter(String intro) throws IOException {
    List<String> lines = Files.readAllLines(CHAPTER);
    int start = lines.indexOf(intro);
    assertThat(start).as("chapter 1 introduces the listing with: %s", intro).isNotNegative();
    int open = start + 1;
    while (!lines.get(open).startsWith("```")) {
      open++;
    }
    List<String> listing = new ArrayList<>();
    for (int i = open + 1; !lines.get(i).startsWith("```"); i++) {
      listing.add(lines.get(i));
    }
    return listing;
  }

  /** The lines of each {@code [section]} of a TOML file, in order. */
  private static Map<String, List<String>> sections(List<String> lines) {
    Map<String, List<String>> sections = new LinkedHashMap<>();
    List<String> current = null;
    for (String line : lines) {
      if (line.startsWith("[") && line.endsWith("]")) {
        current = new ArrayList<>();
        sections.put(line, current);
      } else if (current != null) {
        current.add(line);
      }
    }
    return sections;
  }
}
