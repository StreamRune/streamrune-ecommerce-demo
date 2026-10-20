package org.streamrune.ecommerce.spring.docs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Chapter 1 gives the demo's {@code settings.gradle.kts} twice: the composite build block, "exact
 * copy from the real demo", and in Step 1 the whole file, with a note that says the demo's own file
 * differs only in its {@code include(...)} call, which also lists the modules the note names. This
 * test holds the chapter to both statements, so a change to the settings file shows up as a failure
 * here until the chapter follows it.
 */
class TutorialSettingsListingTest {

  private static final Path CHAPTER = Path.of("../docs/tutorial/01-project-setup.md");

  private static final Path SETTINGS = Path.of("../settings.gradle.kts");

  private static final String BLOCK_INTRO =
      "Open `settings.gradle.kts` and add the composite build block **before** the `include(...)`"
          + " call (exact copy from the real demo):";

  private static final String FILE_INTRO =
      "Open `settings.gradle.kts` and replace its contents with the composite build version (Path"
          + " A):";

  private static final String NOTE_START = "> The real demo's `settings.gradle.kts` also includes ";

  private static final Pattern MODULE = Pattern.compile("\"([a-z0-9-]+)\"");

  @Test
  void theCompositeBuildBlockIsCopiedFromTheDemo() throws IOException {
    String block = String.join("\n", listingAfter(BLOCK_INTRO));

    assertThat(Files.readString(SETTINGS))
        .as("the composite build block of chapter 1 appears verbatim in settings.gradle.kts")
        .contains(block);
  }

  @Test
  void theWholeFileListingIsTheDemosFileApartFromItsIncludeList() throws IOException {
    List<String> listing = listingAfter(FILE_INTRO);
    List<String> settings = Files.readAllLines(SETTINGS);

    assertThat(withoutInclude(listing))
        .as("Step 1's settings.gradle.kts, include(...) call left out")
        .containsExactlyElementsOf(withoutInclude(settings));
  }

  @Test
  void theListingLeavesOutExactlyTheModulesTheNoteNames() throws IOException {
    Set<String> listed = modules(includeOf(listingAfter(FILE_INTRO)));
    Set<String> demo = modules(includeOf(Files.readAllLines(SETTINGS)));
    String note =
        Files.readAllLines(CHAPTER).stream()
            .filter(line -> line.startsWith(NOTE_START))
            .findFirst()
            .orElseThrow(() -> new AssertionError("chapter 1 has no note on the include list"));

    Set<String> leftOut = new TreeSet<>(demo);
    leftOut.removeAll(listed);
    assertThat(demo).as("the listing includes only modules the demo has").containsAll(listed);
    assertThat(leftOut).as("the modules the note names").isEqualTo(modules(List.of(note)));
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

  /** The lines from {@code include(} to its closing parenthesis. */
  private static List<String> includeOf(List<String> lines) {
    return lines.subList(includeStart(lines), includeEnd(lines) + 1);
  }

  /** Every line but the {@code include(...)} call, trailing blank lines removed. */
  private static List<String> withoutInclude(List<String> lines) {
    List<String> rest = new ArrayList<>(lines);
    rest.subList(includeStart(lines), includeEnd(lines) + 1).clear();
    while (!rest.isEmpty() && rest.getLast().isBlank()) {
      rest.removeLast();
    }
    return rest;
  }

  private static int includeStart(List<String> lines) {
    for (int i = 0; i < lines.size(); i++) {
      if (lines.get(i).startsWith("include(")) {
        return i;
      }
    }
    throw new AssertionError("no include(...) call in " + lines);
  }

  private static int includeEnd(List<String> lines) {
    for (int i = includeStart(lines); i < lines.size(); i++) {
      if (lines.get(i).strip().equals(")")) {
        return i;
      }
    }
    throw new AssertionError("the include(...) call is not closed in " + lines);
  }

  private static Set<String> modules(List<String> lines) {
    Set<String> modules = new TreeSet<>();
    for (String line : lines) {
      Matcher module = MODULE.matcher(line);
      while (module.find()) {
        modules.add(module.group(1));
      }
    }
    return modules;
  }
}
