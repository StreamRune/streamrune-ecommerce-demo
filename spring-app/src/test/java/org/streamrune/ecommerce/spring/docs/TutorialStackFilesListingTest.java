package org.streamrune.ecommerce.spring.docs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * The tutorial creates the files that run the demo's stack, {@code docker-compose.yml} and the two
 * Dockerfiles, and grows the Compose file as the chapters add services: each chapter that changes
 * it lists the whole file as it stands at the end of that chapter. This test holds every such
 * listing to the repository's file: an earlier listing is the file with the services of the later
 * chapters left out (its lines are lines of the file, in the file's order), and the last listing is
 * the file, line for line.
 *
 * <p>A listing of a file is the fenced block that follows a line naming the file in backticks and
 * ending in a colon, such as "Create {@code `docker-compose.yml`} in the project root:", and is
 * fenced as {@code yaml} (the Compose file) or {@code dockerfile}. Every block of the tutorial that
 * holds a Compose file (a {@code services:} line) or a Dockerfile is a listing the test reads.
 */
class TutorialStackFilesListingTest {

  private static final Path TUTORIAL = Path.of("../docs/tutorial");

  private static final Path REPOSITORY = Path.of("..");

  /** The stack's files the tutorial lists, relative to the repository root. */
  private static final List<String> FILES =
      List.of("docker-compose.yml", "Dockerfile.backend", "notifications-service/Dockerfile");

  @Test
  void everyListingOfAStackFileIsTheRepositorysFileSoFar() throws IOException {
    List<String> problems = new ArrayList<>();
    Set<String> read = new HashSet<>();
    for (String name : FILES) {
      List<String> file = Files.readAllLines(REPOSITORY.resolve(name));
      List<Listing> listings = listingsOf(name);
      listings.forEach(listing -> read.add(listing.where()));
      if (listings.isEmpty()) {
        problems.add(name + ": no chapter lists it");
        continue;
      }
      for (Listing listing : listings) {
        if (!isSubsequence(listing.lines(), file)) {
          problems.add(
              listing.where()
                  + ": the listing of "
                  + name
                  + " has lines the file does not have,"
                  + " or in another order");
        }
      }
      Listing last = listings.getLast();
      if (!last.lines().equals(file)) {
        problems.add(last.where() + ": the last listing of " + name + " is not the whole file");
      }
    }
    for (String block : stackFileBlocks()) {
      if (!read.contains(block)) {
        problems.add(
            block
                + ": a block of a stack file that is not read as a listing (its intro names the"
                + " file in backticks and ends in a colon)");
      }
    }

    assertThat(problems).as("listings of the stack's files").isEmpty();
  }

  /**
   * Every block that holds a Compose file or a Dockerfile, as chapter and line of its first line.
   */
  private static List<String> stackFileBlocks() throws IOException {
    List<String> blocks = new ArrayList<>();
    for (Path chapter : chapters()) {
      List<String> lines = Files.readAllLines(chapter);
      for (int i = 0; i < lines.size(); i++) {
        String fence = lines.get(i).strip();
        if (!fence.startsWith("```")) {
          continue;
        }
        String language = fence.substring(3).strip();
        int end = i + 1;
        boolean compose = false;
        while (end < lines.size() && !lines.get(end).strip().startsWith("```")) {
          compose |= lines.get(end).startsWith("services:");
          end++;
        }
        if (language.equals("dockerfile") || (language.equals("yaml") && compose)) {
          blocks.add(chapter.getFileName() + ":" + (i + 2));
        }
        i = end;
      }
    }
    return blocks;
  }

  /** The listings of one file, in the order a reader meets them. */
  private static List<Listing> listingsOf(String name) throws IOException {
    String mention = "`" + name + "`";
    String fence = "```" + (name.endsWith(".yml") ? "yaml" : "dockerfile");
    List<Listing> listings = new ArrayList<>();
    for (Path chapter : chapters()) {
      List<String> lines = Files.readAllLines(chapter);
      for (int i = 0; i < lines.size(); i++) {
        String intro = lines.get(i).strip();
        if (intro.startsWith("```")
            || !intro.contains(mention)
            || !(intro.endsWith(":") || intro.endsWith(":**"))) {
          continue;
        }
        int open = i + 1;
        while (open < lines.size() && lines.get(open).isBlank()) {
          open++;
        }
        if (open >= lines.size() || !lines.get(open).strip().equals(fence)) {
          continue;
        }
        List<String> block = new ArrayList<>();
        for (int j = open + 1; j < lines.size() && !lines.get(j).startsWith("```"); j++) {
          block.add(lines.get(j));
        }
        listings.add(new Listing(chapter.getFileName() + ":" + (open + 2), block));
      }
    }
    return listings;
  }

  /** Whether every line of {@code part} is a line of {@code whole}, in the same order. */
  private static boolean isSubsequence(List<String> part, List<String> whole) {
    int next = 0;
    for (String line : part) {
      while (next < whole.size() && !whole.get(next).equals(line)) {
        next++;
      }
      if (next == whole.size()) {
        return false;
      }
      next++;
    }
    return true;
  }

  private static List<Path> chapters() throws IOException {
    try (Stream<Path> files = Files.list(TUTORIAL)) {
      return files.filter(p -> p.toString().endsWith(".md")).sorted().toList();
    }
  }

  private record Listing(String where, List<String> lines) {}
}
