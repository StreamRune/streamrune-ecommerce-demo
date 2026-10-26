package org.streamrune.ecommerce.spring.docs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
 * ending in a colon, such as "Create {@code `docker-compose.yml`} in the project root:".
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
    for (String name : FILES) {
      List<String> file = Files.readAllLines(REPOSITORY.resolve(name));
      List<Listing> listings = listingsOf(name);
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

    assertThat(problems).as("listings of the stack's files").isEmpty();
  }

  /** The listings of one file, in the order a reader meets them. */
  private static List<Listing> listingsOf(String name) throws IOException {
    String mention = "`" + name + "`";
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
        if (open >= lines.size() || !lines.get(open).startsWith("```")) {
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
