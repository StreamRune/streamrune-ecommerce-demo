package org.streamrune.ecommerce.spring.docs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Every complete Java file the tutorial lists compiles against the framework and the demo modules,
 * together with the files the chapters before it listed. A reader who follows the chapters in order
 * builds exactly that set of files, so a listing that drifted from the API (a command type that
 * does not extend {@code Command}, a parameter that became a value object, a constructor that grew
 * a component) fails here rather than in the reader's first build.
 *
 * <p>A listing is a complete file when its first line is a {@code package} declaration. A later
 * listing of the same class replaces the earlier one, as it does in the reader's project. Classes
 * the tutorial never lists in full come from the demo's compiled modules on the test classpath. The
 * fragments a chapter asks the reader to paste into an existing file (bean methods, extra test
 * methods) are not compiled, as they need the file around them; the imports they tell the reader to
 * add must still resolve.
 */
class TutorialListingsCompileTest {

  private static final Path TUTORIAL = Path.of("../docs/tutorial");

  /** A top-level type declaration: the first one in a listing names the file. */
  private static final Pattern TOP_LEVEL_TYPE =
      Pattern.compile(
          "^(?:(?:public|final|abstract|sealed|non-sealed)\\s+)*"
              + "(?:class|interface|record|enum|@interface)\\s+(\\w+)");

  private static final Pattern PACKAGE = Pattern.compile("^package\\s+([\\w.]+)\\s*;");

  private static final Pattern IMPORT =
      Pattern.compile("^import\\s+(?:static\\s+)?[\\w.]+(?:\\.\\*)?\\s*;");

  @Test
  void everyChapterCompilesOnTopOfTheListingsBeforeIt(@TempDir Path out) throws IOException {
    JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
    assertThat(javac).as("the tests run on a JDK with a Java compiler").isNotNull();

    Map<String, Listing> project = new LinkedHashMap<>();
    Set<String> errors = new LinkedHashSet<>();
    int compiledChapters = 0;
    for (Path chapter : chapters()) {
      List<Listing> listings = completeFiles(chapter);
      if (listings.isEmpty()) {
        continue;
      }
      listings.forEach(listing -> project.put(listing.className(), listing));
      Path classes = Files.createDirectories(out.resolve(chapter.getFileName().toString()));
      errors.addAll(compile(javac, List.copyOf(project.values()), classes));
      compiledChapters++;
    }

    assertThat(compiledChapters).as("chapters with complete Java files").isGreaterThan(10);
    assertThat(project).as("complete Java files the tutorial lists").hasSizeGreaterThan(50);
    assertThat(errors).as("compile errors in the tutorial's listings").isEmpty();
  }

  @Test
  void everyImportAFragmentShowsResolves(@TempDir Path out) throws IOException {
    JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
    assertThat(javac).as("the tests run on a JDK with a Java compiler").isNotNull();

    // One compilation unit per import line: two fragments may import different classes that
    // share a simple name, which one file could not hold.
    List<Listing> imports = new ArrayList<>();
    for (Path chapter : chapters()) {
      for (Block block : blocks(chapter)) {
        if (block.isCompleteFile()) {
          continue;
        }
        for (int i = 0; i < block.lines().size(); i++) {
          Matcher m = IMPORT.matcher(block.lines().get(i));
          if (m.find()) {
            String type = "Import" + imports.size();
            imports.add(
                new Listing(
                    block.chapter(),
                    block.firstLine() + i,
                    "tutorial.fragments." + type,
                    "package tutorial.fragments; " + m.group() + " final class " + type + " {}\n"));
          }
        }
      }
    }

    assertThat(imports).as("imports the tutorial's fragments show").hasSizeGreaterThan(20);
    assertThat(compile(javac, imports, out)).as("imports that do not resolve").isEmpty();
  }

  private static List<Path> chapters() throws IOException {
    try (Stream<Path> files = Files.list(TUTORIAL)) {
      return files.filter(p -> p.toString().endsWith(".md")).sorted().toList();
    }
  }

  /** The {@code ```java} blocks of one chapter that are complete files, in reading order. */
  private static List<Listing> completeFiles(Path chapter) throws IOException {
    List<Listing> listings = new ArrayList<>();
    for (Block block : blocks(chapter)) {
      if (!block.isCompleteFile()) {
        continue;
      }
      Matcher pkg = PACKAGE.matcher(block.lines().getFirst());
      pkg.find();
      String type =
          block.lines().stream()
              .map(TOP_LEVEL_TYPE::matcher)
              .filter(Matcher::find)
              .map(m -> m.group(1))
              .findFirst()
              .orElseThrow(
                  () ->
                      new AssertionError(
                          block.chapter() + ":" + block.firstLine() + " declares no type"));
      listings.add(
          new Listing(
              block.chapter(),
              block.firstLine(),
              pkg.group(1) + "." + type,
              String.join("\n", block.lines()) + "\n"));
    }
    return listings;
  }

  /** Every {@code ```java} block of one chapter, with the Markdown line its first line is on. */
  private static List<Block> blocks(Path chapter) throws IOException {
    List<String> lines = Files.readAllLines(chapter);
    List<Block> blocks = new ArrayList<>();
    for (int i = 0; i < lines.size(); i++) {
      if (!lines.get(i).strip().equals("```java")) {
        continue;
      }
      int first = i + 1;
      int end = first;
      while (end < lines.size() && !lines.get(end).strip().startsWith("```")) {
        end++;
      }
      blocks.add(
          new Block(
              chapter.getFileName().toString(), first + 1, List.copyOf(lines.subList(first, end))));
      i = end;
    }
    return blocks;
  }

  private static List<String> compile(JavaCompiler javac, List<Listing> listings, Path classes)
      throws IOException {
    DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
    Map<JavaFileObject, Listing> sources = new LinkedHashMap<>();
    for (Listing listing : listings) {
      sources.put(new ListingSource(listing), listing);
    }
    List<String> options =
        List.of(
            "--release",
            String.valueOf(Runtime.version().feature()),
            "--enable-preview",
            "-proc:none",
            "-implicit:none",
            "-nowarn",
            "-classpath",
            System.getProperty("java.class.path"),
            "-d",
            classes.toString());
    try (StandardJavaFileManager files =
        javac.getStandardFileManager(diagnostics, Locale.ROOT, null)) {
      javac.getTask(null, files, diagnostics, options, null, sources.keySet()).call();
    }
    List<String> errors = new ArrayList<>();
    for (Diagnostic<? extends JavaFileObject> d : diagnostics.getDiagnostics()) {
      if (d.getKind() != Diagnostic.Kind.ERROR) {
        continue;
      }
      Listing listing = d.getSource() == null ? null : sources.get(d.getSource());
      String where =
          listing == null
              ? "javac"
              : listing.chapter() + ":" + (listing.firstLine() + d.getLineNumber() - 1);
      errors.add(where + ": " + d.getMessage(Locale.ROOT).replace('\n', ' '));
    }
    return errors;
  }

  /** One {@code ```java} block: the chapter, the Markdown line its first line is on, its lines. */
  private record Block(String chapter, int firstLine, List<String> lines) {
    boolean isCompleteFile() {
      return !lines.isEmpty() && PACKAGE.matcher(lines.getFirst()).find();
    }
  }

  /** One compilation unit: where its first line is in the tutorial, its class and its source. */
  private record Listing(String chapter, int firstLine, String className, String source) {}

  private static final class ListingSource extends SimpleJavaFileObject {
    private final String source;

    ListingSource(Listing listing) {
      super(
          URI.create("string:///" + listing.className().replace('.', '/') + Kind.SOURCE.extension),
          Kind.SOURCE);
      this.source = listing.source();
    }

    @Override
    public CharSequence getCharContent(boolean ignoreEncodingErrors) {
      return source;
    }
  }
}
