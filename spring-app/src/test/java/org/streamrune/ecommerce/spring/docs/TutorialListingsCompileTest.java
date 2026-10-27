package org.streamrune.ecommerce.spring.docs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
 * A reader who follows the tutorial in order has, at the end of chapter N, exactly the files that
 * chapters 1 to N list in full, each in the version of the last chapter that listed it. This test
 * builds that project for every chapter and compiles it against the framework and the third-party
 * libraries alone, without the demo's own classes: a listing that uses a type a later chapter
 * introduces, or a class no chapter lists, fails here instead of in the reader's build.
 *
 * <p><b>Which listings count.</b> A listing is a complete file when its first line is a {@code
 * package} declaration; its first top-level type names the file. A later listing of the same file
 * replaces the earlier one, as it does in the reader's project. The fragments a chapter shows
 * inside the prose (a method, a few lines of a builder) are not files: they are not compiled, but
 * every import they show must resolve against the project of their chapter.
 *
 * <p><b>The classpath.</b> The test classpath of {@code spring-app} minus every entry under the
 * demo's root directory: the framework (the {@code ../streamrune} composite build or the published
 * artifacts) and the third-party libraries stay, the demo modules' compiled classes go.
 *
 * <p><b>The Spring configuration.</b> Each listing of {@code StreamRuneConfig} grows out of the
 * listing before it, apart from the lines a chapter rewrites ({@link #REWRITES}).
 *
 * <p><b>The finished project.</b> After the last chapter the reader's files are the repository's:
 * the last listing of every file the tutorial lists in full is that file, character for character.
 */
class TutorialListingsCompileTest {

  private static final Path TUTORIAL = Path.of("../docs/tutorial");

  private static final Path REPOSITORY = Path.of("..").toAbsolutePath().normalize();

  /** A top-level type declaration: the first one in a listing names the file. */
  private static final Pattern TOP_LEVEL_TYPE =
      Pattern.compile(
          "^(?:(?:public|final|abstract|sealed|non-sealed)\\s+)*"
              + "(?:class|interface|record|enum|@interface)\\s+(\\w+)");

  private static final Pattern PACKAGE = Pattern.compile("^package\\s+([\\w.]+)\\s*;");

  private static final Pattern IMPORT =
      Pattern.compile("^import\\s+(?:static\\s+)?[\\w.]+(?:\\.\\*)?\\s*;");

  /** A Spring bean method's annotation. */
  private static final Pattern BEAN = Pattern.compile("^\\s*@Bean\\b");

  /** The source sets a listed file can live in, in every module of the demo. */
  private static final List<String> SOURCE_SETS = List.of("src/main/java", "src/test/java");

  /** The Spring configuration, which every chapter that changes it lists in full. */
  private static final String CONFIGURATION =
      "org.streamrune.ecommerce.spring.config.StreamRuneConfig";

  /**
   * The lines of {@code StreamRuneConfig} that a chapter rewrites instead of extending: the chapter
   * of one listing, the chapter of the next listing, the bean, and the start of each line of that
   * bean's method in the earlier listing that the later chapter changes.
   */
  private static final List<Rewrite> REWRITES =
      List.of(
          // Chapter 6 builds the forget service without a snapshot purge; chapter 7 gives it the
          // audit store, the purgers and the query cache, and a javadoc that says so.
          new Rewrite(
              6,
              7,
              "forgetSubjectService",
              List.of(
                  "* The GDPR erasure service:",
                  "* crypto-shreds the",
                  "* builder refuses a service",
                  "* states that no aggregate snapshot",
                  "return ForgetSubjectService.builder()")),
          // Chapter 14's javadoc of the saga runner names the resume route chapter 14 adds.
          new Rewrite(
              13,
              14,
              "orderFulfillmentSagaRunner",
              List.of("* streamrune.saga.compensation-retry-give-up-after} (1 hour by default)")),
          // Up to chapter 14 the locker builds its own pool from the connection settings; from
          // chapter 15 on the app builds the pool, with its meters, and hands it to the locker.
          new Rewrite(
              14,
              15,
              "aggregateLocker",
              List.of(
                  "* ({@code @ConditionalOnMissingBean(AggregateLocker.class)}). The locker owns",
                  "* Spring closes with it.",
                  "@Bean(destroyMethod = \"close\")",
                  "@Value(\"${spring.datasource.url}\") String url,",
                  "@Value(\"${spring.datasource.username}\") String username,",
                  "@Value(\"${spring.datasource.password}\") String password) {",
                  "return PgAdvisoryLocker.withDedicatedPool(url, username, password, 10);")),
          // Chapter 17 keeps the bus builder in a variable, to add the saga command delay of the
          // two-backend runs.
          new Rewrite(15, 17, "commandBus", List.of("return VirtualThreadCommandBus.builder()")));

  /** The marker a comment line starts with, and the end of a comment at the end of a line. */
  private static final Pattern COMMENT_MARKER = Pattern.compile("^\\s*(?:/\\*\\*|\\*/|\\*|//)");

  private static final Pattern COMMENT_END = Pattern.compile("\\*/\\s*$");

  /** A string literal, a word or number, or a single symbol. */
  private static final Pattern TOKEN = Pattern.compile("\"(?:[^\"\\\\]|\\\\.)*\"|[\\w$]+|\\S");

  /** A line of a bean method's javadoc or annotations. */
  private static final Pattern COMMENT_OR_ANNOTATION = Pattern.compile("^\\s*(?:@|/\\*\\*|\\*|//)");

  @Test
  void everyChapterCompilesFromTheListingsOfItsChaptersAlone(@TempDir Path out) throws IOException {
    JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
    assertThat(javac).as("the tests run on a JDK with a Java compiler").isNotNull();
    String classpath = frameworkAndLibraries();

    Map<String, Listing> project = new LinkedHashMap<>();
    Map<String, String> errors = new LinkedHashMap<>();
    int compiledChapters = 0;
    for (Path chapter : chapters()) {
      List<Listing> listings = completeFiles(chapter);
      if (listings.isEmpty()) {
        continue;
      }
      listings.forEach(listing -> project.put(listing.className(), listing));
      String name = chapter.getFileName().toString();
      Path classes = Files.createDirectories(out.resolve(name));
      for (String error : compile(javac, List.copyOf(project.values()), classpath, classes)) {
        errors.putIfAbsent(error, name);
      }
      compiledChapters++;
    }

    assertThat(compiledChapters).as("chapters with complete Java files").isGreaterThan(10);
    assertThat(project).as("complete Java files the tutorial lists").hasSizeGreaterThan(50);
    assertThat(
            errors.entrySet().stream()
                .map(e -> e.getKey() + " (the project of " + e.getValue() + ")")
                .toList())
        .as("compile errors in the project each chapter leaves the reader with")
        .isEmpty();
  }

  @Test
  void everyImportAFragmentShowsResolvesInItsChapter(@TempDir Path out) throws IOException {
    JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
    assertThat(javac).as("the tests run on a JDK with a Java compiler").isNotNull();
    String classpath = frameworkAndLibraries();

    Map<String, Listing> project = new LinkedHashMap<>();
    List<String> errors = new ArrayList<>();
    int imports = 0;
    Path classes = null;
    for (Path chapter : chapters()) {
      String name = chapter.getFileName().toString();
      List<Listing> listings = completeFiles(chapter);
      if (!listings.isEmpty()) {
        listings.forEach(listing -> project.put(listing.className(), listing));
        classes = Files.createDirectories(out.resolve(name));
        compile(javac, List.copyOf(project.values()), classpath, classes);
      }
      // One compilation unit per import line: two fragments may import different classes that
      // share a simple name, which one file could not hold.
      List<Listing> units = new ArrayList<>();
      for (Block block : blocks(chapter)) {
        if (block.isCompleteFile()) {
          continue;
        }
        for (int i = 0; i < block.lines().size(); i++) {
          Matcher m = IMPORT.matcher(block.lines().get(i));
          if (m.find()) {
            String type = "Import" + imports++;
            units.add(
                new Listing(
                    name,
                    block.firstLine() + i,
                    "tutorial.fragments." + type,
                    "package tutorial.fragments; " + m.group() + " final class " + type + " {}\n"));
          }
        }
      }
      if (!units.isEmpty()) {
        String withChapter = classes == null ? classpath : classpath + File.pathSeparator + classes;
        Path stubs = Files.createDirectories(out.resolve(name + "-imports"));
        errors.addAll(compile(javac, units, withChapter, stubs));
      }
    }

    assertThat(imports).as("imports the tutorial's fragments show").isPositive();
    assertThat(errors).as("imports that do not resolve in their chapter").isEmpty();
  }

  /**
   * A chapter that changes the Spring configuration lists the whole class as it stands at the end
   * of the chapter. The bean methods its text shows on their own are excerpts of that listing, line
   * for line apart from the indentation: a bean method shown in another shape (an older one, or one
   * with parts left out) is what a reader pastes into the file, and it replaces the bean the
   * chapters before built, wiring and all.
   */
  @Test
  void aBeanMethodAChapterShowsIsAnExcerptOfItsListingOfTheClass() throws IOException {
    List<String> fragments = new ArrayList<>();
    for (Path chapter : chapters()) {
      List<Block> blocks = blocks(chapter);
      List<List<String>> files =
          blocks.stream().filter(Block::isCompleteFile).map(b -> stripped(b.lines())).toList();
      for (Block block : blocks) {
        if (block.isCompleteFile() || block.lines().stream().noneMatch(BEAN.asPredicate())) {
          continue;
        }
        List<String> excerpt = stripped(block.lines());
        if (files.stream().noneMatch(file -> Collections.indexOfSubList(file, excerpt) >= 0)) {
          fragments.add(block.chapter() + ":" + block.firstLine());
        }
      }
    }

    assertThat(fragments)
        .as("bean methods that are not an excerpt of a listing of their class in their chapter")
        .isEmpty();
  }

  /** The lines of a block without their indentation, blank lines left out. */
  private static List<String> stripped(List<String> lines) {
    return lines.stream().map(String::strip).filter(line -> !line.isEmpty()).toList();
  }

  /**
   * The listings of {@code StreamRuneConfig} between two fragments are the reader's file as much as
   * the ones a fragment pins, so each one grows out of the one before it: its words, strings and
   * symbols, read without the comment markers, are in their order words, strings and symbols of the
   * next listing. Reading them so lets a bean gain a parameter or a javadoc wrap anew, which moves
   * the line breaks and keeps every word. A line that a chapter rewrites instead of extending is in
   * {@link #REWRITES}, and that line of the earlier listing is left out of the comparison, the rest
   * of its bean method not. A change that reaches some listings from its chapter on and misses the
   * ones between fails here.
   */
  @Test
  void eachListingOfTheSpringConfigurationGrowsOutOfTheOneBefore() throws IOException {
    List<Listing> listings = new ArrayList<>();
    for (Path chapter : chapters()) {
      for (Listing listing : completeFiles(chapter)) {
        if (listing.className().equals(CONFIGURATION)) {
          listings.add(listing);
        }
      }
    }

    List<String> problems = new ArrayList<>();
    Set<Rewrite> applied = new HashSet<>();
    Set<Rewrite> needed = new HashSet<>();
    for (int i = 1; i < listings.size(); i++) {
      Listing earlier = listings.get(i - 1);
      Listing later = listings.get(i);
      List<String> lines = earlier.source().lines().toList();
      List<Token> next = tokens(later.source().lines().toList(), Set.of());
      Set<Integer> rewritten = new HashSet<>();
      for (Rewrite rewrite : REWRITES) {
        if (rewrite.from() != chapterNumber(earlier) || rewrite.to() != chapterNumber(later)) {
          continue;
        }
        applied.add(rewrite);
        Set<Integer> method = beanMethod(lines, rewrite.bean());
        if (method.isEmpty()
            || beanMethod(later.source().lines().toList(), rewrite.bean()).isEmpty()) {
          problems.add(rewrite + ": the bean is not in both listings");
        }
        for (String start : rewrite.lines()) {
          List<Integer> changed =
              method.stream().filter(k -> lines.get(k).strip().startsWith(start)).toList();
          if (changed.size() != 1) {
            problems.add(
                rewrite.bean() + ": [" + start + "] does not begin exactly one line of it");
          }
          rewritten.addAll(changed);
        }
        if (firstMissing(tokens(lines, Set.of()), next) != null) {
          needed.add(rewrite);
        }
      }
      Token missing = firstMissing(tokens(lines, rewritten), next);
      if (missing != null) {
        problems.add(
            earlier.chapter()
                + ":"
                + (earlier.firstLine() + missing.line())
                + " ["
                + lines.get(missing.line()).strip()
                + "]: from here on the listing is not, in its order, in the listing of "
                + later.chapter());
      }
    }
    for (Rewrite rewrite : REWRITES) {
      if (!applied.contains(rewrite)) {
        problems.add(
            rewrite + ": no listing of the first chapter is followed by one of the second");
      } else if (!needed.contains(rewrite)) {
        problems.add(rewrite + ": the listings grow without it");
      }
    }

    assertThat(listings).as("listings of StreamRuneConfig").hasSizeGreaterThan(10);
    assertThat(problems)
        .as("listings of StreamRuneConfig that drop what the listing before has")
        .isEmpty();
  }

  /** The chapter's number, from its file name. */
  private static int chapterNumber(Listing listing) {
    return Integer.parseInt(listing.chapter().substring(0, 2));
  }

  /** The lines of the bean method {@code name} with its javadoc and annotations, or none. */
  private static Set<Integer> beanMethod(List<String> lines, String name) {
    Pattern declaration =
        Pattern.compile("^\\s+public\\s+[\\w.<>?, ]+\\s" + Pattern.quote(name) + "\\(");
    for (int start = 0; start < lines.size(); start++) {
      if (!declaration.matcher(lines.get(start)).find()) {
        continue;
      }
      int first = start;
      while (first > 0 && COMMENT_OR_ANNOTATION.matcher(lines.get(first - 1)).find()) {
        first--;
      }
      int depth = 0;
      boolean opened = false;
      for (int end = start; end < lines.size(); end++) {
        depth += braces(lines.get(end));
        opened |= depth > 0;
        if (opened && depth == 0) {
          return IntStream.rangeClosed(first, end).boxed().collect(Collectors.toSet());
        }
      }
    }
    return Set.of();
  }

  /** The opening braces of a line minus its closing braces. */
  private static int braces(String line) {
    return (int)
        (line.chars().filter(c -> c == '{').count() - line.chars().filter(c -> c == '}').count());
  }

  /** The tokens of the lines not left out, read without the comment markers. */
  private static List<Token> tokens(List<String> lines, Set<Integer> leftOut) {
    List<Token> tokens = new ArrayList<>();
    for (int i = 0; i < lines.size(); i++) {
      if (leftOut.contains(i)) {
        continue;
      }
      String text = COMMENT_MARKER.matcher(lines.get(i)).replaceFirst("");
      Matcher token = TOKEN.matcher(COMMENT_END.matcher(text).replaceFirst(""));
      while (token.find()) {
        tokens.add(new Token(i, token.group()));
      }
    }
    return tokens;
  }

  /** The first token of {@code earlier} that {@code later} does not have in the same order. */
  private static Token firstMissing(List<Token> earlier, List<Token> later) {
    int at = 0;
    for (Token token : earlier) {
      while (at < later.size() && !later.get(at).text().equals(token.text())) {
        at++;
      }
      if (at == later.size()) {
        return token;
      }
      at++;
    }
    return null;
  }

  /**
   * A listing of a file starts with its {@code package} declaration, which makes it a file to the
   * tests of this class. A block without one that declares a type the repository has a file of is
   * such a listing with its first line missing, and every check here would pass it over.
   */
  @Test
  void aBlockThatDeclaresAFileOfTheRepositoryStartsWithItsPackage() throws IOException {
    Set<String> files = repositoryTypeNames();
    List<String> blocks = new ArrayList<>();
    for (Path chapter : chapters()) {
      for (Block block : blocks(chapter)) {
        if (block.isCompleteFile()) {
          continue;
        }
        for (String line : block.lines()) {
          Matcher type = TOP_LEVEL_TYPE.matcher(line);
          if (type.find() && files.contains(type.group(1))) {
            blocks.add(block.chapter() + ":" + block.firstLine() + " " + type.group(1));
          }
        }
      }
    }

    assertThat(files).as("Java files of the demo's modules").hasSizeGreaterThan(100);
    assertThat(blocks)
        .as("blocks that declare a file of the repository without its package line")
        .isEmpty();
  }

  /** The simple names of the Java files in the source sets of the demo's modules. */
  private static Set<String> repositoryTypeNames() throws IOException {
    Set<String> names = new HashSet<>();
    try (Stream<Path> modules = Files.list(REPOSITORY)) {
      for (Path module : modules.filter(Files::isDirectory).toList()) {
        for (String sourceSet : SOURCE_SETS) {
          Path root = module.resolve(sourceSet);
          if (!Files.isDirectory(root)) {
            continue;
          }
          try (Stream<Path> files = Files.walk(root)) {
            files
                .map(file -> file.getFileName().toString())
                .filter(file -> file.endsWith(".java"))
                .map(file -> file.substring(0, file.length() - ".java".length()))
                .forEach(names::add);
          }
        }
      }
    }
    return names;
  }

  @Test
  void theLastListingOfEveryFileIsTheRepositorysFile() throws IOException {
    Map<String, Listing> last = new LinkedHashMap<>();
    for (Path chapter : chapters()) {
      completeFiles(chapter).forEach(listing -> last.put(listing.className(), listing));
    }

    List<String> differences = new ArrayList<>();
    for (Listing listing : last.values()) {
      List<Path> files = repositoryFiles(listing.className());
      String where = listing.chapter() + ":" + listing.firstLine() + " " + listing.className();
      if (files.size() != 1) {
        differences.add(where + ": the repository has " + files.size() + " such files " + files);
        continue;
      }
      String file = Files.readString(files.getFirst());
      if (!file.equals(listing.source())) {
        differences.add(
            where
                + " differs from "
                + REPOSITORY.relativize(files.getFirst())
                + firstDifference(listing.source(), file));
      }
    }

    assertThat(last).as("files the tutorial lists in full").hasSizeGreaterThan(50);
    assertThat(differences)
        .as("last listings that are not the repository's file, character for character")
        .isEmpty();
  }

  /**
   * The project of a chapter compiles against the framework and the libraries alone. The test
   * classpath holds the demo's compiled modules; the classpath the chapters compile against holds
   * none of them, and a class of the demo that resolves on the first does not resolve on the
   * second.
   */
  @Test
  void theChaptersCompileWithoutTheDemosClasses(@TempDir Path out) throws IOException {
    JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
    assertThat(javac).as("the tests run on a JDK with a Java compiler").isNotNull();
    String testClasspath = System.getProperty("java.class.path");
    String classpath = frameworkAndLibraries();
    assertThat(underRepository(testClasspath))
        .as("entries of the test classpath under the demo's root")
        .isNotEmpty();
    assertThat(underRepository(classpath))
        .as("entries of the chapters' classpath under the demo's root")
        .isEmpty();

    Listing probe =
        new Listing(
            "a probe",
            1,
            "tutorial.probe.Probe",
            "package tutorial.probe;"
                + " final class Probe { org.streamrune.ecommerce.domain.common.Money money; }\n");
    assertThat(
            compile(
                javac, List.of(probe), testClasspath, Files.createDirectories(out.resolve("a"))))
        .as("a class of the demo, against the test classpath")
        .isEmpty();
    assertThat(compile(javac, List.of(probe), classpath, Files.createDirectories(out.resolve("b"))))
        .as("a class of the demo, against the chapters' classpath")
        .anyMatch(error -> error.contains("package org.streamrune.ecommerce.domain.common"));
  }

  /** The test classpath without the entries under the demo's root: the demo modules' classes. */
  static String frameworkAndLibraries() {
    List<String> demo = underRepository(System.getProperty("java.class.path"));
    String kept =
        Stream.of(System.getProperty("java.class.path").split(File.pathSeparator))
            .filter(entry -> !entry.isBlank())
            .filter(entry -> !demo.contains(entry))
            .collect(Collectors.joining(File.pathSeparator));
    assertThat(kept).as("the framework is on the test classpath").contains("streamrune-core");
    return kept;
  }

  /** The entries of a classpath under the demo's root directory. */
  private static List<String> underRepository(String classpath) {
    return Stream.of(classpath.split(File.pathSeparator))
        .filter(entry -> !entry.isBlank())
        .filter(entry -> Path.of(entry).toAbsolutePath().normalize().startsWith(REPOSITORY))
        .toList();
  }

  /** The files of the demo's modules that hold {@code className}. */
  private static List<Path> repositoryFiles(String className) throws IOException {
    String relative = className.replace('.', '/') + ".java";
    List<Path> files = new ArrayList<>();
    try (Stream<Path> modules = Files.list(REPOSITORY)) {
      for (Path module : modules.filter(Files::isDirectory).sorted().toList()) {
        for (String sourceSet : SOURCE_SETS) {
          Path file = module.resolve(sourceSet).resolve(relative);
          if (Files.isRegularFile(file)) {
            files.add(file);
          }
        }
      }
    }
    return files;
  }

  /** Where two texts part, as the line number and both lines, for a failure message. */
  private static String firstDifference(String listing, String file) {
    List<String> a = listing.lines().toList();
    List<String> b = file.lines().toList();
    for (int i = 0; i < Math.max(a.size(), b.size()); i++) {
      String left = i < a.size() ? a.get(i) : "<end of listing>";
      String right = i < b.size() ? b.get(i) : "<end of file>";
      if (!left.equals(right)) {
        return " at line " + (i + 1) + ": listing [" + left + "] file [" + right + "]";
      }
    }
    return " in its line endings";
  }

  static List<Path> chapters() throws IOException {
    try (Stream<Path> files = Files.list(TUTORIAL)) {
      return files.filter(p -> p.toString().endsWith(".md")).sorted().toList();
    }
  }

  /** The {@code ```java} blocks of one chapter that are complete files, in reading order. */
  static List<Listing> completeFiles(Path chapter) throws IOException {
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

  static List<String> compile(
      JavaCompiler javac, List<Listing> listings, String classpath, Path classes)
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
            // The reader's build compiles with -parameters, as the Spring Boot plugin sets it:
            // Spring tells two beans of one type apart by the name of the parameter they go to.
            "-parameters",
            "-proc:none",
            "-implicit:none",
            "-nowarn",
            "-classpath",
            classpath,
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
  record Listing(String chapter, int firstLine, String className, String source) {}

  /**
   * Lines of a bean method that a chapter rewrites: the chapters of the two listings, the bean, and
   * how each changed line of the earlier listing begins (without its indentation).
   */
  private record Rewrite(int from, int to, String bean, List<String> lines) {}

  /** A word, string or symbol of a listing, and the index of its line. */
  private record Token(int line, String text) {}

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
