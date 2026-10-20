# AGENTS.md

Instructions for coding agents, and a reference for anyone who works on the StreamRune e-commerce
demo. Licensing, the Developer Certificate of Origin and commit messages are in `CONTRIBUTING.md`.

## 1. The repository

The demo is a complete CQRS and event-sourcing shop built on the StreamRune framework: five
aggregates (Product, Order, Customer, Payment, Inventory), projections, sagas, the transactional
outbox and the framework's other features. The same domain code runs in three applications, one per
framework, and each application adds only the HTTP transport and the wiring.

| Module | What it holds | Its tests need Docker |
|---|---|---|
| `domain/` | Events, commands, aggregate state, value types | no |
| `commands/` | The deciders: domain logic without I/O | no |
| `queries/` | The read-model views and the owner-or-admin access rule | no |
| `projections/` | The projections that build the views, and the reads as one caller may see them | no |
| `spring-app/` | The Spring Boot application, port 8080 | yes |
| `micronaut-app/` | The Micronaut application, port 8081 | yes |
| `quarkus-app/` | The Quarkus application, port 8082 | yes |
| `notifications-service/` | A Spring Boot service that consumes the integration events the outbox publishes to RabbitMQ, port 8090 | yes |
| `frontend/` | The Next.js frontend, port 3000, and its Playwright suite | — |

Where each topic is written down:

- `README.md`: the architecture, running the stack, the REST API and who may call what, native
  images.
- `docs/tutorial/`: seventeen chapters that build this application from an empty project, one
  framework feature at a time (`01-project-setup.md` to `17-full-demo.md`).
- `frontend/README.md`: the frontend, its scripts and its Playwright suite.

## 2. Build and test

**The framework comes from a sibling checkout when there is one.** With `../streamrune` present,
`settings.gradle.kts` includes it as a composite build and the demo compiles against that source;
without it, against the published preview in the Central snapshot repository. Every build says
which, either `StreamRune framework: composite ../streamrune @ <revision>` or, as a warning,
`StreamRune framework: Central snapshot 1.0.0-alpha-SNAPSHOT (no ../streamrune sibling)`. Pass
`-PrequireFrameworkSibling` to turn the second case into a failure. Clone the framework next to the
demo:

    git clone https://github.com/StreamRune/streamrune.git ../streamrune

The build uses a Java 25 toolchain that Gradle finds among the installed JDKs and does not download,
so install a JDK 25 first (GraalVM CE 25.0.1 builds the native images). Every module compiles, and
runs its tests, with `--enable-preview`.

| Command | What it runs | Typical wall time |
|---|---|---|
| `./gradlew quickCheck` | Compiles every module, runs the tests of `domain`, `commands`, `queries` and `projections` and, when `frontend/node_modules` exists, `npm run typecheck` and `npm run lint` in `frontend/` | under 20 seconds |
| `./gradlew build` | Every module's tests, the Testcontainers suites of the three applications and the notifications service included. Needs Docker. | about 6 minutes; the Spring application's suite is the longest |
| `./gradlew :spring-app:test` | One application's suite (likewise `:quarkus-app:test`, `:micronaut-app:test`). Needs Docker. | |
| `./gradlew :spring-app:test --tests "org.streamrune.ecommerce.spring.docs.*"` | The tutorial tests (section 5), without Docker | |
| `docker compose up -d postgres rabbitmq`, then `./gradlew :spring-app:bootRun` | One application against local PostgreSQL and RabbitMQ (`:micronaut-app:run`, `:quarkus-app:quarkusDev`) | |
| `./gradlew :spring-app:bootJar :notifications-service:bootJar`, then `docker compose up` | The full stack of the README's Quick Start | |
| `scripts/native-image-smoke-test.sh spring` (or `quarkus`, `micronaut`, `all`) | Builds an application as a native image and probes it on an empty database: health, the schema, a command and its read model, a dead-letter replay, a saga, shutdown. Needs Docker and a GraalVM JDK 25 on `PATH`. | |

In `frontend/`, after `npm ci`:

- `npm run typecheck` and `npm run lint`.
- `npm test` runs the Playwright suite (install the browser once with
  `npx playwright install chromium`). It needs no backend: most specs mock it, and the specs that
  need one skip when nothing answers on `http://localhost:8080`.
- `npm run test:live` runs the same suite with `E2E_REQUIRE_BACKEND=1` against a running stack, where
  an unreachable backend fails the run instead of skipping.

- **`quickCheck` is the inner loop, `build` is the gate.** A green `quickCheck` says nothing about
  the applications' suites: run `./gradlew build` before every pull request, with the framework
  branch the change needs checked out in `../streamrune`.
- **Exit codes.** Never pipe Gradle through `tail`, `grep` or `head` when its result matters: the
  pipeline reports the status of its last command, not Gradle's. Redirect and print the status
  instead, `./gradlew build > build.log 2>&1; echo "EXIT=$?"`, and start a full build in the
  background.
- **No schema scripts.** The applications create the framework's tables when they start
  (`streamrune.event-store.schema.auto-initialize=true` applies the Flyway migrations the framework
  ships), and the projections create the read-model tables on first use.
- **CI.** The required checks of a pull request are `Spring backend integration tests`,
  `Quarkus backend integration tests`, `Micronaut backend integration tests`, `Shared module tests`
  and `dco`. CI checks out the framework's `main` as the sibling, so a pull request is built against
  the framework's current source. The other jobs and workflows run the README's run paths (Docker
  Compose, `bootRun`, the live Playwright suite), the mocked Playwright suite and the native-image
  smoke test.

## 3. Conventions

- **The domain stays framework-agnostic.** The shared modules know no Spring, Quarkus or Micronaut
  type; the applications add only transport and wiring.
- **The three applications serve the same REST API.** A change to an endpoint, its authorization or
  its configuration lands in all three, with a test in each application's suite.
- **Explicit code.** What an application wires is in its configuration classes, and the main code
  defines no annotations of its own.
- **Commits** follow Conventional Commits with a scope, for example `fix(spring-app): …` (template:
  `.gitmessage`). Sign off every commit with `git commit --signoff`: the `dco` check rejects a
  commit without the trailer.
- **Pull requests** go to `main` and are squash-merged.

## 4. Testing conventions

- **A failing test first.** Every behaviour change starts with a test that fails without it.
- **One mutant per fix.** Prove that the test guards the change: remove the change, run the covering
  tests, see them fail, restore it.
- **Where a test belongs.** Decider tests in `commands/` use the framework's `DeciderFixture`; the
  access rules and the projections are tested in their modules; each application's suite drives the
  REST API against PostgreSQL 17 and RabbitMQ containers.
- **One application context for the Spring suite.** The Spring integration tests extend
  `AbstractIntegrationTest`: one set of containers and one application context serve every class. A
  class that changes the context's configuration (an `@Import` of its own, a mocked bean, other
  properties) starts a second application context, with its own pollers and runners, against the
  same database.

## 5. The tutorial follows the code

- **Every change to the demo updates the tutorial in the same pull request:** the chapter that shows
  the code, and every later chapter that lists or describes the same file, endpoint or behaviour.
- **A listing of a whole file** (it starts with its `package` declaration) shows the file as it is in
  the repository.
- **The tutorial tests** in `spring-app` (`org.streamrune.ecommerce.spring.docs`):
  `TutorialListingsCompileTest` compiles every complete listing, chapter by chapter, against the
  framework and the demo modules; `TutorialMetricNamesTest` checks that every meter name the
  tutorial and the README give is one the framework registers; `TutorialInterceptorOrderTest`
  checks every interceptor chain the tutorial shows against the framework's order. The `spring-app`
  test task re-runs them whenever a chapter or the README changes.
- **Public text.** Code, comments, the tutorial, the READMEs, commit messages and pull request
  descriptions are published. Write in the present tense: say what the code does and why, not how it
  came to be. No labels from issue trackers, reviews or private notes, and no space before a period.

## 6. Pitfalls

- **`git stash` is shared** by every worktree of a clone, so another session can pop an entry. In a
  worktree, set work aside with a commit on your branch.
- **The snapshot fallback.** Without `../streamrune` the demo compiles against the published preview,
  which can lag the framework's `main`: a change that needs new framework API fails to compile, or
  passes against framework code other than the one under review. `-PrequireFrameworkSibling` makes
  that a failure.
- **`--enable-preview`.** A test or main class started from an IDE needs `--enable-preview` on its
  JVM as well.
- **Quarkus scopes.** The Quarkus application produces its projections as `@Singleton` beans. A
  normal-scoped or intercepted `BaseProjection` subclass also needs
  `quarkus.arc.transform-unproxyable-classes=true` (the Quarkus default), because
  `BaseProjection.process(List, ProjectionRepository)` is final.
- **The Compose images copy the JARs** that `bootJar` builds: run it again before
  `docker compose up --build` whenever the code changes.
- **Native images.** GraalVM CE 25.0.1 lacks the reachability-metadata schema that native-build-tools
  expects for the Spring and Micronaut builds; the README's "GraalVM Native Image" section shows the
  way around it.
- **Testcontainers without Docker.** The suites of the applications and the notifications service
  fail without a running Docker daemon; nothing skips them.
