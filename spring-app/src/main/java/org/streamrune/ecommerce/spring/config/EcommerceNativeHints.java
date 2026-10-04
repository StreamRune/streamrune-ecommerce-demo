package org.streamrune.ecommerce.spring.config;

import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.streamrune.spring.StreamRuneRuntimeHints;

/**
 * GraalVM native-image reflection hints for the demo's domain types.
 *
 * <p>The StreamRune event store serialises every domain event (and snapshot state) through Jackson,
 * and its {@code CryptoShreddingModule} inspects each serialised record via {@link
 * Class#getRecordComponents()} to discover {@code @Encrypted} components — a call that fails in a
 * native image unless the record's component accessor methods are registered at build time. Spring
 * Boot's AOT engine auto-registers the request/response types reachable from
 * {@code @RestController} signatures, but the event and state records are reached only dynamically
 * through the event store, so it cannot see them.
 *
 * <p>Delegates to the framework helper {@link
 * StreamRuneRuntimeHints#registerDomainPackages(RuntimeHints, ClassLoader, String...)}, which scans
 * the framework-agnostic domain modules and registers every concrete type found — events, commands,
 * aggregate states, value objects (e.g. {@code Money}), query/view DTOs and enums. New records are
 * covered automatically; over-registration (e.g. a never-serialised decider) is harmless.
 *
 * <p>Wired via {@code @ImportRuntimeHints(EcommerceNativeHints.class)} on {@link StreamRuneConfig}.
 */
public class EcommerceNativeHints implements RuntimeHintsRegistrar {

  /** Base packages of the framework-agnostic domain modules shared by all three runtime apps. */
  private static final String[] DOMAIN_PACKAGES = {
    "org.streamrune.ecommerce.domain",
    "org.streamrune.ecommerce.commands",
    "org.streamrune.ecommerce.queries",
    "org.streamrune.ecommerce.projections",
  };

  @Override
  public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
    StreamRuneRuntimeHints.registerDomainPackages(hints, classLoader, DOMAIN_PACKAGES);
  }
}
