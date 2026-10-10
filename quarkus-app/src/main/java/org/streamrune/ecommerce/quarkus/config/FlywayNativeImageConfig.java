package org.streamrune.ecommerce.quarkus.config;

import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Registers the Flyway classes that Flyway itself loads by name, for the GraalVM native image.
 *
 * <p>The application creates its schema at startup ({@code
 * streamrune.event-store.schema.auto-initialize=true}): the framework's event store factory runs
 * Flyway over the migration scripts it ships. Flyway picks its logging back end by instantiating a
 * {@code LogCreator} from a class name, and a native image can only do that for a class registered
 * for reflection. The Spring and Micronaut builds get the registration from the GraalVM
 * reachability-metadata repository their build plugin applies; a Quarkus build does not consult
 * that repository, so the app registers the class here. Flyway's plugins, which it finds with
 * {@code ServiceLoader}, are covered by {@code quarkus.native.auto-service-loader-registration} in
 * {@code application.properties}.
 *
 * <p>The native smoke test ({@code scripts/native-image-smoke-test.sh quarkus}) starts the binary
 * on an empty database and requires the schema to be there, so a missing registration fails it.
 */
@RegisterForReflection(
    classNames = {"org.flywaydb.core.internal.logging.slf4j.Slf4jLogCreator"},
    fields = false)
public final class FlywayNativeImageConfig {

  private FlywayNativeImageConfig() {}
}
