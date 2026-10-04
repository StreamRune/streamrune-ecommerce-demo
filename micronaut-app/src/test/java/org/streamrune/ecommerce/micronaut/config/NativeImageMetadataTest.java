package org.streamrune.ecommerce.micronaut.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.streamrune.ecommerce.commands.integration.EcommerceIntegrationEventMapper;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.product.ProductCommand;

/**
 * Pins what the GraalVM native image needs for the sealed hierarchies and the outbox DTOs. A native
 * image answers {@code Class#getPermittedSubclasses()} only for a sealed type registered for
 * reflection itself; registering its records alone ({@code reflect-config.json}) leaves the sealed
 * type reporting no permitted subclasses, and StreamRune's startup walks (the {@code Encrypted}
 * check when the command bus is built, the dead-letter runner's {@code
 * registerCommand(InventoryCommand.class)}) then refuse to start the application. On Micronaut the
 * registration has to be a {@code {"type": ...}} entry in the app's {@code
 * reachability-metadata.json}: a Micronaut {@code @TypeHint} naming the sealed type was tried first
 * and the binary still refused. Runs on the JVM, so the rule is checked on every build, not only
 * after a native compile.
 */
class NativeImageMetadataTest {

  private static final String DIR =
      "/META-INF/native-image/org.streamrune.ecommerce/micronaut-app/";
  private static final String REFLECT_CONFIG = DIR + "reflect-config.json";
  private static final String REACHABILITY_METADATA = DIR + "reachability-metadata.json";

  private static JsonNode read(String resource) throws IOException {
    try (InputStream in = NativeImageMetadataTest.class.getResourceAsStream(resource)) {
      assertThat(in).as(resource).isNotNull();
      return new ObjectMapper().readTree(in);
    }
  }

  /** The records and DTOs, registered by name in the legacy {@code reflect-config.json}. */
  private static Set<String> namesInReflectConfig() throws IOException {
    Set<String> names = new LinkedHashSet<>();
    for (JsonNode entry : read(REFLECT_CONFIG)) {
      names.add(entry.path("name").asText());
    }
    return names;
  }

  /** The sealed types, registered as {@code {"type": ...}} entries. */
  private static Set<String> typesInReachabilityMetadata() throws IOException {
    Set<String> types = new LinkedHashSet<>();
    for (JsonNode entry : read(REACHABILITY_METADATA).path("reflection")) {
      types.add(entry.path("type").asText());
    }
    return types;
  }

  @Test
  void lists_the_sealed_command_roots_the_dead_letter_runner_expands() throws IOException {
    assertThat(typesInReachabilityMetadata())
        .contains(
            ProductCommand.class.getName(),
            OrderCommand.class.getName(),
            CustomerCommand.class.getName(),
            PaymentCommand.class.getName(),
            InventoryCommand.class.getName());
  }

  @Test
  void lists_the_sealed_supertype_of_every_type_in_reflect_config() throws Exception {
    Set<String> listed = typesInReachabilityMetadata();
    List<String> missing = new ArrayList<>();
    for (String name : namesInReflectConfig()) {
      Class<?> type = Class.forName(name);
      List<Class<?>> supertypes = new ArrayList<>(List.of(type.getInterfaces()));
      if (type.getSuperclass() != null) {
        supertypes.add(type.getSuperclass());
      }
      for (Class<?> supertype : supertypes) {
        if (supertype.isSealed() && !listed.contains(supertype.getName())) {
          missing.add(supertype.getName() + " (permits " + name + ")");
        }
      }
    }
    assertThat(missing).as("sealed supertypes missing from " + REACHABILITY_METADATA).isEmpty();
  }

  @Test
  void registers_every_type_a_listed_sealed_root_permits() throws Exception {
    Set<String> registered = namesInReflectConfig();
    List<String> missing = new ArrayList<>();
    for (String name : typesInReachabilityMetadata()) {
      Class<?> root = Class.forName(name);
      assertThat(root.isSealed()).as("%s is sealed", name).isTrue();
      for (Class<?> permitted : root.getPermittedSubclasses()) {
        if (!registered.contains(permitted.getName())) {
          missing.add(permitted.getName() + " (permitted by " + name + ")");
        }
      }
    }
    assertThat(missing).as("permitted subclasses not in " + REFLECT_CONFIG).isEmpty();
  }

  /**
   * The outbox mapper serialises package-private nested DTO records with Jackson when an event is
   * appended; the image serialises only records registered for reflection (the Quarkus binary
   * answered {@code POST /api/products} with 500, "No serializer found for class
   * EcommerceIntegrationEventMapper$ProductCreatedDto", until they were registered).
   */
  @Test
  void registers_the_outbox_mappers_nested_integration_dtos() throws IOException {
    Set<String> registered = namesInReflectConfig();
    List<String> dtos = new ArrayList<>();
    for (Class<?> nested : EcommerceIntegrationEventMapper.class.getDeclaredClasses()) {
      if (nested.isRecord()) {
        dtos.add(nested.getName());
      }
    }

    assertThat(dtos).as("the mapper's nested DTO records").isNotEmpty();
    assertThat(registered).containsAll(dtos);
  }
}
