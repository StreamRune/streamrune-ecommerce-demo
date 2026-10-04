package org.streamrune.ecommerce.quarkus.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.annotations.RegisterForReflection;
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
 * Pins what the GraalVM native image needs for the sealed hierarchies. A native image answers
 * {@code Class#getPermittedSubclasses()} only for a sealed type registered for reflection itself;
 * registering its records alone leaves the sealed type reporting no permitted subclasses, and
 * StreamRune's startup walks (the dead-letter runner's {@code
 * registerCommand(InventoryCommand.class)}, the {@code @Encrypted} and authorization checks) then
 * refuse to start the application. On Quarkus the registration has to be a {@code {"type": ...}}
 * entry in the app's {@code reachability-metadata.json}: {@code @RegisterForReflection} on the
 * sealed type writes a legacy entry without permitted subclasses, which the binary does not list.
 * Runs on the JVM, so the rule is checked on every build, not only after a native compile.
 */
class EcommerceDomainReflectionConfigTest {

  private static final String METADATA =
      "/META-INF/native-image/org.streamrune.ecommerce/quarkus-app/reachability-metadata.json";

  private static Set<Class<?>> registeredForReflection() {
    RegisterForReflection registration =
        EcommerceDomainReflectionConfig.class.getAnnotation(RegisterForReflection.class);
    assertThat(registration).as("@RegisterForReflection on the config class").isNotNull();
    return new LinkedHashSet<>(List.of(registration.targets()));
  }

  private static Set<String> typesInReachabilityMetadata() throws IOException {
    JsonNode metadata;
    try (InputStream in = EcommerceDomainReflectionConfigTest.class.getResourceAsStream(METADATA)) {
      assertThat(in).as(METADATA).isNotNull();
      metadata = new ObjectMapper().readTree(in);
    }
    Set<String> types = new LinkedHashSet<>();
    for (JsonNode entry : metadata.path("reflection")) {
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
  void lists_the_sealed_supertype_of_every_type_registered_for_reflection() throws IOException {
    Set<String> listed = typesInReachabilityMetadata();
    List<String> missing = new ArrayList<>();
    for (Class<?> type : registeredForReflection()) {
      List<Class<?>> supertypes = new ArrayList<>(List.of(type.getInterfaces()));
      if (type.getSuperclass() != null) {
        supertypes.add(type.getSuperclass());
      }
      for (Class<?> supertype : supertypes) {
        if (supertype.isSealed() && !listed.contains(supertype.getName())) {
          missing.add(supertype.getName() + " (permits " + type.getName() + ")");
        }
      }
    }
    assertThat(missing).as("sealed supertypes missing from " + METADATA).isEmpty();
  }

  /**
   * The outbox mapper serialises package-private nested DTO records with Jackson when an event is
   * appended. Measured in the native binary before this registration: {@code POST /api/products}
   * answered 500 ({@code InvalidDefinitionException: No serializer found for class
   * EcommerceIntegrationEventMapper$ProductCreatedDto}). Registering the mapper registers its
   * nested records too ({@code ignoreNested = false}); they are not visible to name here one by
   * one.
   */
  @Test
  void registers_the_outbox_mapper_whose_nested_integration_dtos_jackson_serialises() {
    RegisterForReflection registration =
        EcommerceDomainReflectionConfig.class.getAnnotation(RegisterForReflection.class);

    assertThat(registeredForReflection()).contains(EcommerceIntegrationEventMapper.class);
    assertThat(registration.ignoreNested()).isFalse();
    assertThat(EcommerceIntegrationEventMapper.class.getDeclaredClasses())
        .as("the nested DTO records the registration reaches")
        .anyMatch(
            nested -> nested.getSimpleName().equals("ProductCreatedDto") && nested.isRecord());
  }

  @Test
  void registers_every_type_a_listed_sealed_root_permits() throws Exception {
    Set<Class<?>> registered = registeredForReflection();
    List<String> missing = new ArrayList<>();
    for (String name : typesInReachabilityMetadata()) {
      Class<?> root = Class.forName(name);
      assertThat(root.isSealed()).as("%s is sealed", name).isTrue();
      for (Class<?> permitted : root.getPermittedSubclasses()) {
        if (!registered.contains(permitted)) {
          missing.add(permitted.getName() + " (permitted by " + name + ")");
        }
      }
    }
    assertThat(missing).as("permitted subclasses not registered for reflection").isEmpty();
  }
}
