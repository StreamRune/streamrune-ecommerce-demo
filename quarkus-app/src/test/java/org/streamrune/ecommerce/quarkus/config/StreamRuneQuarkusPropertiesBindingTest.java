package org.streamrune.ecommerce.quarkus.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.smallrye.config.EnvConfigSource;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.quarkus.StreamRuneQuarkusProperties;

/**
 * {@link StreamRuneProducer#bind} reads the {@code streamrune.*} keys as Quarkus does: an
 * environment variable sets a key the properties file does not name, a dashed key included, a
 * variable that matches no key (Spring's spelling of a dashed key) is ignored, and a key nothing
 * maps is still refused when a file or a system property names it.
 */
class StreamRuneQuarkusPropertiesBindingTest {

  private static SmallRyeConfig live(Map<String, String> environment, Map<String, String> file) {
    return new SmallRyeConfigBuilder()
        .withSources(
            new EnvConfigSource(environment, EnvConfigSource.ORDINAL),
            new PropertiesConfigSource(file, "application.properties", 250))
        .build();
  }

  @Test
  void anEnvironmentVariableSetsADashedKeyTheFileDoesNotName() {
    StreamRuneQuarkusProperties properties =
        StreamRuneProducer.bind(
            live(
                Map.of(
                    "STREAMRUNE_TRACING_EVENT_STORE_ENABLED", "false",
                    "STREAMRUNE_POLLING_INTERVAL", "2s"),
                Map.of("streamrune.outbox.enabled", "false")));

    assertThat(properties.tracing().eventStore().enabled()).isFalse();
    assertThat(properties.tracing().command().enabled()).isTrue();
    assertThat(properties.pollingInterval()).isEqualTo(Duration.ofSeconds(2));
    assertThat(properties.outbox().enabled()).isFalse();
  }

  @Test
  void anEnvironmentVariableOverridesAKeyTheFileNames() {
    StreamRuneQuarkusProperties properties =
        StreamRuneProducer.bind(
            live(
                Map.of("STREAMRUNE_TRACING_EVENT_STORE_ENABLED", "false"),
                Map.of("streamrune.tracing.event-store.enabled", "true")));

    assertThat(properties.tracing().eventStore().enabled()).isFalse();
  }

  @Test
  void springsSpellingOfADashedKeyIsIgnored() {
    StreamRuneQuarkusProperties properties =
        StreamRuneProducer.bind(
            live(Map.of("STREAMRUNE_TRACING_EVENTSTORE_ENABLED", "false"), Map.of()));

    assertThat(properties.tracing().eventStore().enabled()).isTrue();
  }

  @Test
  void aKeyNothingMapsIsStillRefusedFromASystemProperty() {
    SmallRyeConfig live =
        new SmallRyeConfigBuilder()
            .withSources(
                new PropertiesConfigSource(
                    Map.of("streamrune.tracing.event-stor.enabled", "false"),
                    "SysPropConfigSource",
                    400))
            .build();

    assertThatThrownBy(() -> StreamRuneProducer.bind(live))
        .hasStackTraceContaining("streamrune.tracing.event-stor.enabled");
  }

  @Test
  void aKeyNothingMapsIsStillRefusedFromTheFile() {
    assertThatThrownBy(
            () ->
                StreamRuneProducer.bind(
                    live(Map.of(), Map.of("streamrune.tracing.event-stor.enabled", "false"))))
        .hasStackTraceContaining("streamrune.tracing.event-stor.enabled");
  }
}
