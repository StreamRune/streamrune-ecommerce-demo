package org.streamrune.ecommerce.micronaut.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import io.micronaut.serde.ObjectMapper;
import java.io.InputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.OrderStatus;
import org.streamrune.ecommerce.queries.dto.OrderPage;
import org.streamrune.ecommerce.queries.dto.OrderView;

/**
 * The body of {@code GET /api/orders}, one page of the order list, is written by Micronaut
 * Serialization, configured as the app's {@code application.yml} configures it, field for field as
 * Jackson writes it in the Spring and Quarkus apps: a {@code null} and an empty list included.
 */
class OrderPageSerializationTest {

  private static final Money PRICE = Money.usd(new BigDecimal("12.50"));

  /** An order that is not paid yet: its payment status and its reason are {@code null}. */
  private static final OrderView ORDER =
      new OrderView(
          "o-1",
          "c-1",
          List.of(new OrderView.OrderLineView("p-1", 2, PRICE)),
          PRICE.multiply(2),
          OrderStatus.CREATED,
          null,
          null,
          Instant.parse("2026-11-15T10:00:00Z"),
          Instant.parse("2026-11-15T10:00:01Z"));

  private static final com.fasterxml.jackson.databind.ObjectMapper JACKSON =
      new com.fasterxml.jackson.databind.ObjectMapper()
          .registerModule(new JavaTimeModule())
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

  /** Micronaut Serialization with the serde settings of the app's {@code application.yml}. */
  private static ObjectMapper appMapper() throws Exception {
    try (InputStream yml =
        OrderPageSerializationTest.class.getResourceAsStream("/application.yml")) {
      Map<String, Object> configuration = new YamlPropertySourceLoader().read("application", yml);
      return ObjectMapper.create(configuration);
    }
  }

  @Test
  void aPageIsWrittenAsJacksonWritesIt() throws Exception {
    OrderPage page = new OrderPage(List.of(ORDER), 1, 20, 21, false);

    assertThat(JACKSON.readTree(appMapper().writeValueAsString(page)))
        .isEqualTo(JACKSON.readTree(JACKSON.writeValueAsString(page)));
  }

  @Test
  void anEmptyPageIsWrittenWithAnEmptyListOfItems() throws Exception {
    OrderPage empty = new OrderPage(List.of(), 7, 2, 3, false);

    assertThat(JACKSON.readTree(appMapper().writeValueAsString(empty)))
        .isEqualTo(
            JACKSON.readTree("{\"items\":[],\"page\":7,\"size\":2,\"total\":3,\"hasNext\":false}"));
  }

  @Test
  void aPageIsReadBack() throws Exception {
    ObjectMapper micronaut = appMapper();
    OrderPage page = new OrderPage(List.of(ORDER), 0, 20, 1, false);

    assertThat(micronaut.readValue(micronaut.writeValueAsString(page), OrderPage.class))
        .isEqualTo(page);
  }
}
