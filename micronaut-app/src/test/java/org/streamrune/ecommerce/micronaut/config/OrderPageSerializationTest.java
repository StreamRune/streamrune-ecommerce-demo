package org.streamrune.ecommerce.micronaut.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import io.micronaut.serde.ObjectMapper;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.OrderStatus;
import org.streamrune.ecommerce.queries.dto.OrderPage;
import org.streamrune.ecommerce.queries.dto.OrderView;

/**
 * The body of {@code GET /api/orders}, one page of the order list, is written by Micronaut
 * Serialization as the Spring and Quarkus apps write it with Jackson, and read back. {@code
 * OrderPage} lives in the framework-agnostic {@code queries} module, so it needs a
 * {@code @SerdeImport} on {@code MicronautEcommerceApplication}.
 */
class OrderPageSerializationTest {

  private static final Money PRICE = Money.usd(new BigDecimal("12.50"));

  private static final OrderPage PAGE =
      new OrderPage(
          List.of(
              new OrderView(
                  "o-1",
                  "c-1",
                  List.of(new OrderView.OrderLineView("p-1", 2, PRICE)),
                  PRICE.multiply(2),
                  OrderStatus.CONFIRMED,
                  null,
                  null,
                  Instant.parse("2026-11-15T10:00:00Z"),
                  Instant.parse("2026-11-15T10:00:01Z"))),
          1,
          20,
          21,
          false);

  @Test
  void micronautSerializationWritesAPageAsJacksonDoes() throws Exception {
    var jackson = new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules();
    String written = ObjectMapper.getDefault().writeValueAsString(PAGE);
    JsonNode tree = jackson.readTree(written);

    assertThat(tree.fieldNames())
        .toIterable()
        .containsExactlyInAnyOrder("items", "page", "size", "total", "hasNext");
    assertThat(tree.get("items").get(0).get("orderId").asText()).isEqualTo("o-1");
    assertThat(tree.get("page").asInt()).isEqualTo(1);
    assertThat(tree.get("total").asLong()).isEqualTo(21);
    assertThat(tree.get("hasNext").asBoolean()).isFalse();
  }

  @Test
  void anEmptyPageHoldsAnEmptyListOfItems() throws Exception {
    OrderPage empty = new OrderPage(List.of(), 7, 2, 3, false);
    String written = ObjectMapper.getDefault().writeValueAsString(empty);

    assertThat(new com.fasterxml.jackson.databind.ObjectMapper().readTree(written))
        .isEqualTo(
            new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree("{\"items\":[],\"page\":7,\"size\":2,\"total\":3,\"hasNext\":false}"));
    assertThat(ObjectMapper.getDefault().readValue(written, OrderPage.class)).isEqualTo(empty);
  }

  @Test
  void micronautSerializationReadsAPageBack() throws Exception {
    ObjectMapper micronaut = ObjectMapper.getDefault();

    assertThat(micronaut.readValue(micronaut.writeValueAsString(PAGE), OrderPage.class))
        .isEqualTo(PAGE);
  }
}
