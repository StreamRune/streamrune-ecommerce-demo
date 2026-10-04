package org.streamrune.ecommerce.commands.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.*;
import org.streamrune.test.DeciderFixture;

class OrderDeciderTest {

  private final DeciderFixture<OrderCommand, OrderState, OrderEvent> fixture =
      DeciderFixture.of(new OrderDecider());

  private static final Money TEN_USD = new Money(BigDecimal.TEN, "USD");

  private OrderEvent.OrderPlaced placed() {
    return new OrderEvent.OrderPlaced(
        "o-1",
        "c-1",
        List.of(new OrderEvent.OrderLine("p-1", 2, TEN_USD)),
        new Money(BigDecimal.valueOf(20), "USD"));
  }

  @Test
  void placeOrder_emitsOrderPlaced() {
    fixture
        .given()
        .when(
            new OrderCommand.PlaceOrder(
                "o-1", "c-1", List.of(new OrderCommand.OrderLine("p-1", 2, TEN_USD))))
        .expectState(
            s -> {
              assertThat(s.status()).isEqualTo(OrderStatus.CREATED);
              assertThat(s.total().amount()).isEqualByComparingTo(BigDecimal.valueOf(20));
            });
  }

  @Test
  void confirmOrder_fromCreated() {
    fixture
        .given(placed())
        .when(new OrderCommand.ConfirmOrder("o-1"))
        .expectEvents(new OrderEvent.OrderConfirmed("o-1"))
        .expectState(s -> assertThat(s.status()).isEqualTo(OrderStatus.CONFIRMED));
  }

  @Test
  void confirmOrder_notCreated_throws() {
    fixture
        .given(placed(), new OrderEvent.OrderConfirmed("o-1"))
        .when(new OrderCommand.ConfirmOrder("o-1"))
        .expectException(DomainException.class);
  }

  @Test
  void shipOrder_fromConfirmed() {
    fixture
        .given(placed(), new OrderEvent.OrderConfirmed("o-1"))
        .when(new OrderCommand.ShipOrder("o-1"))
        .expectEvents(new OrderEvent.OrderShipped("o-1"));
  }

  @Test
  void shipOrder_notConfirmed_throws() {
    fixture
        .given(placed())
        .when(new OrderCommand.ShipOrder("o-1"))
        .expectException(DomainException.class);
  }

  @Test
  void deliverOrder_fromShipped() {
    fixture
        .given(placed(), new OrderEvent.OrderConfirmed("o-1"), new OrderEvent.OrderShipped("o-1"))
        .when(new OrderCommand.DeliverOrder("o-1"))
        .expectEvents(new OrderEvent.OrderDelivered("o-1"));
  }

  @Test
  void cancelOrder_fromCreated() {
    fixture
        .given(placed())
        .when(new OrderCommand.CancelOrder("o-1", "Changed mind"))
        .expectEvents(new OrderEvent.OrderCancelled("o-1", "Changed mind"));
  }

  @Test
  void cancelOrder_delivered_throws() {
    fixture
        .given(
            placed(),
            new OrderEvent.OrderConfirmed("o-1"),
            new OrderEvent.OrderShipped("o-1"),
            new OrderEvent.OrderDelivered("o-1"))
        .when(new OrderCommand.CancelOrder("o-1", "Too late"))
        .expectException(DomainException.class);
  }
}
