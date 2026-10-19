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
  void placeOrder_existingOrder_throws() {
    fixture
        .given(placed(), new OrderEvent.OrderConfirmed("o-1"))
        .when(
            new OrderCommand.PlaceOrder(
                "o-1", "c-2", List.of(new OrderCommand.OrderLine("p-9", 1, TEN_USD))))
        .expectFailedWith(DomainException.class, "Order already exists: o-1");
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
        .expectEvents(new OrderEvent.OrderShipped("o-1", placed().lines()));
  }

  @Test
  void shipOrder_carriesTheLines() {
    fixture
        .given(placed(), new OrderEvent.OrderConfirmed("o-1"))
        .when(new OrderCommand.ShipOrder("o-1"))
        .expectProduced(
            events ->
                assertThat(events)
                    .containsExactly(new OrderEvent.OrderShipped("o-1", placed().lines())),
            s -> assertThat(s.status()).isEqualTo(OrderStatus.SHIPPED));
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
        .given(
            placed(),
            new OrderEvent.OrderConfirmed("o-1"),
            new OrderEvent.OrderShipped("o-1", placed().lines()))
        .when(new OrderCommand.DeliverOrder("o-1"))
        .expectEvents(new OrderEvent.OrderDelivered("o-1"));
  }

  @Test
  void cancelOrder_fromCreated() {
    fixture
        .given(placed())
        .when(new OrderCommand.CancelOrder("o-1", "Changed mind"))
        .expectEvents(new OrderEvent.OrderCancelled("o-1", "Changed mind", placed().lines()));
  }

  @Test
  void cancelOrder_confirmed_carriesTheLines() {
    OrderEvent.OrderPlaced twoLines =
        new OrderEvent.OrderPlaced(
            "o-1",
            "c-1",
            List.of(
                new OrderEvent.OrderLine("p-1", 2, TEN_USD),
                new OrderEvent.OrderLine("p-2", 1, TEN_USD)),
            new Money(BigDecimal.valueOf(30), "USD"));
    fixture
        .given(twoLines, new OrderEvent.OrderConfirmed("o-1"))
        .when(new OrderCommand.CancelOrder("o-1", "why"))
        .expectEvents(new OrderEvent.OrderCancelled("o-1", "why", twoLines.lines()));
  }

  @Test
  void cancelOrder_cancelled_appendsAnotherCancellation() {
    fixture
        .given(placed(), new OrderEvent.OrderCancelled("o-1", "first", placed().lines()))
        .when(new OrderCommand.CancelOrder("o-1", "second"))
        .expectEvents(new OrderEvent.OrderCancelled("o-1", "second", placed().lines()));
  }

  @Test
  void cancelOrder_shipped_isRefused() {
    fixture
        .given(
            placed(),
            new OrderEvent.OrderConfirmed("o-1"),
            new OrderEvent.OrderShipped("o-1", placed().lines()))
        .when(new OrderCommand.CancelOrder("o-1", "Too late"))
        .expectFailedWith(DomainException.class, "Cannot cancel SHIPPED orders");
  }

  @Test
  void cancelOrder_delivered_throws() {
    fixture
        .given(
            placed(),
            new OrderEvent.OrderConfirmed("o-1"),
            new OrderEvent.OrderShipped("o-1", placed().lines()),
            new OrderEvent.OrderDelivered("o-1"))
        .when(new OrderCommand.CancelOrder("o-1", "Too late"))
        .expectFailedWith(DomainException.class, "Cannot cancel DELIVERED orders");
  }

  // An order that was never placed is not confirmed, shipped or delivered: its empty state reads
  // CREATED, so without the check ConfirmOrder would open a stream for an id nobody placed.

  @Test
  void confirmOrder_neverPlaced_throws() {
    fixture
        .given()
        .when(new OrderCommand.ConfirmOrder("o-none"))
        .expectFailedWith(DomainException.class, "Order not found: o-none");
  }

  @Test
  void shipOrder_neverPlaced_throws() {
    fixture
        .given()
        .when(new OrderCommand.ShipOrder("o-none"))
        .expectFailedWith(DomainException.class, "Order not found: o-none");
  }

  @Test
  void deliverOrder_neverPlaced_throws() {
    fixture
        .given()
        .when(new OrderCommand.DeliverOrder("o-none"))
        .expectFailedWith(DomainException.class, "Order not found: o-none");
  }

  @Test
  void cancelOrder_neverPlaced_isAcceptedWithoutLines() {
    // CancelOrder is the last step of the fulfillment saga's compensation, and a refused
    // compensation command ends the saga FAILED: the decider accepts it for any order that is not
    // shipped or delivered, an id nobody placed included.
    fixture
        .given()
        .when(new OrderCommand.CancelOrder("o-none", "Compensation"))
        .expectEvents(new OrderEvent.OrderCancelled("o-none", "Compensation", List.of()));
  }

  @Test
  void placeOrder_afterACancellationOfTheUnplacedId_isRefused() {
    fixture
        .given(new OrderEvent.OrderCancelled("o-1", "x", List.of()))
        .when(
            new OrderCommand.PlaceOrder(
                "o-1", "c-1", List.of(new OrderCommand.OrderLine("p-1", 2, TEN_USD))))
        .expectFailedWith(DomainException.class, "Order already exists: o-1");
  }
}
