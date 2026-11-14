package org.streamrune.ecommerce.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStore;
import org.streamrune.core.ValidationException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * The application's command bus validates a command built in code, which no request DTO checked:
 * the {@code BeanValidationInterceptor} the bus is wired with refuses a blank product name before
 * the decider, and no event is stored. The interceptor finds the constraints of the commands only
 * because {@code application.properties} has Quarkus index the {@code domain} module ({@code
 * quarkus.index-dependency.domain.*}); without that index every command passes.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class CommandBusValidationIT {

  @Inject VirtualThreadCommandBus commandBus;
  @Inject EventStore eventStore;

  @Test
  void theBusRefusesACommandBuiltInCodeThatBreaksAConstraint() {
    String productId = "bus-validation-" + System.nanoTime();
    var blankName =
        new ProductCommand.CreateProduct(
            productId, " ", null, "General", new Money(new BigDecimal("5.00"), "USD"));

    assertThatThrownBy(() -> commandBus.execute(blankName))
        .isInstanceOfSatisfying(
            ValidationException.class,
            refused -> {
              assertThat(refused).hasMessage("Command validation failed for CreateProduct");
              assertThat(refused.errors().getFirst().field()).isEqualTo("name");
              assertThat(refused.errors().getFirst().code()).isEqualTo("NotBlank");
            });
    assertThat(
            eventStore.readStream(
                StreamId.of(ProductState.TYPE, AggregateId.of(productId)), Version.initial(), 10))
        .as("a refused command stores no event")
        .isEmpty();
  }
}
