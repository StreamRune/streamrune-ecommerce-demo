package org.streamrune.ecommerce.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.validation.Validation;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandInterceptor.CommandContext;
import org.streamrune.core.ValidationException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.runtime.BeanValidationInterceptor;

class ProductCommandValidationTest {

  private final BeanValidationInterceptor interceptor =
      new BeanValidationInterceptor(Validation.buildDefaultValidatorFactory().getValidator());

  private static CommandContext contextOf(ProductCommand command, String productId) {
    return new CommandContext(
        command,
        command.getClass().getSimpleName(),
        null,
        ProductState.TYPE,
        AggregateId.of(productId),
        null,
        Instant.now());
  }

  @Test
  void aCommandWithoutAViolationPasses() {
    var command =
        new ProductCommand.CreateProduct(
            "p-valid", "Sprocket", null, null, new Money(new BigDecimal("4.99"), "USD"));

    assertTrue(interceptor.before(contextOf(command, "p-valid")));
  }

  /** The controller's DTO would refuse this name; a command built in code never meets the DTO. */
  @Test
  void aBlankNameIsRefusedBeforeTheDecider() {
    var command =
        new ProductCommand.CreateProduct(
            "p-blank", " ", null, null, new Money(new BigDecimal("5.00"), "USD"));

    var refused =
        assertThrows(
            ValidationException.class, () -> interceptor.before(contextOf(command, "p-blank")));

    assertEquals("Command validation failed for CreateProduct", refused.getMessage());
    assertEquals("name", refused.errors().getFirst().field());
    assertEquals("NotBlank", refused.errors().getFirst().code());
  }
}
