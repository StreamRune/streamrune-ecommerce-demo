package org.streamrune.ecommerce.domain.product;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import org.streamrune.core.Command;
import org.streamrune.core.RequireRole;
import org.streamrune.ecommerce.domain.common.Money;

public sealed interface ProductCommand extends Command {
  record CreateProduct(
      @NotBlank String productId,
      @NotBlank String name,
      String description,
      String category,
      @NotNull Money price,
      @PositiveOrZero int initialStock)
      implements ProductCommand {}

  record UpdatePrice(@NotBlank String productId, @NotNull Money newPrice)
      implements ProductCommand {}

  record AdjustStock(@NotBlank String productId, int quantityChange, String reason)
      implements ProductCommand {}

  @RequireRole("ADMIN")
  record DiscontinueProduct(@NotBlank String productId) implements ProductCommand {}
}
