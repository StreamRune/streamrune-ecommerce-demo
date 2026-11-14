package org.streamrune.ecommerce.domain.product;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.streamrune.core.Command;
import org.streamrune.core.RequireRole;
import org.streamrune.ecommerce.domain.common.Money;

public sealed interface ProductCommand extends Command {
  record CreateProduct(
      @NotBlank String productId,
      @NotBlank String name,
      String description,
      String category,
      @NotNull Money price)
      implements ProductCommand {}

  record UpdatePrice(@NotBlank String productId, @NotNull Money newPrice)
      implements ProductCommand {}

  @RequireRole("ADMIN")
  record DiscontinueProduct(@NotBlank String productId) implements ProductCommand {}
}
