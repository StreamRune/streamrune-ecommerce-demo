package org.streamrune.ecommerce.domain.common;

import java.math.BigDecimal;

public record Money(BigDecimal amount, String currency) {
  public static Money usd(BigDecimal amount) {
    return new Money(amount, "USD");
  }

  public Money add(Money other) {
    return new Money(this.amount.add(other.amount), this.currency);
  }

  public Money multiply(int quantity) {
    return new Money(this.amount.multiply(BigDecimal.valueOf(quantity)), this.currency);
  }
}
