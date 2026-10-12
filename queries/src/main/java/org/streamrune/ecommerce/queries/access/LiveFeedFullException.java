package org.streamrune.ecommerce.queries.access;

/**
 * The open live feed has no place for another connection: thrown by {@link LiveFeed#admit} when
 * {@link LiveFeed#MAX_CONNECTIONS} clients are connected. Each application answers it with {@code
 * 503 Service Unavailable} and a {@code Retry-After} of {@link LiveFeed#RETRY_AFTER_SECONDS}
 * seconds.
 *
 * <p>{@code 503} and not {@code 429}: the limit is the application's, shared by every caller, and
 * says nothing about how many requests this caller sent. The feed cannot tell its callers apart, so
 * it has nobody to count requests for.
 */
public final class LiveFeedFullException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public LiveFeedFullException(int places) {
    super(
        "The live feed serves "
            + places
            + " connections at once and all of them are taken; try again in a few seconds");
  }
}
