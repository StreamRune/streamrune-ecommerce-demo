package org.streamrune.ecommerce.quarkus.controller;

/**
 * Replaced by {@link EventExplorerController}, which covers the full {@code /api/events} surface
 * (global list, per-stream history, and global SSE tail at {@code /api/events/sse}). This file is
 * kept as a placeholder to avoid breaking any class-reference that might still exist in compile
 * scope; the actual endpoint logic now lives in {@code EventExplorerController}.
 *
 * @deprecated Use {@link EventExplorerController} instead.
 */
@Deprecated
class EventSseController {
  // Intentionally empty — superseded by EventExplorerController.
}
