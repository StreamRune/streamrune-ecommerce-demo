import type { QueryClient, QueryKey } from "@tanstack/react-query";

/**
 * Keeping the lists of a page current with read models that are late.
 *
 * A command is answered as soon as its events are stored, and the live feed
 * announces them within a second. The lists the pages show are read models,
 * written by projections that poll the event store: every five seconds plus
 * up to one second of jitter by default (`streamrune.polling-interval-ms` and
 * `streamrune.polling-jitter-ms` on the backend). A list read right after a
 * command, or right after the frame that announces its event, is therefore
 * often the list as it was before.
 *
 * So a change to a query key is followed by one read at once and by a short
 * series of re-reads at set moments. The series starts again from the last change to
 * the key, and it ends: nothing here polls.
 */

/**
 * When a list is read again, in milliseconds after the last change to its key.
 *
 * - 2 s: the feed and the saga are through; a list that is not a projection
 *   (the saga list) and a projection that polled early have the change.
 * - 7 s: the latest moment the default projection can be, five seconds of
 *   polling, one of jitter and the second the frame took to arrive.
 * - 15 s and 30 s: a projection that is late: a poll that failed and is
 *   repeated after its backoff, a paused container, a polling interval
 *   configured well above the default.
 */
export const RE_READ_AFTER_MS = [2_000, 7_000, 15_000, 30_000];

/**
 * How long the read at once covers. A change to a key within this time of the
 * key's last read at once does not read again; it only restarts the re-reads.
 * A burst of frames is thus answered by one read, not one per frame.
 */
export const BURST_MS = 1_000;

interface PendingKey {
  /** When the key was last read at once. */
  readAt: number;
  /** The re-reads the key is still waiting for. */
  reReads: ReturnType<typeof setTimeout>[];
}

const pendingKeys = new Map<string, PendingKey>();

/**
 * Reads the given queries again: at once, and at each of `RE_READ_AFTER_MS`
 * after the last change to each key.
 *
 * A read is an invalidation. A query a mounted component shows is fetched
 * again, with the session's identity headers; a query nothing shows is marked
 * stale and fetched when a page next uses it, so the re-reads of a page that
 * was left ask the backend for nothing. A read that finds the query already
 * being fetched joins that request instead of replacing it.
 */
export function readAgainUntilSettled(queryClient: QueryClient, keys: QueryKey[]): void {
  const calledAt = Date.now();
  for (const key of keys) {
    const id = JSON.stringify(key);
    const pending = pendingKeys.get(id);
    if (pending) pending.reReads.forEach(clearTimeout);

    const coveredByTheLastRead = pending !== undefined && calledAt - pending.readAt < BURST_MS;
    if (!coveredByTheLastRead) read(queryClient, key);

    pendingKeys.set(id, {
      readAt: coveredByTheLastRead ? pending.readAt : calledAt,
      reReads: RE_READ_AFTER_MS.map((delay, step) =>
        setTimeout(() => {
          read(queryClient, key);
          if (step === RE_READ_AFTER_MS.length - 1) pendingKeys.delete(id);
        }, delay)
      ),
    });
  }
}

/**
 * Drops every re-read that is still waiting. The role provider calls it when
 * the role or the identity changes, before it empties the query cache: what
 * the previous identity was waiting for is not read under the next one. The
 * SSE provider calls it when the application unmounts.
 */
export function cancelPendingReReads(): void {
  for (const pending of pendingKeys.values()) {
    pending.reReads.forEach(clearTimeout);
  }
  pendingKeys.clear();
}

function read(queryClient: QueryClient, key: QueryKey): void {
  // cancelRefetch: false — a fetch of the query that is already running answers this read too.
  void queryClient.invalidateQueries({ queryKey: key }, { cancelRefetch: false });
}
