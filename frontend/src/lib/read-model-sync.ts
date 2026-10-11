import type { QueryClient, QueryKey } from "@tanstack/react-query";

/**
 * How long a read model may be behind the event store.
 *
 * A command is answered as soon as its events are stored, and the live feed
 * announces them within a second. The lists the pages show are read models,
 * written by projections that poll the event store: every five seconds plus
 * up to one second of jitter by default (`streamrune.polling-interval-ms` and
 * `streamrune.polling-jitter-ms` on the backend). A list read right after a
 * command, or right after the frame that announces its event, is therefore
 * often the list as it was before. Seven seconds after the last change it
 * holds that change.
 */
export const READ_MODEL_SETTLE_MS = 7_000;

/** The second read each query key is waiting for, by key. */
const settleTimers = new Map<string, ReturnType<typeof setTimeout>>();

/**
 * Reads the given queries again at once, and once more when the read models
 * have settled. The first read shows what is already there (an order's new status
 * once its row exists, a saga's next step); the second one is the read that is
 * certain to see the change. A key that changes again before its second read
 * waits from the later change, so a burst of events ends in one read, not one
 * per event.
 */
export function refreshAndAgainOnceSettled(queryClient: QueryClient, keys: QueryKey[]): void {
  for (const key of keys) {
    void queryClient.invalidateQueries({ queryKey: key });

    const id = JSON.stringify(key);
    const pending = settleTimers.get(id);
    if (pending) clearTimeout(pending);
    settleTimers.set(
      id,
      setTimeout(() => {
        settleTimers.delete(id);
        void queryClient.invalidateQueries({ queryKey: key });
      }, READ_MODEL_SETTLE_MS)
    );
  }
}
