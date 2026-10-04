# Chapter 16: The Frontend

> **What you'll learn:** How the ecommerce demo's Next.js 16 frontend is structured, the four architectural patterns that connect it to the backend (API layer, SSE provider, role provider, permission gate), how each of the eight pages surfaces the backend capabilities you built in earlier chapters, and what "real-time UI" looks like when SSE events automatically invalidate TanStack Query caches.

---

## What We're Building

This chapter is a reference tour, not a build-along. The frontend is already written and lives in `frontend/`. Your job is to run it, understand how it is wired together, and see the backend features from Chapters 1–15 through a browser.

The application is a Next.js 16 app with React 19. It talks to the single Spring Boot backend you built, using plain HTTP for reads and mutations and a persistent SSE connection for real-time updates. There are eight pages, each demonstrating a different slice of the domain:

| Page | What it shows |
|---|---|
| Dashboard | Live stat cards and a scrolling event feed |
| Products | CRUD with role-guarded actions |
| Orders | Order lifecycle workflow with expandable rows |
| Customers | GDPR features — data export and right to forget |
| Fulfillment | Saga step visualizer per order |
| Events | Historical event browser plus live SSE feed |
| Audit | Audit log (ADMIN-only), with a row-count selector |
| Admin | Circuit breaker state, DLQ, outbox, payment failure toggle |

The technology choices are minimal on purpose. The stack is Next.js (16.2.4), React 19, TanStack Query v5, Tailwind CSS v4, shadcn-style components built on the `@base-ui/react` primitive layer, and the browser's native `EventSource` API — nothing exotic. One thing to note if you inspect the components: the `@base-ui/react` triggers use a render-prop pattern (e.g. `<DialogTrigger render={<Button />}>`) rather than `asChild`.

---

## Setup

Make sure the backend is running on `http://localhost:8080`, then start the frontend:

```bash
cd frontend
npm install
npm run dev
# Open http://localhost:3000
```

The dev server proxies nothing. Every API call goes directly to `http://localhost:8080` (configurable via `NEXT_PUBLIC_API_URL`).

---

## Architecture Patterns

Four files carry most of the architectural weight. Understanding them makes the rest of the codebase easy to navigate.

### API Layer — `src/lib/api.ts`

All HTTP calls flow through a single `apiFetch` wrapper. Before every request it injects two headers that your backend authorization interceptor reads:

- `X-User-Role` — the currently selected role (`GUEST`, `CUSTOMER`, or `ADMIN`)
- `X-User-Id` — the current user's ID: the selected customer for `CUSTOMER`, `admin-1` for `ADMIN`, and none for `GUEST`

The module keeps these values in two module-level variables (`currentRole` and `currentUserId`) that are updated by `setApiAuth(role, userId)`. No React context is consulted at fetch time, which means the values are always current even inside `queryFn` callbacks that run outside the component tree.

> **The browser plays the gateway here.** The backends run in StreamRune's trusted-gateway mode (Chapter 9), so `X-User-Id` is the caller's identity and nothing but the browser vouches for it. Every role that can send a guarded command needs one: a `@RequireRole` command without `X-User-Id` is refused with `Authentication required` whatever `X-User-Role` says, which is why `ADMIN` sends `admin-1`. `X-User-Role` is the demo's role shortcut — the backend's `HeaderUserRoleResolver` believes it as sent. Both are fine for a local showcase and wrong anywhere else: in production a gateway or the backend's own security framework authenticates the user and sets the identity, and the roles come from that authenticated identity, never from a header the browser picks. The same holds for the Orders page's per-customer view: its `?customerId=` filter is a query parameter the browser chooses, not a rule the backend enforces.

The wrapper also handles the two content types the backend returns: JSON responses are parsed with `res.json()`; plain-text responses (used for a few admin endpoints) are returned as strings. Any non-2xx status throws an `Error` with the HTTP status and body text, which TanStack Query surfaces as a query error.

Named exports like `fetchProducts()`, `placeOrder(body)`, and `togglePaymentFailure()` wrap `apiFetch` and carry full TypeScript types for their arguments and return values. Adding a new API call means adding one typed function here — there is no additional boilerplate.

### SSE Provider — `src/providers/sse-provider.tsx`

`SSEProvider` wraps the entire application. It opens a single `EventSource` to `/api/events/sse` on mount and keeps it alive for the lifetime of the page. Two pieces of state are exposed through `useSSE()`: a `connected` boolean for the status dot on the **Event Explorer** item in the sidebar (a small colored dot that pulses green when connected, muted when disconnected), and a `recentEvents` array capped at 50 entries for the live feed widgets.

When an SSE message arrives, the provider does two things. First, it prepends the parsed `EventEntry` to `recentEvents`. Second, it calls `queryClient.invalidateQueries` for every TanStack Query cache key that is affected by the event type. The mapping is encoded in `invalidationKeysFor(eventType)`:

- Product events invalidate `["products"]`
- Order events invalidate `["orders"]`
- Customer events invalidate `["customers"]`
- Payment events (`PaymentInitiated`, `PaymentCaptured`, `PaymentRefunded`, `PaymentFailed`) invalidate `["fulfillment"]` only
- Inventory events (`StockReserved`, `StockReleased`, `ReservationConfirmed`, `ShipmentReceived`) invalidate both `["products"]` and `["fulfillment"]`
- All events also invalidate `["dashboard"]` and `["events"]`

The result is that placing an order in one browser tab instantly refreshes the order list in another tab — no polling, no manual refetch. Every page that uses TanStack Query is automatically kept in sync as long as the SSE connection is open.

When the connection drops, the provider reconnects using exponential backoff. The delay starts at one second and doubles on each failure, capping at thirty seconds. The retry counter resets to zero when the connection reopens successfully.

### Role Provider — `src/providers/role-provider.tsx`

`RoleProvider` owns the concept of "who is currently using the app." It exposes three roles — `GUEST`, `CUSTOMER`, and `ADMIN` — selectable from a role switcher at the bottom of the sidebar.

When the role changes, three things happen in sequence:

1. `localStorage` is updated so the selection survives a page refresh.
2. `setApiAuth(role, userId)` is called to update the API layer's headers immediately.
3. `queryClient.invalidateQueries()` is called with no filter, which invalidates the entire cache. This forces every page to refetch with the new role, because the backend may return different data or reject requests outright depending on the role.

The provider also tracks the `userId` the API layer sends, chosen per role by `userIdForRole` in `src/lib/identity.ts`: `admin-1` for `ADMIN`, none for `GUEST`, and for `CUSTOMER` the selected customer, `cust-alice` by default. For `CUSTOMER` a second selector at the bottom of the sidebar lets you switch between Alice and Bob, which shows the per-customer views on the Orders and Customers pages (a view the browser selects, not one the backend enforces — see the note above). `tests/e2e/identity-headers.spec.ts` pins the headers each role sends.

The provider exposes a `hasPermission(permission: string): boolean` function that delegates to a `permissions.ts` module. This is the source of truth used by `PermissionGate` to decide whether to render an action or block it.

### Permission Gate — `src/components/domain/permission-gate.tsx`

`PermissionGate` is a wrapper component that takes a `permission` string. The permission identifiers are `UPPER_SNAKE_CASE` strings defined in `src/lib/permissions.ts`: `ADMIN` grants `PRODUCT_MANAGE`, `CUSTOMER_MANAGE`, `ORDER_SHIP`, `ORDER_DELIVER`, `ORDER_CANCEL_CONFIRMED`, `AUDIT_VIEW`, and `ADMIN_PANEL`; `CUSTOMER` grants `ORDER_PLACE`, `ORDER_CANCEL_OWN`, and `PROFILE_MANAGE`; `GUEST` grants nothing. If the current role has the requested permission, it renders its children unchanged. If not, it renders the children inside a dimmed wrapper (50% opacity, `cursor-not-allowed`) that intercepts all click events to show a toast: *"This action requires the `<role>` role."* — where `<role>` is whichever role grants the permission (`CUSTOMER` for things like `ORDER_PLACE`, otherwise `ADMIN`), computed by `requiredRoleFor(permission)`. A small `Shield` icon from Lucide appears in the top-right corner of the blocked element as a visual cue, and a tooltip reads "Requires `<role>` role".

This design means that button components never need to know about roles. A `<Button onClick={handleDelete}>Delete</Button>` becomes `<PermissionGate permission="PRODUCT_MANAGE"><Button onClick={handleDelete}>Delete</Button></PermissionGate>`. The gate handles both the visual feedback and the click interception; the inner button never fires its handler when blocked.

---

## Guided Tour of the Eight Pages

### Dashboard

The dashboard is the landing page at `/`. It displays four stat cards — **Products**, **Orders**, **Customers**, and **Live Events**. There is no dedicated `/api/dashboard` endpoint: the page derives every value client-side. The first three cards read `products.length`, `orders.length`, and `customers.length` from the existing `useProducts()`, `useOrders()`, and `useCustomers()` queries (with subtitles like "N available" / "N pending" / "N active" computed from the same arrays). The fourth card, titled **Live Events** with the subtitle "Last 50 events", shows `useSSE().recentEvents.length` — the count of the most recent live SSE events, capped at 50, not a one-hour window.

Below the stat cards are two panels: a **Recent Orders** table (the five most recent orders) and a **Live Event Feed**. The feed reads from `useSSE().recentEvents`, so it updates in real time without any polling. Each entry shows the event type, stream ID, and timestamp. The feed is capped at the most recent 50 events.

### Products

The Products page (`/products`) lists all products with their current price and stock level. Switching to the `ADMIN` role unlocks four permission-gated actions available in a per-row `⋯` dropdown menu:

- **Adjust Stock** — opens a dialog that `PUT`s a delta to `/api/products/{id}/stock`
- **Update Price** — opens a dialog that `PUT`s a new price to `/api/products/{id}/price`
- **Receive Shipment** — opens a dialog that `POST`s a quantity to `/api/inventory/{id}/receive`
- **Discontinue** — `POST`s to `/api/products/{id}/discontinue`

An **Add Product** button at the top is gated behind the same `PRODUCT_MANAGE` permission as the row actions — there is no separate create-vs-modify permission; all product mutations (create, adjust stock, update price, receive shipment, discontinue) are covered by `PRODUCT_MANAGE`. As a `GUEST` or `CUSTOMER`, all of these actions are visible but dimmed and blocked by `PermissionGate`. Clicking them shows the toast without dispatching any command.

### Orders

The Orders page (`/orders`) shows a table of orders. Expanding a row reveals the order lines (product ID, quantity, and unit price). A status badge in the collapsed row shows the current order state. The lifecycle buttons — **Confirm**, **Ship**, **Deliver**, **Cancel** — appear only for the transitions that are valid in the current state; the backend enforces the same rules, but the UI mirrors them to avoid showing buttons that would immediately fail.

As a `CUSTOMER`, the page filters orders to the current user's orders by passing `?customerId=` to `GET /api/orders`. Switching to Alice or Bob in the role switcher re-filters the list immediately.

### Customers

The Customers page (`/customers`) renders its table of registered customers for *any* role — there is no page-level role guard here (unlike Audit, below). What is gated is the action menu: the **Register Customer** button and each row's `⋯` actions dropdown are wrapped in `<PermissionGate permission="CUSTOMER_MANAGE">`, so a `GUEST` or `CUSTOMER` still sees the table but the actions are blocked. The row actions are:

- **Edit Profile** — opens a dialog that `PUT`s to `/api/customers/{id}` (the `updateProfile` call in `api.ts`), which writes a `ProfileUpdated` event.
- **Export Data** — `POST`s to `/api/customers/{id}/export-data` (`@PostMapping("/{id}/export-data")` on `CustomerCommandController`), which writes a `DataExportRequested` event.
- **Forget (GDPR)** — `POST`s to `/api/customers/{id}/forget`, which writes a `CustomerForgotten` event, deletes the customer's encryption key and purges the read-model row — the exact behavior you implemented in Chapter 7. The backend lets the ADMIN persona through its ADMIN-or-the-customer-themself check (Chapter 9). If the erasure stops part-way the backend answers `500` and the dialog shows that error; sending the same request again finishes it (Chapter 7, Step 2) — from the dialog while it is open, or with `curl` once the projection has dropped the row from the table. The table row updates automatically when the SSE event invalidates the `["customers"]` cache.

### Fulfillment

The Fulfillment page (`/fulfillment`) displays a `SagaCard` for each order fulfillment saga. The saga status is one of `AWAITING_PAYMENT`, `AWAITING_INVENTORY`, `AWAITING_CONFIRMATION`, `COMPLETED`, `COMPENSATING`, or `FAILED` (the `OrderFulfillmentStatus` type). Each card renders a fixed four-step visualizer — **Payment → Inventory → Confirmation → Completed** — where steps before the current status show a green check, the current status shows a spinner (or a red ✗ when the saga is `FAILED`/`COMPENSATING`), and later steps are greyed out.

This page does **not** have a per-saga inject-failure button. Instead it has a single page-level **Inject Payment Failure** switch in the header, gated behind `ADMIN_PANEL`, that calls `useTogglePaymentFailure()` → `POST /api/admin/payment-failure/toggle`. Flip it on and a warning banner appears; the next order then fails at the payment step, letting you watch the saga compensate in real time. (The backend's `POST /api/saga/fulfillments/{id}/inject-failure` route exists but is not wired into this page; despite the saga id in its path it flips the same shared flag, behind the same ADMIN check — Chapter 11.)

### Events

The Events page (`/events`) has two panels. The **top panel is the Live Feed**, powered by `useSSE().recentEvents`. Each entry is expandable to show the full event payload. A green pulse dot and "Connected"/"Disconnected" label confirm the SSE connection state.

The **bottom panel is the Historical Browser**: it reads from `GET /api/events?offset=0&limit=25` and provides **Previous / Next** pagination in pages of 25. A **Filter by stream** search box above the table lets you switch to per-stream events: it takes `type:id` (for example `product:p-1`), splits the input at the first `:` and calls `GET /api/events/{aggregateType}/{aggregateId}` — useful for inspecting the full history of a single order or product. An input without `:` shows a hint and sends no request. Each row shows the global offset, stream ID, event type, timestamp, and a collapsible JSON payload viewer.

Because both panels share the same underlying data source, a `StockAdjusted` event you trigger on the Products page appears in the Live Feed within milliseconds and in the Historical Browser after the next `["events"]` cache invalidation.

### Audit

The Audit page (`/audit`) is the one page with a real page-level role guard: when the current role lacks `AUDIT_VIEW` it renders an "Access Restricted" screen instead of the table. For an `ADMIN`, it fetches the audit log from `GET /api/audit/commands?limit=...` (`@GetMapping("/commands")` under `@RequestMapping("/api/audit")` on `AuditController`). Each entry records the command ID, command type, aggregate ID, user, timestamp, outcome, and event count — the data written by `AuditCommandInterceptor` in Chapter 10.

A **Show** selector at the top lets you choose how many entries to load (25, 50, or 100; the default is 25). There is no client-side report download — just the limit selector and the table.

### Admin

The Admin page (`/admin`) is a control panel for the operational features from Chapters 12, 13, and 14.

**Circuit breaker** — calls `GET /api/admin/circuit-breaker`, which returns both breaker states: `state` (the command-bus `CircuitBreakerCommandInterceptor`) and `paymentGateway` (the `PaymentGatewayCircuitBreaker` around the gateway call), each one of `CLOSED`, `OPEN`, or `HALF_OPEN`. The `["admin", ...]` query keys are not part of the SSE invalidation mapping and `useCircuitBreaker()` sets no `refetchInterval`, so the card refreshes on normal TanStack Query triggers (mount, window refocus) and whenever a role change invalidates the whole cache — not on a fixed timer.

**Dead Letter Queue** — lists entries from `GET /api/admin/dead-letters`. Each entry shows the command type, failure reason (error message), and timestamp. A **Retry** button calls `POST /api/admin/dead-letters/{id}/retry`, which re-dispatches the dead-lettered command through the bus via `DeadLetterRetryRunner.retry(...)` (discarding the entry on success) — the feature you built in Chapter 14.

**Outbox** — lists the `PENDING` outbox entries from `GET /api/admin/outbox` (a read-only `findByStatus(PENDING)`, so refreshing the page never stalls delivery — Chapter 14). Each entry shows the entry ID, event type, and created-at timestamp — the transactional outbox from Chapter 13. The page does not list `FAILED` entries; resolve those with the replay and skip endpoints from Chapter 14.

**System Health** — calls `GET /actuator/health` and surfaces the overall status plus the `streamRune` health component (event store offset and timestamp) — the same data as the `StreamRuneHealthIndicator` you verified in Chapter 15.

**Payment failure toggle** — a single switch backed by `GET /api/admin/payment-failure` and `POST /api/admin/payment-failure/toggle`. Flipping it on causes the next payment saga to fail, which lets you trigger the compensation path without mocking anything. Flip it on, place an order on the Orders page, then watch the Fulfillment page to see the saga compensate.

Non-`ADMIN` roles see all cards rendered read-only (dimmed at 50% opacity, all action buttons disabled) rather than hidden — the page is still visible so the state is observable.

---

## What We Learned

**Frontend-backend integration without a BFF.** The frontend calls the same REST endpoints used by `curl` in the backend chapters. The `X-User-Role` and `X-User-Id` headers carry the identity context that the `AnnotationAuthorizationInterceptor` enforces. There is no session, no JWT, no cookie — just headers, which makes it trivial to test endpoints directly without a running frontend. That convenience is the demo's shortcut, not a pattern: the backend believes those headers only because it runs in the trusted-gateway mode with a header-based role resolver (Chapter 9), and a real deployment puts authentication in front of it.

**SSE in React.** A single `EventSource` at the application root — not one per component — avoids the thundering-herd problem where twenty components each open their own SSE connection. The `SSEProvider` distributes events to any component via `useSSE()`, and the TanStack Query cache invalidation means components that do not subscribe to `useSSE()` directly still get updated automatically when relevant events arrive.

**Role-based UI without conditional rendering everywhere.** `PermissionGate` centralises the role check to a single component. Pages do not contain `if (role === "ADMIN")` guards scattered through JSX. The result is that a role change in the sidebar immediately reflects across every page because the `RoleProvider` invalidates the entire query cache, and `PermissionGate` re-evaluates on every render with the current role.

**Real-time updates as a side effect of event sourcing.** The backend's event store is the source of truth. Every mutation dispatches a command that appends one or more events. Those events flow through the SSE endpoint to the frontend. The frontend invalidates its cache on receipt. The combination means the UI is eventually consistent with the event store within the SSE polling interval — typically one second or less — without any dedicated WebSocket infrastructure or push mechanism beyond the standard `EventSource` API.

---

## Next Up

Let's put it all together and run the complete demo end-to-end.
