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
| Orders | Order lifecycle workflow with expandable rows: every order for an `ADMIN`, a customer's own for a `CUSTOMER` |
| Customers | GDPR features — data export and right to forget: every customer for an `ADMIN`, a customer's own record for a `CUSTOMER` |
| Fulfillment | Saga step visualizer per order (ADMIN-only) |
| Events | Historical event browser plus live SSE feed |
| Audit | Audit log (ADMIN-only), with a row-count selector |
| Admin | Circuit breaker state, DLQ, outbox, payment failure toggle |

The technology choices are minimal on purpose. The stack is Next.js (16.3.8), React 19, TanStack Query v5, Tailwind CSS v4, shadcn-style components built on the `@base-ui/react` primitive layer, and the browser's native `EventSource` API — nothing exotic. One thing to note if you inspect the components: the `@base-ui/react` triggers use a render-prop pattern (e.g. `<DialogTrigger render={<Button />}>`) rather than `asChild`.

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

> **The browser plays the gateway here.** The backends run in StreamRune's trusted-gateway mode (Chapter 9), so `X-User-Id` is the caller's identity and nothing but the browser vouches for it. Every role that calls a guarded endpoint needs one: an `ADMIN` is the role together with an identity on every endpoint, and a `@RequireRole` command without `X-User-Id` is refused with `Authentication required` whatever `X-User-Role` says, which is why `ADMIN` sends `admin-1` with every request. `X-User-Role` is the demo's role shortcut — the backend's `HeaderUserRoleResolver` believes it as sent. Both are fine for a local showcase and wrong anywhere else: in production a gateway or the backend's own security framework authenticates the user and sets the identity, and the roles come from that authenticated identity, never from a header the browser picks. The same holds for the Orders page's per-customer view: its `?customerId=` filter is a query parameter the browser chooses, not a rule the backend enforces.

The wrapper also handles the two content types the backend returns: JSON responses are parsed with `res.json()`; anything else is returned as text. Any non-2xx status throws an `ApiError` that carries the HTTP status and a message a toast can show as it is: "Not allowed for the selected role (403): …" for a refusal of the access rule, "The backend refused the request (400): …" with the decider's or the validation's own words for a request the backend does not accept. A page never fails silently: every mutation's `catch` shows that message.

Named exports like `fetchProducts()`, `placeOrder(body)`, and `togglePaymentFailure()` wrap `apiFetch` and carry full TypeScript types for their arguments and return values. **Those types are the backend's contracts, field for field.** A command's body is typed as the controller's request record reads it — `placeOrder` takes `{ orderId, customerId, lines: [{ productId, quantity, unitPrice }] }` for `PlaceOrderRequest`, `adjustStock` takes `{ quantity, reason }` for `AdjustStockRequest`, `updatePrice` takes `{ price }` — and a read is typed as the endpoint answers it, `{ failureInjected }` for the payment-failure switch. This matters more than it looks: the Spring app ignores a JSON field it does not know, so a body that says `delta` where the record says `quantity` is not refused. It adjusts the stock by zero and answers `200`. The compiler catches that mistake here, and `tests/e2e/write-contracts.spec.ts` drives every write action through its page and compares the request with the contract.

Three properties of the backend shape the write side:

- **A command answers `200` with an empty body.** The events are stored; what changed is read from the read models. Every command function therefore returns `Promise<void>` (the one exception is `forgetCustomer`, whose answer says whether the erasure is complete, Chapter 7).
- **The client picks the id.** `POST /api/orders`, `POST /api/customers` and `POST /api/products` create nothing under an id of their own: the id is a required field of the request, as in every `curl` example of this tutorial. `src/lib/ids.ts` generates one in the form the seed data uses — `order-`, `cust-` or `prod-` followed by eight hex digits of a random UUID — and the success toast shows it. An id that is taken is the backend's to refuse (`400 … already exists`).
- **An order line carries its unit price.** The Place Order dialog takes it from the product the line names, as `GET /api/products` lists it.

### SSE Provider — `src/providers/sse-provider.tsx`

`SSEProvider` wraps the entire application. It opens a single `EventSource` to `/api/events/sse` on mount and keeps it alive for the lifetime of the page. This is the one backend call that carries no `X-User-Id` and no `X-User-Role`: an `EventSource` cannot send request headers. The backend therefore keeps the feed open to everyone and sends frames that name no stream and carry no payload (Chapter 15) — a frame is a `LiveEvent`: global offset, aggregate type, event type and timestamp. It tells the page that something of a kind happened, not to whom. The feed is live: a connection starts at the head of the event store, so a page that has just loaded receives no frame until something happens, and its lists come from the queries the pages run on mount. Two pieces of state are exposed through `useSSE()`: a `connected` boolean for the status dot on the **Event Explorer** item in the sidebar (a small colored dot that pulses green when connected, muted when disconnected), and a `recentEvents` array capped at 50 entries for the live feed widgets.

When an SSE message arrives, the provider does two things. First, it prepends the parsed `LiveEvent` to `recentEvents` — unless its global offset is not above the highest one seen so far, so a frame that reaches the page twice is counted once. Second, it calls `queryClient.invalidateQueries` for every TanStack Query cache key that is affected by the event type. A frame has no id to select one order or one customer by, so the page reads the whole query of that kind again, with the session's identity headers, and the backend answers with what that identity may see. The mapping is encoded in `invalidationKeysFor(eventType)`:

- Product events invalidate `["products"]`
- Order events invalidate `["orders"]` and `["fulfillment"]`: the saga starts with `OrderPlaced` and ends with `OrderConfirmed` or `OrderCancelled`
- Customer events invalidate `["customers"]`
- Payment events (`PaymentInitiated`, `PaymentCaptured`, `PaymentRefunded`, `PaymentFailed`) invalidate `["fulfillment"]` only
- Inventory events (`StockReserved`, `StockReleased`, `ReservationConfirmed`, `ShipmentReceived`) invalidate both `["products"]` and `["fulfillment"]`
- All events also invalidate `["dashboard"]` and `["events"]`

**A frame is early; the read models are late.** The feed announces an event within a second of its commit. The lists the pages show are read models, and their projections poll the event store every five seconds plus up to one of jitter by default (Chapter 9), while the saga's and the payment process manager's subscriptions poll every 500 ms. An order placed on the Orders page is therefore confirmed by the saga before the order read model has its row: every frame of the order, from `OrderPlaced` to `OrderConfirmed`, reaches the browser while `GET /api/orders` still returns the list from before. Reading again on each frame is not enough.

So a change makes the page read twice. `refreshAndAgainOnceSettled` in `src/lib/read-model-sync.ts` invalidates the affected keys at once and once more `READ_MODEL_SETTLE_MS` — seven seconds — after the last change to each key: the first read shows what is already there, the second is the one that is certain to see the change. A key that changes again before its second read waits from the later change, so a burst of frames ends in one read per key, not one per frame. The SSE provider calls it for every frame, and every mutation calls it when the backend has answered its command, which also covers a browser whose feed is disconnected.

The result is that placing an order in one browser tab refreshes the order list in that tab and in every other one within a few seconds — no reload, no polling loop, no manual refetch. Every page that uses TanStack Query is kept in sync as long as the SSE connection is open.

When the connection drops, the provider reconnects using exponential backoff. The delay starts at one second and doubles on each failure, capping at thirty seconds. The retry counter resets to zero when the connection reopens successfully. The feed does not send what was stored while the page was away, so when the connection is back the provider invalidates every query once: each visible list is read again with the session's headers, and the others are stale for their next use.

### Role Provider — `src/providers/role-provider.tsx`

`RoleProvider` owns the concept of "who is currently using the app." It exposes three roles — `GUEST`, `CUSTOMER`, and `ADMIN` — selectable from a role switcher at the bottom of the sidebar.

When the role changes, three things happen in sequence:

1. `localStorage` is updated so the selection survives a page refresh.
2. `setApiAuth(role, userId)` is called to update the API layer's headers immediately.
3. `queryClient.resetQueries()` is called with no filter, which drops the entire cache and refetches what is on screen. What the backend returns depends on the identity — a `CUSTOMER` gets their own orders, an `ADMIN` everyone's, a `GUEST` a `403` — so nothing one identity loaded is shown under the next one.

The provider also tracks the `userId` the API layer sends, chosen per role by `userIdForRole` in `src/lib/identity.ts`: `admin-1` for `ADMIN`, none for `GUEST`, and for `CUSTOMER` the selected customer, `cust-alice` by default. For `CUSTOMER` a second selector at the bottom of the sidebar lets you switch between Alice and Bob, which shows the per-customer views on the Orders and Customers pages. The backend enforces those views (Chapter 9): as Alice the browser is sent Alice's record and Alice's orders and nothing else. The two names in the selector are the demo's seeded customers, `cust-alice` and `cust-bob` (`SeedDataRunner`), kept as a constant in the provider — the selector needs identities to offer and gets them without asking the backend for a customer list, which a customer would not be given. `tests/e2e/identity-headers.spec.ts` pins the headers each role sends.

The provider exposes a `hasPermission(permission: string): boolean` function that delegates to a `permissions.ts` module. This is the source of truth used by `PermissionGate` to decide whether to render an action or block it.

### Permission Gate — `src/components/domain/permission-gate.tsx`

`PermissionGate` is a wrapper component that takes a `permission` string. The permission identifiers are `UPPER_SNAKE_CASE` strings defined in `src/lib/permissions.ts`: `ADMIN` grants `PRODUCT_MANAGE`, `CUSTOMER_MANAGE`, `ORDER_PLACE`, `ORDER_CONFIRM_CANCEL`, `ORDER_SHIP`, `ORDER_DELIVER`, `AUDIT_VIEW`, `EVENT_HISTORY_VIEW`, `FULFILLMENT_VIEW`, `OWN_DATA_VIEW`, and `ADMIN_PANEL`; `CUSTOMER` grants `ORDER_PLACE`, `ORDER_CONFIRM_CANCEL`, `PROFILE_MANAGE`, `OWN_DATA_VIEW`, and `FULFILLMENT_VIEW`; `GUEST` grants nothing. The table mirrors what the backend decides, so that a page can say what it requires instead of sending a request that would be answered `403`: `OWN_DATA_VIEW` stands for "has an identity", the condition of every customer and order read. If the current role has the requested permission, it renders its children unchanged. If not, it renders the children inside a dimmed wrapper (50% opacity, `cursor-not-allowed`) that intercepts all click events to show a toast: *"This action requires the `<role>` role."* — where `<role>` is whichever role grants the permission (`CUSTOMER` for what a customer may do too, like `ORDER_PLACE`, otherwise `ADMIN`), computed by `requiredRoleFor(permission)`. A small `Shield` icon from Lucide appears in the top-right corner of the blocked element as a visual cue, and a tooltip reads "Requires `<role>` role".

This design means that button components never need to know about roles. A `<Button onClick={handleDelete}>Delete</Button>` becomes `<PermissionGate permission="PRODUCT_MANAGE"><Button onClick={handleDelete}>Delete</Button></PermissionGate>`. The gate handles both the visual feedback and the click interception; the inner button never fires its handler when blocked.

A whole page that a role may not read uses the gate's sibling, `AccessRestricted` (`src/components/domain/access-restricted.tsx`): an "Access Restricted" screen with one line saying which role the page requires. A page in that state sends no request — its queries take an `enabled` flag (`useOrders(customerId, enabled)`, `useCustomers(enabled)`, `useSagaList(limit, enabled)`, `useAuditLog(limit, enabled)`), and the page passes `false`.

---

## Guided Tour of the Eight Pages

### Dashboard

The dashboard is the landing page at `/`. It displays four stat cards — **Products**, **Orders**, **Customers**, and **Live Events**. There is no dedicated `/api/dashboard` endpoint: the page derives every value client-side. The first three cards read `products.length`, `orders.length`, and `customers.length` from the existing `useProducts()`, `useOrders()`, and `useCustomers()` queries (with subtitles like "N available" / "N pending" / "N active" computed from the same arrays). The fourth card, titled **Live Events** with the subtitle "Since this page opened", shows `useSSE().recentEvents.length` — the count of the frames received since the page connected, capped at 50. It starts at zero: the feed sends what happens while the page is open, not what happened before.

The **Orders** and **Customers** cards follow the role, as the two lists behind them do (Chapter 9). An `ADMIN` counts every order and every customer. A `CUSTOMER` counts their own orders ("N pending, your own") and sees "Your own record" on the Customers card. A `GUEST` has no identity, so the page sends neither request and both cards show "—" with "Requires CUSTOMER or ADMIN role"; the Recent Orders panel says the same. Products and the live feed are open to everyone.

Below the stat cards are two panels: a **Recent Orders** table (the five most recent orders) and a **Live Event Feed**. The feed reads from `useSSE().recentEvents`, so it updates in real time without any polling. Each entry shows the event type, the aggregate type, and the timestamp; the feed names no stream. The panel is empty until the first event after the page connected and then holds the most recent 50.

### Products

The Products page (`/products`) lists all products with their current price and stock level; the catalogue is open to every role. Its writes are an `ADMIN`'s on the backend (Chapters 9 and 13), and switching to the `ADMIN` role unlocks four permission-gated actions available in a per-row `⋯` dropdown menu:

- **Adjust Stock** — opens a dialog that `PUT`s `{ quantity, reason }` to `/api/products/{id}/stock`. `quantity` is the change to apply, negative to remove; the backend refuses a change below zero with `400 Insufficient stock`, and the dialog shows that message
- **Update Price** — opens a dialog that `PUT`s `{ price }`, a plain USD amount, to `/api/products/{id}/price`
- **Receive Shipment** — opens a dialog that `POST`s `{ quantity }` to `/api/inventory/{id}/receive`. The inventory is its own aggregate (Chapter 13), the one the fulfillment saga reserves from: the Stock column of this table is the product's own figure and does not move with a shipment, which the dialog says. An order for a product created on this page is fulfilled once a shipment for it is received; without one the saga cancels the order
- **Discontinue** — `POST`s to `/api/products/{id}/discontinue`

An **Add Product** button at the top opens a dialog that `POST`s `{ productId, name, description, category, price, initialStock }` to `/api/products`, with a `productId` the page generates (`prod-` and eight hex digits) and the price as a plain USD amount. It is gated behind the same `PRODUCT_MANAGE` permission as the row actions — there is no separate create-vs-modify permission; all product mutations (create, adjust stock, update price, receive shipment, discontinue) are covered by `PRODUCT_MANAGE`. As a `GUEST` or `CUSTOMER`, all of these actions are visible but dimmed and blocked by `PermissionGate`. Clicking them shows the toast without dispatching any command.

### Orders

The Orders page (`/orders`) shows a table of orders. Expanding a row reveals the order lines (product ID, quantity, and unit price). A status badge in the collapsed row shows the current order state. The table shows each order's id and its customer's id in full. The lifecycle buttons — **Confirm**, **Ship**, **Deliver**, **Cancel** — appear only for the transitions that are valid in the current state; the backend enforces the same rules, but the UI mirrors them to avoid showing buttons that would immediately fail. **Confirm** shows on a `CREATED` order, which the fulfillment saga confirms itself within seconds; **Ship** on a `CONFIRMED` one, **Deliver** on a `SHIPPED` one. **Cancel** shows on a `CREATED` and on a `CONFIRMED` order and opens a dialog with an optional reason: `POST /api/orders/{id}/cancel` reads the reason from a JSON body and answers `400` to a request without one, so the dialog always sends `{ reason }`. **Confirm** and **Cancel** are gated by `ORDER_CONFIRM_CANCEL`, which a `CUSTOMER` and an `ADMIN` hold, as on the backend: the order's customer or an `ADMIN`. **Ship** and **Deliver** are an `ADMIN`'s.

Whose orders the table holds is the backend's answer (Chapter 9). An `ADMIN` gets every order, and `?customerId=` in the address narrows the list to one customer — the link a row of the Customers page follows. A `CUSTOMER` gets their own orders: the page sends `GET /api/orders` without a filter, whatever the address says, and the backend returns the orders of the `X-User-Id` it was sent; the subtitle reads "N total for cust-alice (your own orders)". Switching to Alice or Bob in the role switcher reloads the list. A `GUEST` sees "Access Restricted — Requires CUSTOMER or ADMIN role to view orders." and the page sends no request. **Place Order** is for a `CUSTOMER` and an `ADMIN` (`ORDER_PLACE`). The dialog sends what `PlaceOrderRequest` reads: an `orderId` the page generates (`order-` and eight hex digits), the `customerId`, and for each line the `productId`, the `quantity` and the `unitPrice` of the product picked. The toast shows the id ("Order placed: order-1a2b3c4d"), and the row appears a few seconds later, usually already `CONFIRMED` — the read model's delay described above. Its customer selector is filled from `GET /api/customers`. For a `CUSTOMER` that holds their own record and nothing else, the only customer the backend accepts an order from them for; for an `ADMIN` it holds every customer.

### Customers

The Customers page (`/customers`) shows what `GET /api/customers` returns for the current identity (Chapter 9): every registered customer for an `ADMIN`, the customer's own record for a `CUSTOMER` ("Your own record (cust-alice)"), and for a `GUEST` an "Access Restricted — Requires CUSTOMER or ADMIN role to view customers." screen without a request. The actions follow the backend's rule too — an `ADMIN`, or the customer themself. The table shows each customer's id next to the name. The **Register Customer** button is wrapped in `<PermissionGate permission="CUSTOMER_MANAGE">`, an `ADMIN`'s permission: its dialog `POST`s `{ customerId, name, email, address, phone }` with a `customerId` the page generates (`cust-` and eight hex digits), and the backend lets only an `ADMIN` register an id that is not the caller's own; a row's `⋯` actions dropdown is gated by `CUSTOMER_MANAGE` for an `ADMIN` and by `PROFILE_MANAGE` on the row of the customer who is looking at it, so a `CUSTOMER` can edit, export and erase their own record. The row actions are:

- **Edit Profile** — opens a dialog that `PUT`s to `/api/customers/{id}` (the `updateProfile` call in `api.ts`), which writes a `ProfileUpdated` event.
- **Export Data** — `POST`s to `/api/customers/{id}/export-data` (`@PostMapping("/{id}/export-data")` on `CustomerCommandController`), which writes a `DataExportRequested` event.
- **Forget (GDPR)** — `POST`s to `/api/customers/{id}/forget`, which writes a `CustomerForgotten` event, deletes the customer's encryption key and purges the read-model row — the exact behavior you implemented in Chapter 7. The backend lets the ADMIN persona, and the customer themself, through its ADMIN-or-the-customer-themself check (Chapter 9). If the erasure stops part-way the backend answers `500` and the dialog shows that error; sending the same request again finishes it (Chapter 7, Step 2) — from the dialog while it is open, or with `curl` once the projection has dropped the row from the table. The table row updates automatically when the SSE event invalidates the `["customers"]` cache.

### Fulfillment

The Fulfillment page (`/fulfillment`) has a page-level role guard, like Orders: a saga names the order, the customer and the payment of one purchase, so the backend serves the saga list as it serves orders — every saga to an `ADMIN`, the sagas of their own orders to a `CUSTOMER`, and `403` to a caller without an identity (Chapter 11). A `GUEST` has no `FULFILLMENT_VIEW` and sees "Access Restricted — Requires CUSTOMER or ADMIN role to view fulfillment sagas."; the page sends no request. For an `ADMIN` the page displays a `SagaCard` for every order fulfillment saga; for a `CUSTOMER` the same request, sent with their own `X-User-Id`, returns their own sagas only, and the subtitle reads "Saga-based fulfillment status of your own orders". A card shows the saga id (`fulfillment-<orderId>`), the saga type and the status the list returns: the framework's `SagaStatus` (`STARTED`, `RUNNING`, `COMPLETED`, `COMPENSATING`, `COMPENSATED`, `FAILED` or `FAULTED`), not the domain's `OrderFulfillmentStatus`. Each card renders a three-step tracker — **Started → In Progress → Completed** — where steps before the current status show a green check, the current status shows a spinner (or a red ✗ when the saga is failed, faulted or compensating), and later steps are greyed out.

This page does **not** have a per-saga inject-failure button. Instead it has a single page-level **Inject Payment Failure** switch in the header, shown to a role with `ADMIN_PANEL` only — a `CUSTOMER` sees neither the switch nor a request for the flag behind it — that calls `useTogglePaymentFailure()` → `POST /api/admin/payment-failure/toggle`. The switch shows the backend's flag: both the toggle and `GET /api/admin/payment-failure` answer `{ "failureInjected": true | false }`, and the page reads that field. Flip it on and a warning banner appears; the next order then fails at the payment step, letting you watch the saga compensate in real time. (The backend's `POST /api/saga/fulfillments/{id}/inject-failure` route exists but is not wired into this page; despite the saga id in its path it flips the same shared flag, behind the same ADMIN check — Chapter 11.)

### Events

The Events page (`/events`) has two panels. The **top panel is the Live Feed**, powered by `useSSE().recentEvents`, and every role sees it. Each row shows the global offset, the event type, the aggregate type and the time — the feed names no stream and carries no payload, so there is nothing to expand and no id to read off it. A green pulse dot and "Connected"/"Disconnected" label confirm the SSE connection state.

The **bottom panel is the Historical Browser**, and it is for the `ADMIN` role: the backend returns each event with its payload decrypted and answers `403` to anyone else (Chapter 15). For a role without the `EVENT_HISTORY_VIEW` permission the panel shows "Requires ADMIN role to browse the event history." and sends no request (`useEvents` and `useStreamEvents` are disabled). For an `ADMIN` it reads from `GET /api/events?offset=0&limit=25`, with the role headers `apiFetch` adds to every call, and provides **Previous / Next** pagination in pages of 25. A **Filter by stream** search box above the table lets you switch to per-stream events: it takes `type:id` (for example `product:p-1`), splits the input at the first `:` and calls `GET /api/events/{aggregateType}/{aggregateId}` — useful for inspecting the full history of a single order or product. An input without `:` shows a hint and sends no request. Each row shows the global offset, stream ID, event type, timestamp, and a collapsible JSON payload viewer.

Because both panels read the same event store, a `StockAdjusted` event you trigger on the Products page appears in the Live Feed within about a second and, for an `ADMIN`, in the Historical Browser after the next `["events"]` cache invalidation — which that same frame triggers. That is how an `ADMIN` sees which stream a live event belongs to: the frame says "read again", and the history, asked with the `ADMIN`'s headers, answers with the stream and the payload.

### Audit

The Audit page (`/audit`) has a page-level role guard: when the current role lacks `AUDIT_VIEW` it renders an "Access Restricted" screen instead of the table and sends no request — the backend answers `403` to anyone but an `ADMIN` (Chapter 10). For an `ADMIN`, it fetches the audit log from `GET /api/audit/commands?limit=...` (`@GetMapping("/commands")` under `@RequestMapping("/api/audit")` on `AuditController`). Each entry records the command ID, command type, aggregate ID, user, timestamp, outcome, and event count — the data written by `AuditCommandInterceptor` in Chapter 10.

A **Show** selector at the top lets you choose how many entries to load (25, 50, or 100; the default is 25). There is no client-side report download — just the limit selector and the table.

### Admin

The Admin page (`/admin`) is a control panel for the operational features from Chapters 12, 13, and 14.

**Circuit breaker** — calls `GET /api/admin/circuit-breaker`, which returns both breaker states: `state` (the command-bus `CircuitBreakerCommandInterceptor`) and `paymentGateway` (the `PaymentGatewayCircuitBreaker` around the gateway call), each one of `CLOSED`, `OPEN`, or `HALF_OPEN`. The `["admin", ...]` query keys are not part of the SSE invalidation mapping and `useCircuitBreaker()` sets no `refetchInterval`, so the card is read when the page mounts and whenever a role change resets the whole cache — not on a fixed timer.

**Dead Letter Queue** — lists entries from `GET /api/admin/dead-letters`, the framework's `DeadLetterEntry` as JSON. Each row shows the command type (the class's simple name, from `commandType`), the failure reason (`errorMessage`) and the time of the first attempt (`firstAttemptAt`). A **Retry** button calls `POST /api/admin/dead-letters/{commandId}/retry` with the entry's `commandId`, which re-dispatches the dead-lettered command through the bus via `DeadLetterRetryRunner.retry(...)` (discarding the entry on success) — the feature you built in Chapter 14.

**Outbox** — lists the `PENDING` outbox entries from `GET /api/admin/outbox` (a read-only `findByStatus(PENDING)`, so refreshing the page never stalls delivery — Chapter 14). Each entry shows the entry `id`, the event type (`payloadType`, e.g. `OrderConfirmed`), and the `createdAt` timestamp — the transactional outbox from Chapter 13. The page does not list `FAILED` entries; resolve those with the replay and skip endpoints from Chapter 14.

**System Health** — calls `GET /api/admin/health` and shows its `status`, `UP` while the backend answers. It is the one health answer the three apps share, it is open to every caller, and it lies under `/api`, the path the backend's CORS mapping (`CorsConfig`) allows the browser to call from `http://localhost:3000`. The detailed health of Chapter 15 — `GET /actuator/health` with the `streamRune` component, the event store's offset and timestamp — is the Spring app's alone and outside that mapping; read it with `curl`.

**Payment failure toggle** — a single switch backed by `GET /api/admin/payment-failure` and `POST /api/admin/payment-failure/toggle`, both of which answer `{ "failureInjected": … }`; the switch shows that field. Flipping it on causes the next payment saga to fail, which lets you trigger the compensation path without mocking anything. Flip it on, place an order on the Orders page, then watch the Fulfillment page to see the saga compensate.

Non-`ADMIN` roles see all cards rendered read-only (dimmed at 50% opacity, all action buttons disabled) and empty: the admin endpoints answer `403` to everyone but an `ADMIN` (Chapters 12 and 14), so for those roles the cards' queries are disabled and no request is sent.

---

## What We Learned

**Frontend-backend integration without a BFF.** The frontend calls the same REST endpoints used by `curl` in the backend chapters. The `X-User-Role` and `X-User-Id` headers carry the identity context that the `AnnotationAuthorizationInterceptor` enforces. There is no session, no JWT, no cookie — just headers, which makes it trivial to test endpoints directly without a running frontend. That convenience is the demo's shortcut, not a pattern: the backend believes those headers only because it runs in the trusted-gateway mode with a header-based role resolver (Chapter 9), and a real deployment puts authentication in front of it.

**SSE in React.** A single `EventSource` at the application root — not one per component — avoids the thundering-herd problem where twenty components each open their own SSE connection. The `SSEProvider` distributes events to any component via `useSSE()`, and the TanStack Query cache invalidation means components that do not subscribe to `useSSE()` directly still get updated automatically when relevant events arrive.

**Role-based UI without conditional rendering everywhere.** `PermissionGate` centralises the role check to a single component. Pages do not contain `if (role === "ADMIN")` guards scattered through JSX. The result is that a role change in the sidebar immediately reflects across every page because the `RoleProvider` resets the entire query cache, and `PermissionGate` re-evaluates on every render with the current role.

**Real-time updates as a side effect of event sourcing.** The backend's event store is the source of truth. Every mutation dispatches a command that appends one or more events. Those events flow through the SSE endpoint to the frontend. The frontend invalidates its cache on receipt. The combination means the UI is eventually consistent with the event store within the SSE polling interval — typically one second or less — without any dedicated WebSocket infrastructure or push mechanism beyond the standard `EventSource` API.

---

## Next Up

Let's put it all together and run the complete demo end-to-end.
