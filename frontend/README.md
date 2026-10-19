# StreamRune e-commerce demo — frontend

The browser side of the demo: a Next.js 16 / React 19 application that talks to the demo's Spring
Boot backend over plain HTTP and keeps itself current through the backend's live event feed.
Chapter 16 of the tutorial (`../docs/tutorial/16-frontend.md`) walks through how it is built.

## Run it

Start the backend first (see the repository README), then:

```bash
npm ci
npm run dev
```

Open http://localhost:3000. The frontend calls the backend at `http://localhost:8080`; set
`NEXT_PUBLIC_API_URL` when it listens somewhere else.

`docker compose up` in the repository root builds and starts the frontend together with the
backend, so you only need the commands above when you work on the frontend itself.

## Pages

| Page | What it shows |
|------|---------------|
| Dashboard | Stat cards and the live event feed |
| Products | Product catalogue with role-guarded actions |
| Orders | The order lifecycle: every order for ADMIN, a customer's own for CUSTOMER |
| Customers | Registration, data export and the right to be forgotten: every customer for ADMIN, a customer's own record for CUSTOMER |
| Fulfillment | The order-fulfillment sagas: every saga for ADMIN, the sagas of their own orders for CUSTOMER |
| Events | The live feed for every role, and the event history (ADMIN only) |
| Audit | The command audit log (ADMIN only) |
| Admin | Circuit breaker, dead letters, outbox, payment-failure toggle |

## Roles

There is no login. The role switcher picks `GUEST`, `CUSTOMER` or `ADMIN`, and every request
carries the choice in the `X-User-Role` and `X-User-Id` headers (`src/lib/api.ts`). The backend
trusts those headers because the demo stands in for a gateway that would authenticate the caller
and set them; do not copy this into a real deployment (tutorial chapter 9).

What each role sees follows what the backend serves it:

| Role | Identity sent | Orders, Customers and Fulfillment | Audit, Admin, event history |
|------|---------------|-----------------------------------|------------------------------|
| `ADMIN` | `admin-1` | every order, every customer and every saga | everything |
| `CUSTOMER` | `cust-alice`, or `cust-bob` from the identity selector | their own orders, the sagas of those orders and their own record, which they may edit, export and erase | "Requires ADMIN role" |
| `GUEST` | none | "Requires CUSTOMER or ADMIN role" | "Requires ADMIN role" |

A page in a "Requires … role" state sends no request: the backend would answer `403`. The product
catalogue and the live event feed are open to every role. The feed is opened without identity
headers, as an `EventSource` cannot send any, so its frames name no stream: each one holds the
event type, the aggregate type and the time, and the page reads what changed through the guarded
endpoints. The identity selector offers the two
seeded customers and needs no customer list from the backend.

## Where things are

| Path | Contents |
|------|----------|
| `src/app/` | One directory per page |
| `src/lib/api.ts` | Every HTTP call to the backend, in the shape the backend's controllers read |
| `src/lib/ids.ts` | The ids the page picks for the orders, customers and products it creates |
| `src/lib/queries.ts`, `src/lib/mutations.ts` | TanStack Query hooks over those calls |
| `src/lib/read-model-sync.ts` | Reading a list again when the read models have caught up with a change |
| `src/providers/sse-provider.tsx` | The live feed connection and the query invalidation it drives |
| `src/providers/role-provider.tsx`, `src/lib/permissions.ts` | The selected role and what it may do |
| `src/components/` | Domain components and the UI primitives they are built from |
| `tests/e2e/` | Playwright tests |

## Writing to the backend

A write action sends the request the backend's controller reads, field for field
(`src/lib/api.ts`). Five things follow from the backend's contracts:

- **The page picks the id.** `POST /api/orders`, `POST /api/customers` and `POST /api/products`
  take the id from the request body, so the page generates one in the form of the seed data:
  `order-`, `cust-` or `prod-` and eight hex digits (`src/lib/ids.ts`). The toast of a successful
  action shows it.
- **An order line names a product and a quantity.** The backend prices each line from the product
  catalogue, and takes one line per product: the Place Order dialog merges the lines of one
  product, their quantities summed, before it sends the order.
- **Nobody confirms an order by hand**: the fulfillment saga confirms it once its payment is
  captured and its stock reserved. Each order row shows its payment status beside the order's.
- **Cancel is offered for a confirmed order too**, and says what it does: the order becomes
  `CANCELLED`, its stock is released and its payment voided, or refunded in full once captured.
- **A command is answered before the lists show it.** The backend answers `200` with an empty
  body once the events are stored; the read models follow by up to the projections' polling
  interval (five seconds and up to one of jitter by default). The page therefore reads a list
  again at once and 2, 7, 15 and 30 seconds after the last change (`src/lib/read-model-sync.ts`),
  so a placed order appears in the table by itself, a few seconds after its toast, and a read
  model that is late still shows up. A burst of feed frames is answered by one read per list.

An action is offered to the roles the backend accepts it from (`src/lib/permissions.ts`). A
request the backend refuses is shown in a toast with the backend's reason: "Not allowed for the
selected role (403): …" or "The backend refused the request (400): …".

Receive Shipment on the Products page adds to the inventory the fulfillment saga reserves from.
The inventory is its own aggregate: the Stock column of the catalogue is the product's own figure
(Adjust Stock changes it) and does not move with a shipment. An order for a product created on
the page is fulfilled once a shipment for it is received; without one the saga cancels the order.

## Checks

```bash
npm run lint
npm run build
npm test             # Playwright; install the browser once with: npx playwright install chromium
npm run test:live    # the same suite against a running backend, which it requires
```

The Playwright tests start the dev server themselves, or use the one already running on
`http://localhost:3000`.

- `npm test` needs no backend. Most specs mock it; `write-contracts.spec.ts` drives every write
  action and compares the request the page sends with the backend's contract. The specs that
  need a backend skip when none answers on `http://localhost:8080` (override with `BACKEND_URL`).
- `npm run test:live` runs the same suite with `E2E_REQUIRE_BACKEND=1`: a backend that does not
  answer fails it instead of skipping. Start the stack first (`docker compose up -d --wait` in the
  repository root, or PostgreSQL, RabbitMQ and `./gradlew :spring-app:bootRun`).
  `writes.live.spec.ts` then performs each write through the pages against the backend — register
  a customer, place and cancel an order, ship and deliver, create a product and change its price
  and stock, receive a shipment, the payment-failure switch, edit, export and forget — and asserts
  the result through the pages. It brings what it consumes: a customer it registers and a product
  it creates and stocks through the API before the first test, so it changes no seed data and
  passes run after run on the same database. Each run leaves a forgotten customer, two products
  and four orders with their events behind.

CI runs both: the suite without a backend in its own job, and the suite against the Docker
Compose stack of the Quick Start (`E2E_REUSE_SERVER=1` points Playwright at the frontend
container instead of a dev server).
