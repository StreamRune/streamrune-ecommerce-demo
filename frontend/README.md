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
| Orders | The order lifecycle |
| Customers | Registration, data export and the right to be forgotten |
| Fulfillment | The order-fulfillment saga, step by step |
| Events | The event history and the live feed |
| Audit | The command audit log (ADMIN only) |
| Admin | Circuit breaker, dead letters, outbox, payment-failure toggle |

## Roles

There is no login. The role switcher picks `GUEST`, `CUSTOMER` or `ADMIN`, and every request
carries the choice in the `X-User-Role` and `X-User-Id` headers (`src/lib/api.ts`). The backend
trusts those headers because the demo stands in for a gateway that would authenticate the caller
and set them; do not copy this into a real deployment (tutorial chapter 9).

## Where things are

| Path | Contents |
|------|----------|
| `src/app/` | One directory per page |
| `src/lib/api.ts` | Every HTTP call to the backend |
| `src/lib/queries.ts`, `src/lib/mutations.ts` | TanStack Query hooks over those calls |
| `src/providers/sse-provider.tsx` | The live feed connection and the query invalidation it drives |
| `src/providers/role-provider.tsx`, `src/lib/permissions.ts` | The selected role and what it may do |
| `src/components/` | Domain components and the UI primitives they are built from |
| `tests/e2e/` | Playwright tests |

## Checks

```bash
npm run lint
npm run build
npm test        # Playwright; install the browser once with: npx playwright install chromium
```

The Playwright tests start the dev server themselves. Tests that need the backend skip when none
answers on `http://localhost:8080` (override with `BACKEND_URL`).
