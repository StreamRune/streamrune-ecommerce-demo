export interface Money {
  amount: number;
  currency: string;
}

export type ProductStatus = "AVAILABLE" | "LOW_STOCK" | "OUT_OF_STOCK" | "DISCONTINUED";

export interface ProductView {
  productId: string;
  name: string;
  description: string;
  category: string;
  price: Money;
  stock: number;
  status: ProductStatus;
  createdAt: string;
  updatedAt: string;
}

export type OrderStatus = "CREATED" | "CONFIRMED" | "SHIPPED" | "DELIVERED" | "CANCELLED";

export interface OrderLineView {
  productId: string;
  quantity: number;
  unitPrice: Money;
}

/** The status of an order's payment; an order's row has none until the payment is initiated. */
export type PaymentStatus = "PENDING" | "CAPTURED" | "REFUNDED" | "VOIDED" | "FAILED";

export interface OrderView {
  orderId: string;
  customerId: string;
  lines: OrderLineView[];
  total: Money;
  status: OrderStatus;
  paymentStatus: PaymentStatus | null;
  createdAt: string;
  updatedAt: string;
}

export type CustomerStatus = "ACTIVE" | "FORGOTTEN";

export interface CustomerView {
  customerId: string;
  name: string;
  email: string;
  address: string;
  phone: string;
  status: CustomerStatus;
  createdAt: string;
  updatedAt: string;
}

// Domain-specific fulfillment status (OrderFulfillmentStatus on the backend). This is what the
// saga DETAIL endpoint (`GET /api/saga/fulfillments/{id}`) returns inside `fulfillmentStatus`,
// serialized straight from the saga's domain state.
export type OrderFulfillmentStatus =
  | "AWAITING_PAYMENT"
  | "AWAITING_INVENTORY"
  | "AWAITING_CONFIRMATION"
  | "CANCELLING"
  | "COMPLETED"
  | "COMPENSATING"
  | "FAILED";

// Framework saga lifecycle status (SagaStatus on the backend). This is what the saga LIST
// endpoint (`GET /api/saga/fulfillments`) returns in `status` — it reads the raw `saga_state.status`
// column, which every saga type (not just order fulfillment) shares, so it uses the framework's
// lifecycle vocabulary rather than any domain-specific status. COMPENSATED/FAULTED are included
// for completeness (other saga types / crash-recovery paths can reach them) even though this
// saga's own `status()` derivation never returns them today.
export type SagaLifecycleStatus =
  | "STARTED"
  | "RUNNING"
  | "COMPLETED"
  | "COMPENSATING"
  | "COMPENSATED"
  | "FAILED"
  | "FAULTED";

export interface SagaListEntry {
  sagaId: string;
  sagaType: string;
  status: SagaLifecycleStatus;
  state: string;
}

export interface OrderFulfillmentState {
  sagaId: string;
  orderId: string;
  customerId: string;
  paymentId: string;
  orderTotal: Money;
  lines: OrderLineView[];
  /** The products whose stock the saga has reserved so far, once each, in the order of the lines. */
  reservedProductIds: string[];
  fulfillmentStatus: OrderFulfillmentStatus;
}

/**
 * One frame of the live feed (GET /api/events/sse): an event of this type
 * happened to an aggregate of this type, and when. The feed is open and cannot
 * ask who is calling, so a frame names no stream: no stream id, no aggregate
 * id, no payload. It is the signal to read again; what changed is read from
 * the guarded endpoints with the session's identity headers.
 */
export interface LiveEvent {
  globalOffset: number;
  aggregateType: string;
  eventType: string;
  timestamp: string;
}

/**
 * One event of the history (GET /api/events, GET /api/events/{type}/{id}),
 * with its payload as the event store reads it: decrypted. ADMIN only.
 */
export interface EventEntry {
  globalOffset: number;
  streamId: string;
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  timestamp: string;
  payload: Record<string, unknown>;
}

export interface AuditEntry {
  commandId: string;
  commandType: string;
  aggregateId: string;
  userId: string;
  occurredAt: string;
  outcome: string;
  eventCount: number;
}

/**
 * One entry of GET /api/admin/dead-letters: a command that failed on
 * infrastructure and waits for a retry. `commandId` is the id the retry route
 * takes. The entry has more fields (the stored command, trace ids); these are
 * the ones the admin page shows.
 */
export interface DeadLetterEntry {
  commandId: string;
  commandType: string;
  errorType: string;
  errorMessage: string;
  attempts: number;
  dlqAttempts: number;
  firstAttemptAt: string;
  lastAttemptAt: string | null;
}

/** What `POST /api/admin/dead-letters/{commandId}/retry` answers: the runner's outcome, and why when it refused. */
export interface DeadLetterRetryAnswer {
  outcome: string;
  message?: string;
}

/**
 * One entry of GET /api/admin/outbox: an integration event waiting for the
 * broker. `payloadType` names the event, e.g. "OrderConfirmed".
 */
export interface OutboxEntry {
  id: string;
  payloadType: string;
  status: string;
  attempts: number;
  createdAt: string;
}

/** GET /api/admin/payment-failure and the answer of both routes that flip the switch. */
export interface PaymentFailureState {
  failureInjected: boolean;
}

/** GET /api/admin/circuit-breaker: the command bus's breaker and the payment gateway's. */
export interface CircuitBreakerState {
  state: string;
  paymentGateway: string;
}

/** GET /api/admin/health. */
export interface HealthState {
  status: string;
}

/** The answer of POST /api/customers/{id}/forget. */
export interface ForgetResult {
  keyDeleted: boolean;
  fullyErased: boolean;
  failedPurgers: string[];
}

export type Role = "ADMIN" | "CUSTOMER" | "GUEST";

export interface RoleState {
  role: Role;
  userId: string | null;
}
