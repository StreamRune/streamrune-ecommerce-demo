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

export interface OrderView {
  orderId: string;
  customerId: string;
  lines: OrderLineView[];
  total: Money;
  status: OrderStatus;
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
  sagaId: { value: string };
  orderId: string;
  customerId: string;
  paymentId: string;
  orderTotal: Money;
  fulfillmentStatus: OrderFulfillmentStatus;
}

/**
 * One frame of the live feed (GET /api/events/sse): which event, of which
 * stream, and when. The feed is open to every role, so it carries no payload.
 */
export interface LiveEvent {
  globalOffset: number;
  streamId: string;
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  version: number;
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

export interface DeadLetterEntry {
  id: string;
  commandType: string;
  error: string;
  timestamp: string;
}

export interface OutboxEntry {
  id: string;
  eventType: string;
  createdAt: string;
}

export type Role = "ADMIN" | "CUSTOMER" | "GUEST";

export interface RoleState {
  role: Role;
  userId: string | null;
}
