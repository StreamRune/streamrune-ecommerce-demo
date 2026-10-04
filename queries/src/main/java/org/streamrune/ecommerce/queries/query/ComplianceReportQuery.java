package org.streamrune.ecommerce.queries.query;

import java.time.Instant;
import org.streamrune.core.audit.Auditable;

@Auditable
public record ComplianceReportQuery(Instant from, Instant to)
    implements org.streamrune.core.Query<org.streamrune.core.audit.ComplianceReport> {}
