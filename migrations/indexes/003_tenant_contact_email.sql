CREATE INDEX CONCURRENTLY idx_customers_tenant_contact_email
ON customers (tenant_id, contact_email);
