ALTER TABLE customers
  ADD CONSTRAINT customers_contact_email_present
  CHECK (contact_email IS NOT NULL AND length(btrim(contact_email)) > 0) NOT VALID;

ALTER TABLE customers
  VALIDATE CONSTRAINT customers_contact_email_present;

-- PostgreSQL can use the validated check when setting the column NOT NULL.
-- SET NOT NULL still requires a brief ACCESS EXCLUSIVE lock acquisition.
ALTER TABLE customers ALTER COLUMN contact_email SET NOT NULL;
