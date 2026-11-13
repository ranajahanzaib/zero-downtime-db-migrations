ALTER TABLE customers ADD COLUMN IF NOT EXISTS contact_email TEXT;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS contact_phone TEXT;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS contact_timezone TEXT;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS contact_migrated_at TIMESTAMPTZ;

CREATE OR REPLACE FUNCTION bridge_customer_contact() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
 old_email TEXT; old_phone TEXT; old_timezone TEXT;
 json_changed BOOLEAN; relational_changed BOOLEAN;
BEGIN
 IF TG_OP = 'INSERT' THEN
   IF jsonb_typeof(NEW.contact_details) <> 'object' THEN
     RAISE EXCEPTION 'contact_details must be a JSON object' USING ERRCODE='23514';
   END IF;
   IF (NEW.contact_details->'email' IS NOT NULL AND jsonb_typeof(NEW.contact_details->'email') NOT IN ('string','null')) OR
      (NEW.contact_details->'phone' IS NOT NULL AND jsonb_typeof(NEW.contact_details->'phone') NOT IN ('string','null')) OR
      (NEW.contact_details->'timezone' IS NOT NULL AND jsonb_typeof(NEW.contact_details->'timezone') NOT IN ('string','null')) THEN
     RAISE EXCEPTION 'contact attributes must be strings or JSON null' USING ERRCODE='23514';
   END IF;
   IF NEW.contact_email IS NOT NULL AND NULLIF(NEW.contact_details->>'email','') IS NOT NULL AND NEW.contact_email IS DISTINCT FROM NULLIF(NEW.contact_details->>'email','') THEN
     RAISE EXCEPTION 'conflicting contact representations' USING ERRCODE='23514';
   END IF;
   IF NEW.contact_phone IS NOT NULL AND NULLIF(NEW.contact_details->>'phone','') IS NOT NULL AND NEW.contact_phone IS DISTINCT FROM NULLIF(NEW.contact_details->>'phone','') THEN
     RAISE EXCEPTION 'conflicting contact representations' USING ERRCODE='23514';
   END IF;
   IF NEW.contact_timezone IS NOT NULL AND NULLIF(NEW.contact_details->>'timezone','') IS NOT NULL AND NEW.contact_timezone IS DISTINCT FROM NULLIF(NEW.contact_details->>'timezone','') THEN
     RAISE EXCEPTION 'conflicting contact representations' USING ERRCODE='23514';
   END IF;
   IF NEW.contact_email IS NULL THEN NEW.contact_email := NULLIF(NEW.contact_details->>'email',''); END IF;
   IF NEW.contact_phone IS NULL THEN NEW.contact_phone := NULLIF(NEW.contact_details->>'phone',''); END IF;
   IF NEW.contact_timezone IS NULL THEN NEW.contact_timezone := NULLIF(NEW.contact_details->>'timezone',''); END IF;
   IF NEW.contact_email IS NOT NULL THEN
     NEW.contact_details := jsonb_set(NEW.contact_details,'{email}',to_jsonb(NEW.contact_email),true);
   END IF;
   NEW.contact_details := CASE WHEN NEW.contact_phone IS NULL THEN NEW.contact_details - 'phone' ELSE jsonb_set(NEW.contact_details,'{phone}',to_jsonb(NEW.contact_phone),true) END;
   NEW.contact_details := CASE WHEN NEW.contact_timezone IS NULL THEN NEW.contact_details - 'timezone' ELSE jsonb_set(NEW.contact_details,'{timezone}',to_jsonb(NEW.contact_timezone),true) END;
   IF NEW.contact_email IS NOT NULL AND btrim(NEW.contact_email) <> '' AND jsonb_typeof(NEW.contact_details->'email')='string' AND
      (NEW.contact_details->'phone' IS NULL OR jsonb_typeof(NEW.contact_details->'phone') IN ('string','null')) AND
      (NEW.contact_details->'timezone' IS NULL OR jsonb_typeof(NEW.contact_details->'timezone') IN ('string','null')) THEN
     NEW.contact_migrated_at := COALESCE(NEW.contact_migrated_at,now());
   END IF;
   RETURN NEW;
 END IF;
 old_email := NULLIF(OLD.contact_details->>'email','');
 old_phone := NULLIF(OLD.contact_details->>'phone','');
 old_timezone := NULLIF(OLD.contact_details->>'timezone','');
 json_changed := (NEW.contact_details->'email' IS DISTINCT FROM OLD.contact_details->'email') OR
   (NEW.contact_details->'phone' IS DISTINCT FROM OLD.contact_details->'phone') OR
   (NEW.contact_details->'timezone' IS DISTINCT FROM OLD.contact_details->'timezone');
 relational_changed := NEW.contact_email IS DISTINCT FROM OLD.contact_email OR NEW.contact_phone IS DISTINCT FROM OLD.contact_phone OR NEW.contact_timezone IS DISTINCT FROM OLD.contact_timezone;
 IF json_changed AND relational_changed AND
   (NULLIF(NEW.contact_details->>'email','') IS DISTINCT FROM NEW.contact_email OR NULLIF(NEW.contact_details->>'phone','') IS DISTINCT FROM NEW.contact_phone OR NULLIF(NEW.contact_details->>'timezone','') IS DISTINCT FROM NEW.contact_timezone) THEN
   RAISE EXCEPTION 'conflicting contact representations' USING ERRCODE='23514';
 END IF;
 IF json_changed AND NOT relational_changed THEN
   NEW.contact_email := NULLIF(NEW.contact_details->>'email','');
   NEW.contact_phone := NULLIF(NEW.contact_details->>'phone','');
   NEW.contact_timezone := NULLIF(NEW.contact_details->>'timezone','');
 ELSIF relational_changed AND NOT json_changed THEN
   IF NEW.contact_email IS NULL THEN RAISE EXCEPTION 'contact email is required' USING ERRCODE='23514'; END IF;
   NEW.contact_details := CASE WHEN NEW.contact_phone IS NULL THEN NEW.contact_details - 'phone' ELSE jsonb_set(NEW.contact_details,'{phone}',to_jsonb(NEW.contact_phone),true) END;
   NEW.contact_details := CASE WHEN NEW.contact_timezone IS NULL THEN NEW.contact_details - 'timezone' ELSE jsonb_set(NEW.contact_details,'{timezone}',to_jsonb(NEW.contact_timezone),true) END;
   NEW.contact_details := jsonb_set(NEW.contact_details,'{email}',to_jsonb(NEW.contact_email),true);
 END IF;
 IF (NEW.contact_details->'email' IS NOT NULL AND jsonb_typeof(NEW.contact_details->'email') NOT IN ('string','null')) OR
    (NEW.contact_details->'phone' IS NOT NULL AND jsonb_typeof(NEW.contact_details->'phone') NOT IN ('string','null')) OR
    (NEW.contact_details->'timezone' IS NOT NULL AND jsonb_typeof(NEW.contact_details->'timezone') NOT IN ('string','null')) THEN
   RAISE EXCEPTION 'contact attributes must be strings or JSON null' USING ERRCODE='23514';
 END IF;
 IF NEW.contact_email IS NOT NULL AND btrim(NEW.contact_email) <> '' AND NEW.contact_email IS NOT DISTINCT FROM NULLIF(NEW.contact_details->>'email','') AND
    jsonb_typeof(NEW.contact_details->'email')='string' AND
    (NEW.contact_details->'phone' IS NULL OR jsonb_typeof(NEW.contact_details->'phone') IN ('string','null')) AND
    (NEW.contact_details->'timezone' IS NULL OR jsonb_typeof(NEW.contact_details->'timezone') IN ('string','null')) THEN
   NEW.contact_migrated_at := COALESCE(NEW.contact_migrated_at,now());
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS customers_contact_bridge ON customers;
CREATE TRIGGER customers_contact_bridge BEFORE INSERT OR UPDATE ON customers FOR EACH ROW EXECUTE FUNCTION bridge_customer_contact();
