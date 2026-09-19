-- Seed rules for the local stack. psql -h localhost -U chargemon -d chargemon -f deploy/local/sample-rules.sql
INSERT INTO rules (id, name, kind, subject_type, spec, grace_window, suppression_window, severity, channels) VALUES
('11111111-1111-1111-1111-111111111111', 'Connector faulted', 'EVENT', 'STATION',
 '{"trigger":{"actions":["StatusNotification"]},"condition":{"op":"eq","field":"event.status","value":"FAULTED"}}',
 'PT0S', 'PT10M', 'HIGH', '{email:ops@example.test}'),
('22222222-2222-2222-2222-222222222222', 'No heartbeat 2 min', 'ABSENCE', 'STATION',
 '{"expectedAction":"Heartbeat","within":"PT2M"}',
 'PT30S', 'PT15M', 'CRITICAL', '{email:oncall@example.test}'),
('33333333-3333-3333-3333-333333333333', 'Stuck preparing', 'STATE_DURATION', 'STATION',
 '{"enter":{"op":"eq","field":"event.status","value":"PREPARING"},"exit":{"op":"ne","field":"event.status","value":"PREPARING"},"maxDuration":"PT3M","scopeField":"event.connectorId"}',
 'PT0S', 'PT30M', 'MEDIUM', '{}'),
('44444444-4444-4444-4444-444444444444', 'Boot rejected', 'EVENT', 'STATION',
 '{"trigger":{"actions":["BootCompleted"]},"condition":{"op":"eq","field":"event.status","value":"REJECTED"}}',
 'PT0S', 'PT1H', 'HIGH', '{}'),
('55555555-5555-5555-5555-555555555555', 'Zero-energy sessions today', 'EVENT', 'STATION',
 '{"trigger":{"aggregate":"zeroEnergy"},"condition":{"op":"gte","field":"agg.zeroEnergy.daily","value":3}}',
 'PT0S', 'PT6H', 'LOW', '{}'),
('66666666-6666-6666-6666-666666666666', 'CSMS call errors', 'EVENT', 'STATION',
 '{"trigger":{"actions":["CallFailed"]},"condition":{"op":"in","field":"event.errorCode","values":["InternalError","SecurityError"]}}',
 'PT0S', 'PT5M', 'MEDIUM', '{}')
ON CONFLICT (id) DO NOTHING;
