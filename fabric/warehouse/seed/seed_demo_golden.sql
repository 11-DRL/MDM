-- Demo seed data for gold.dim_location + gold.dim_legal_entity
-- Compatible with apply-warehouse-ddl.js (semicolons, no GO)

-- ═══ dim_location — 5 demo restaurants ═══
INSERT INTO gold.dim_location (location_sk, location_hk, valid_from, valid_to, is_current, name, country, city, zip_code, address, phone, latitude, longitude, website_url, timezone, currency_code, avg_rating, review_count, cost_center, region, name_source, country_source, city_source, created_at, updated_at)
VALUES (1, 'a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2', '2025-01-01', '9999-12-31', 1, 'L''Osteria Warszawa Zlote Tarasy', 'PL', 'Warszawa', '00-222', 'ul. Zlota 59', '+48221234567', 52.2297, 21.0122, 'https://losteria.pl/zlote-tarasy', 'Europe/Warsaw', 'PLN', 4.5, 328, 'CC-WAW-01', 'CEE', 'gopos', 'gopos', 'gopos', GETUTCDATE(), GETUTCDATE());

INSERT INTO gold.dim_location (location_sk, location_hk, valid_from, valid_to, is_current, name, country, city, zip_code, address, phone, latitude, longitude, website_url, timezone, currency_code, avg_rating, review_count, cost_center, region, name_source, country_source, city_source, created_at, updated_at)
VALUES (2, 'b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3', '2025-01-01', '9999-12-31', 1, 'L''Osteria Berlin Friedrichstrasse', 'DE', 'Berlin', '10117', 'Friedrichstr. 67', '+493012345678', 52.5200, 13.3880, 'https://losteria.de/berlin-friedrichstr', 'Europe/Berlin', 'EUR', 4.3, 512, 'CC-BER-01', 'DACH', 'lightspeed', 'lightspeed', 'lightspeed', GETUTCDATE(), GETUTCDATE());

INSERT INTO gold.dim_location (location_sk, location_hk, valid_from, valid_to, is_current, name, country, city, zip_code, address, phone, latitude, longitude, website_url, timezone, currency_code, avg_rating, review_count, cost_center, region, name_source, country_source, city_source, created_at, updated_at)
VALUES (3, 'c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4', '2025-01-01', '9999-12-31', 1, 'L''Osteria Munchen Leopoldstrasse', 'DE', 'Munchen', '80802', 'Leopoldstr. 28', '+498912345678', 48.1617, 11.5842, 'https://losteria.de/muenchen-leopoldstr', 'Europe/Berlin', 'EUR', 4.6, 891, 'CC-MUC-01', 'DACH', 'lightspeed', 'yext', 'lightspeed', GETUTCDATE(), GETUTCDATE());

INSERT INTO gold.dim_location (location_sk, location_hk, valid_from, valid_to, is_current, name, country, city, zip_code, address, phone, latitude, longitude, website_url, timezone, currency_code, avg_rating, review_count, cost_center, region, name_source, country_source, city_source, created_at, updated_at)
VALUES (4, 'd4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5', '2025-01-01', '9999-12-31', 1, 'L''Osteria Wien Mariahilfer', 'AT', 'Wien', '1060', 'Mariahilfer Str. 45', '+4311234567', 48.1953, 16.3486, 'https://losteria.at/wien-mariahilfer', 'Europe/Vienna', 'EUR', 4.4, 276, 'CC-WIE-01', 'DACH', 'mcwin', 'mcwin', 'mcwin', GETUTCDATE(), GETUTCDATE());

INSERT INTO gold.dim_location (location_sk, location_hk, valid_from, valid_to, is_current, name, country, city, zip_code, address, phone, latitude, longitude, website_url, timezone, currency_code, avg_rating, review_count, cost_center, region, name_source, country_source, city_source, created_at, updated_at)
VALUES (5, 'e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6', '2025-01-01', '9999-12-31', 1, 'L''Osteria Praha Wenceslas', 'CZ', 'Praha', '11000', 'Vaclavske nam. 12', '+420212345678', 50.0813, 14.4267, 'https://losteria.cz/praha-wenceslas', 'Europe/Prague', 'CZK', 4.2, 194, 'CC-PRG-01', 'CEE', 'gopos', 'gopos', 'gopos', GETUTCDATE(), GETUTCDATE());

-- ═══ dim_legal_entity — 4 demo companies ═══
INSERT INTO gold.dim_legal_entity (legal_entity_hk, legal_entity_code, name, tax_id, country, currency_code, parent_entity_code, consolidation_method, ownership_pct, valid_from, valid_to, is_active, is_current, effective_from, effective_to, source_system, created_at)
VALUES ('le01aabbccdd01aabbccdd01aabbccdd01aabbccdd01aabbccdd01aabbccdd01aa', 'LE-HQ', 'L''Osteria SE', 'DE123456789', 'DE', 'EUR', NULL, 'full', 100.00, '2020-01-01', NULL, 1, 1, '2025-01-01', NULL, 'manual', GETUTCDATE());

INSERT INTO gold.dim_legal_entity (legal_entity_hk, legal_entity_code, name, tax_id, country, currency_code, parent_entity_code, consolidation_method, ownership_pct, valid_from, valid_to, is_active, is_current, effective_from, effective_to, source_system, created_at)
VALUES ('le02aabbccdd02aabbccdd02aabbccdd02aabbccdd02aabbccdd02aabbccdd02aa', 'LE-PL', 'L''Osteria Polska Sp. z o.o.', 'PL5261234567', 'PL', 'PLN', 'LE-HQ', 'full', 100.00, '2022-03-01', NULL, 1, 1, '2025-01-01', NULL, 'manual', GETUTCDATE());

INSERT INTO gold.dim_legal_entity (legal_entity_hk, legal_entity_code, name, tax_id, country, currency_code, parent_entity_code, consolidation_method, ownership_pct, valid_from, valid_to, is_active, is_current, effective_from, effective_to, source_system, created_at)
VALUES ('le03aabbccdd03aabbccdd03aabbccdd03aabbccdd03aabbccdd03aabbccdd03aa', 'LE-AT', 'L''Osteria Oesterreich GmbH', 'ATU12345678', 'AT', 'EUR', 'LE-HQ', 'full', 100.00, '2021-06-15', NULL, 1, 1, '2025-01-01', NULL, 'manual', GETUTCDATE());

INSERT INTO gold.dim_legal_entity (legal_entity_hk, legal_entity_code, name, tax_id, country, currency_code, parent_entity_code, consolidation_method, ownership_pct, valid_from, valid_to, is_active, is_current, effective_from, effective_to, source_system, created_at)
VALUES ('le04aabbccdd04aabbccdd04aabbccdd04aabbccdd04aabbccdd04aabbccdd04aa', 'LE-CZ', 'L''Osteria Czech s.r.o.', 'CZ12345678', 'CZ', 'CZK', 'LE-HQ', 'equity', 51.00, '2023-09-01', NULL, 1, 1, '2025-01-01', NULL, 'manual', GETUTCDATE());
