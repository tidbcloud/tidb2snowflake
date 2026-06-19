-- Derived from tidb-fivetran-connector/e2e-test/test.sql.
-- This fixture keeps only TiDB types and DML shapes currently supported by
-- tidb2snowflake's TiDB -> Snowflake type mapper.

DROP DATABASE IF EXISTS t2sf_matrix;
CREATE DATABASE t2sf_matrix;
USE t2sf_matrix;
SET time_zone = '+00:00';

CREATE TABLE supported_type_matrix (
  id BIGINT NOT NULL,

  c_boolean BOOLEAN NULL,
  c_tinyint TINYINT NULL,
  c_tinyint_unsigned TINYINT UNSIGNED NULL,
  c_smallint SMALLINT NULL,
  c_smallint_unsigned SMALLINT UNSIGNED NULL,
  c_mediumint MEDIUMINT NULL,
  c_mediumint_unsigned MEDIUMINT UNSIGNED NULL,
  c_int INT NULL,
  c_int_unsigned INT UNSIGNED NULL,
  c_bigint BIGINT NULL,
  c_bigint_unsigned BIGINT UNSIGNED NULL,

  c_decimal_default DECIMAL NULL,
  c_decimal_10 DECIMAL(10) NULL,
  c_decimal_20_6 DECIMAL(20, 6) NULL,
  c_decimal_38_30 DECIMAL(38, 30) NULL,
  c_numeric_30_10 NUMERIC(30, 10) NULL,
  c_float FLOAT NULL,
  c_float_24 FLOAT(24) NULL,
  c_float_25 FLOAT(25) NULL,
  c_double DOUBLE NULL,

  c_date DATE NULL,
  c_datetime DATETIME(6) NULL,
  c_timestamp TIMESTAMP(6) NULL DEFAULT NULL,
  c_time TIME(6) NULL,
  c_year YEAR NULL,

  c_char CHAR(16) NULL,
  c_varchar VARCHAR(255) NULL,
  c_tinytext TINYTEXT NULL,
  c_text TEXT NULL,
  c_mediumtext MEDIUMTEXT NULL,
  c_longtext LONGTEXT NULL,
  c_enum ENUM('small', 'medium', 'large') NULL,

  c_binary BINARY(16) NULL,
  c_varbinary VARBINARY(255) NULL,
  c_tinyblob TINYBLOB NULL,
  c_blob BLOB NULL,
  c_vector VECTOR(3) NULL,

  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

INSERT INTO supported_type_matrix VALUES
  (
    1,
    TRUE,
    -8,
    250,
    -12345,
    60000,
    -800000,
    16000000,
    -123456789,
    4000000000,
    -900000000000000000,
    18446744073709551615,
    1234567890,
    1234567890,
    1234567890.123456,
    12345678.123456789012345678901234567890,
    12345678901234567890.1234567890,
    3.14,
    3.14,
    3.141592653589793,
    2.718281828459045,
    '2026-06-04',
    '2026-06-04 12:34:56.123456',
    '2026-06-04 12:34:56.123456',
    '23:59:59.123456',
    2026,
    'char-value',
    'varchar parameter value',
    'tiny text value',
    'text value',
    'medium text value',
    'long text value',
    'medium',
    X'00112233445566778899AABBCCDDEEFF',
    X'FFFE000102',
    X'01',
    X'0203',
    '[0.1,0.2,0.3]'
  ),
  (
    2,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    '',
    '',
    '',
    '',
    '',
    '',
    NULL,
    X'00000000000000000000000000000000',
    X'',
    NULL,
    NULL,
    NULL
  ),
  (
    3,
    FALSE,
    -128,
    0,
    -32768,
    0,
    -8388608,
    0,
    -2147483648,
    0,
    -9223372036854775807 - 1,
    0,
    -9999999999,
    -9999999999,
    -99999999999999.999999,
    CAST(CONCAT('-', REPEAT('9', 8), '.', REPEAT('9', 30)) AS DECIMAL(38, 30)),
    -99999999999999999999.9999999999,
    -3.402823466E38,
    -3.402823466E38,
    -1.7976931348623157E308,
    -1.7976931348623157E308,
    '1000-01-01',
    '1000-01-01 00:00:00.000001',
    '1970-01-01 00:00:02.000001',
    '00:00:01.000001',
    1970,
    '                ',
    'row to update',
    'tiny text two',
    'text before update',
    'medium text two',
    'long text two',
    'small',
    X'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',
    X'CAFEBABE',
    X'11',
    X'1213',
    '[1,2,3]'
  ),
  (
    4,
    TRUE,
    127,
    255,
    32767,
    65535,
    8388607,
    16777215,
    2147483647,
    4294967295,
    9223372036854775807,
    18446744073709551615,
    9999999999,
    9999999999,
    99999999999999.999999,
    CAST(CONCAT(REPEAT('9', 8), '.', REPEAT('9', 30)) AS DECIMAL(38, 30)),
    99999999999999999999.9999999999,
    3.402823466E38,
    3.402823466E38,
    1.7976931348623157E308,
    1.7976931348623157E308,
    '9999-12-31',
    '9999-12-31 23:59:59.999999',
    '2038-01-19 03:14:07.999999',
    '838:59:58.999999',
    2155,
    '1234567890abcdef',
    REPEAT('x', 255),
    REPEAT('t', 255),
    REPEAT('x', 1024),
    REPEAT('m', 1024),
    REPEAT('l', 1024),
    'large',
    X'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',
    UNHEX(REPEAT('FF', 255)),
    UNHEX(REPEAT('FF', 255)),
    UNHEX(REPEAT('FF', 1024)),
    '[-0.5,0,0.5]'
  );

INSERT INTO supported_type_matrix (id, c_varchar, c_text) VALUES
  (10, 'row to update', 'text before update'),
  (11, 'row to primary-key update', 'text before primary-key update'),
  (12, 'row to delete', 'text before delete'),
  (13, 'row to delete and reinsert', 'text before delete and reinsert');

UPDATE supported_type_matrix
  SET c_varchar = '',
      c_text = 'text after update to empty varchar',
      c_longtext = REPEAT('updated long text ', 16)
  WHERE id = 10;

UPDATE supported_type_matrix
  SET id = 110,
      c_varchar = 'primary key updated'
  WHERE id = 11;

DELETE FROM supported_type_matrix WHERE id = 12;
DELETE FROM supported_type_matrix WHERE id = 13;
INSERT INTO supported_type_matrix (id, c_varchar, c_text) VALUES
  (13, 'reinserted after delete', 'text after reinsert');

CREATE TABLE supported_composite_pk_dml (
  tenant_id BIGINT NOT NULL,
  item_id BIGINT NOT NULL,
  payload VARCHAR(128) NULL,
  updated_at TIMESTAMP(6) NULL DEFAULT NULL,
  PRIMARY KEY (tenant_id, item_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

INSERT INTO supported_composite_pk_dml VALUES
  (1, 1, 'composite row to update', '2026-06-04 13:00:00.000001'),
  (1, 2, 'composite row to key update', '2026-06-04 13:00:00.000002'),
  (2, 1, 'composite row to delete', '2026-06-04 13:00:00.000003'),
  (2, 2, 'composite row to delete and reinsert', '2026-06-04 13:00:00.000004');

UPDATE supported_composite_pk_dml
  SET payload = 'composite payload updated',
      updated_at = '2026-06-04 14:00:00.000001'
  WHERE tenant_id = 1 AND item_id = 1;

UPDATE supported_composite_pk_dml
  SET item_id = 20,
      payload = 'composite key part updated'
  WHERE tenant_id = 1 AND item_id = 2;

DELETE FROM supported_composite_pk_dml WHERE tenant_id = 2 AND item_id = 1;
DELETE FROM supported_composite_pk_dml WHERE tenant_id = 2 AND item_id = 2;
INSERT INTO supported_composite_pk_dml VALUES
  (2, 2, 'composite row reinserted', '2026-06-04 15:00:00.000004');
