# Type Coverage Testing Design

Comprehensive testing strategy for database type conversions across all supported backends.

## Background

After PR #64 (NUMERIC column casting fix), we identified gaps in type conversion testing. This design ensures every database type is thoroughly tested for correctness, coverage, and regression prevention.

## Test Architecture

Three-layer approach:

1. **Unit Tests** - Exhaustive tests of `*_type_to_arrow()` functions (no database needed)
2. **Integration Tests** - Data round-trip tests using testcontainers
3. **Shared Fixtures** - TOML files with test values reused across backends

### Directory Structure

```
tests/type_coverage/
├── fixtures/
│   ├── integers.toml
│   ├── floats.toml
│   ├── decimals.toml
│   ├── strings.toml
│   ├── binary.toml
│   ├── timestamps.toml
│   ├── dates.toml
│   ├── times.toml
│   ├── booleans.toml
│   ├── json.toml
│   └── uuids.toml
├── mod.rs              # Test harness
├── postgres_types.rs
├── mysql_types.rs
└── duckdb_types.rs
```

## Backends in Scope

| Backend | Test Method | Status |
|---------|-------------|--------|
| PostgreSQL | testcontainers | In scope |
| MySQL | testcontainers | In scope |
| DuckDB | file-based | In scope |
| Snowflake | — | Future (needs credentials) |
| Iceberg | — | Future (needs catalog) |

## Fixture Format

Fixtures define test values by logical category. They serve as the **compatibility contract** of the system—documenting the guarantees the engine makes about type behavior.

### Constraint-Based Value Selection

Instead of filtering with code, fixtures declare their own constraints. Backends select compatible values without understanding semantic rules:

```toml
# fixtures/decimals.toml

[category]
name = "decimals"
description = "Fixed-precision decimal/numeric types"
semantic_type = "Decimal"

[[values]]
name = "simple"
sql = "123.45"
expected = "123.45"
requires = { max_precision = 38, max_scale = 10 }

[[values]]
name = "high_precision"
sql = "12345678901234567890.12345678901234567890"
expected = "12345678901234567890.12345678901234567890"
requires = { max_precision = 40, max_scale = 20 }
note = "Tests precision beyond f64"

[[values]]
name = "null"
sql = "NULL"
expected_null = true
requires = { nullable = true }
```

### Expected Failures

Some conversions should fail by design. Fixtures declare expected errors to prevent regressions where panics or silent truncation appear:

```toml
[[values]]
name = "overflow"
sql = "1e1000"
expect_error = "Overflow"
note = "Should reject, not silently truncate"
```

### Comparison Modes

Different types need different comparison strategies:

```toml
# fixtures/floats.toml

[[values]]
name = "pi"
sql = "3.14159265358979"
expected = "3.14159265358979"
comparison = "approx"
epsilon = 1e-10

[[values]]
name = "nan"
sql = "'NaN'"
expected = "NaN"
comparison = "special"
note = "NaN == NaN for test purposes"

[[values]]
name = "pos_infinity"
sql = "'Infinity'"
expected = "Infinity"
comparison = "special"
```

### Backends Reference Fixtures

```rust
TypeTestCase {
    db_type: "NUMERIC(10,2)",
    semantic_type: SemanticType::Decimal,
    expected_arrow_type: DataType::Utf8,
    values: fixtures.select_compatible(Constraints {
        max_precision: 10,
        max_scale: 2,
        nullable: false,
    }),
}
```

Backend-specific types (e.g., PostgreSQL CIDR, MySQL BIGINT UNSIGNED) define values inline.

## Test Harness

### Semantic vs Arrow Types

Tests separate what the data means from how it's represented. This future-proofs tests when representations change (e.g., moving decimals from Utf8 to Decimal128):

```rust
struct TypeTestCase {
    db_type: String,
    semantic_type: SemanticType,      // What the data means
    expected_arrow_type: DataType,    // Current representation
    values: Vec<TestValue>,
    shape: TestShape,
}

enum SemanticType {
    Boolean,
    Integer { signed: bool, bits: u8 },
    Float { bits: u8 },
    Decimal,
    String,
    Binary,
    Timestamp { with_tz: bool },
    Date,
    Time,
    Interval,
    Json,
    Uuid,
    Array { element: Box<SemanticType> },
    Struct,
    // Backend-specific
    Network,
    Geometric,
    Range,
}
```

### Test Shapes

Non-scalar types need different handling:

```rust
enum TestShape {
    Scalar,                    // Simple insert-select
    Array,                     // Arrow List type
    Struct,                    // Arrow Struct type
    Composite { fields: .. },  // Multi-field verification
}
```

### Expected Output

```rust
enum ExpectedOutput {
    String(String),
    Float { value: f64, epsilon: f64 },
    Decimal128 { value: i128, precision: u8, scale: i8 },
    Bool(bool),
    Null,
    JsonValue(serde_json::Value),  // Semantic JSON comparison
    Error(String),                  // Expected failure
}

enum ComparisonMode {
    Exact,
    Approx { epsilon: f64 },
    Special,  // NaN, Infinity handling
    Json,     // Parse and compare semantically
}
```

### Harness Function

```rust
async fn run_type_tests(
    connection: &DatabaseConnection,
    test_cases: Vec<TypeTestCase>,
) -> Result<TestReport>;
```

Each test:
1. Creates table with the target column type
2. Inserts fixture values
3. Fetches through the engine
4. Verifies Arrow schema and values match (using appropriate comparison mode)

## Type Coverage

### PostgreSQL

| Category | Types |
|----------|-------|
| Boolean | `boolean`, `bool` |
| Integers | `smallint`, `integer`, `bigint` |
| Floats | `real`, `double precision` |
| Decimal | `numeric`, `decimal`, `money` |
| Strings | `char`, `varchar`, `text` |
| Binary | `bytea` |
| Date/Time | `date`, `time`, `timestamp`, `timestamptz`, `interval` |
| JSON | `json`, `jsonb` |
| UUID | `uuid` |
| Network | `inet`, `cidr`, `macaddr` |
| Geometric | `point`, `line`, `box`, `path`, `polygon`, `circle` |
| Arrays | `integer[]`, `text[]`, etc. |
| Range | `int4range`, `tsrange`, `daterange` |

### MySQL

| Category | Types |
|----------|-------|
| Boolean | `tinyint(1)`, `bool` |
| Signed Int | `tinyint`, `smallint`, `mediumint`, `int`, `bigint` |
| Unsigned Int | `tinyint unsigned` through `bigint unsigned` |
| Floats | `float`, `double` |
| Decimal | `decimal`, `numeric` |
| Strings | `char`, `varchar`, `text` variants |
| Binary | `binary`, `varbinary`, `blob` variants |
| Date/Time | `date`, `time`, `datetime`, `timestamp`, `year` |
| JSON | `json` |
| Enum/Set | `enum`, `set` |

### DuckDB

| Category | Types |
|----------|-------|
| Boolean | `boolean` |
| Signed Int | `tinyint` through `hugeint` |
| Unsigned Int | `utinyint` through `ubigint` |
| Floats | `float`, `double` |
| Decimal | `decimal` (native Decimal128) |
| Strings | `varchar`, `text` |
| Binary | `blob` |
| Date/Time | `date`, `time`, `timestamp`, `interval` |
| UUID | `uuid` |
| JSON | `json` |

## Test Report

Failures include detailed diagnostics:

```
FAILED: postgres_types::test_postgres_decimal_types

  Type: NUMERIC(20,10)
  Expected Arrow: Utf8
  Actual Arrow:   Utf8

  Value mismatch at index 3:
    Input SQL:  12345678901234567890.1234567890
    Expected:   "12345678901234567890.1234567890"
    Actual:     NULL

  Hint: Similar to PR #64 - NUMERIC decode without bigdecimal feature
```

Summary output:

```
Type Coverage Report: PostgreSQL

Category        Passed  Failed
---------       ------  ------
Integers        6/6     0
Decimals        3/5     2       <- FAILURES
Strings         8/8     0
...

Total: 44/46 passed, 2 failed
```

## Coverage Completeness Guard

A test that fails if a backend adds a new type without coverage:

```rust
#[test]
fn test_postgres_type_coverage_complete() {
    let documented_types = pg_documented_types();  // From PG docs or information_schema
    let tested_types = pg_tested_types();          // Collected from test cases

    let missing: Vec<_> = documented_types
        .difference(&tested_types)
        .collect();

    assert!(
        missing.is_empty(),
        "PostgreSQL types missing test coverage: {:?}",
        missing
    );
}
```

This enforces long-term discipline—new types in `*_type_to_arrow()` require corresponding tests.

## Design Notes

### Fixtures as Contract

Fixtures are not just tests—they define the compatibility guarantees the engine makes about type behavior. When in doubt about whether to add a test value, ask: "Is this a guarantee we want to make?"

### When Representations Change

If we later move decimals from `Utf8` to `Decimal128`:
1. Update `expected_arrow_type` in test cases
2. `semantic_type` stays the same
3. Fixtures stay the same
4. Only the expected output transformation changes

This separation makes representation migrations straightforward to test.
