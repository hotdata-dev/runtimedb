//! TPC-H Benchmark Tests for RuntimeDB.
//!
//! These tests measure query performance using standard TPC-H benchmark queries
//! to enable optimization comparisons.
//!
//! Usage:
//! ```bash
//! # Quick test (CI, ~10MB data)
//! cargo test tpch_benchmark -- --nocapture
//!
//! # Full benchmark (1GB data)
//! TPCH_SCALE_FACTOR=1.0 cargo test tpch_benchmark --release -- --nocapture
//!
//! # Or use ignored test for full benchmark
//! cargo test tpch_benchmark_full --release -- --ignored --nocapture
//! ```

use anyhow::Result;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use rand::RngCore;
use runtimedb::http::app_server::{AppServer, PATH_CONNECTIONS};
use runtimedb::datafetch::ParquetConfig;
use runtimedb::RuntimeEngine;
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tower::util::ServiceExt;

/// Generate a test secret key (base64-encoded 32 bytes)
fn generate_test_secret_key() -> String {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    STANDARD.encode(key)
}

/// TPC-H benchmark query definitions
struct TpchQuery {
    name: &'static str,
    sql: &'static str,
}

/// TPC-H Q1: Pricing Summary Report
/// Pattern: Aggregation with GROUP BY over lineitem
const TPCH_Q1: TpchQuery = TpchQuery {
    name: "Q1",
    sql: r#"
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity) AS avg_qty,
    AVG(l_extendedprice) AS avg_price,
    AVG(l_discount) AS avg_disc,
    COUNT(*) AS count_order
FROM
    tpch.main.lineitem
WHERE
    l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus
"#,
};

/// TPC-H Q3: Shipping Priority
/// Pattern: 3-way join (customer-orders-lineitem)
const TPCH_Q3: TpchQuery = TpchQuery {
    name: "Q3",
    sql: r#"
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    tpch.main.customer,
    tpch.main.orders,
    tpch.main.lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-03-15'
    AND l_shipdate > DATE '1995-03-15'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10
"#,
};

/// TPC-H Q5: Local Supplier Volume
/// Pattern: 6-way join (all dimension tables)
const TPCH_Q5: TpchQuery = TpchQuery {
    name: "Q5",
    sql: r#"
SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    tpch.main.customer,
    tpch.main.orders,
    tpch.main.lineitem,
    tpch.main.supplier,
    tpch.main.nation,
    tpch.main.region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= DATE '1994-01-01'
    AND o_orderdate < DATE '1995-01-01'
GROUP BY
    n_name
ORDER BY
    revenue DESC
"#,
};

/// TPC-H Q2: Minimum Cost Supplier
/// Pattern: Correlated subquery
const TPCH_Q2: TpchQuery = TpchQuery {
    name: "Q2",
    sql: r#"
SELECT
    s_acctbal, s_name, n_name, p_partkey, p_mfgr,
    s_address, s_phone, s_comment
FROM
    tpch.main.part,
    tpch.main.supplier,
    tpch.main.partsupp,
    tpch.main.nation,
    tpch.main.region
WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND p_type LIKE '%BRASS'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT MIN(ps_supplycost)
        FROM tpch.main.partsupp, tpch.main.supplier, tpch.main.nation, tpch.main.region
        WHERE
            p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE'
    )
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100
"#,
};

/// TPC-H Q4: Order Priority Checking
/// Pattern: EXISTS subquery
const TPCH_Q4: TpchQuery = TpchQuery {
    name: "Q4",
    sql: r#"
SELECT
    o_orderpriority,
    COUNT(*) AS order_count
FROM
    tpch.main.orders
WHERE
    o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-10-01'
    AND EXISTS (
        SELECT * FROM tpch.main.lineitem
        WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate
    )
GROUP BY o_orderpriority
ORDER BY o_orderpriority
"#,
};

/// TPC-H Q6: Forecasting Revenue Change
/// Pattern: Scan + filter (lineitem with predicates)
const TPCH_Q6: TpchQuery = TpchQuery {
    name: "Q6",
    sql: r#"
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM
    tpch.main.lineitem
WHERE
    l_shipdate >= DATE '1994-01-01'
    AND l_shipdate < DATE '1995-01-01'
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24
"#,
};

/// TPC-H Q7: Volume Shipping
/// Pattern: 6-way join + CASE
const TPCH_Q7: TpchQuery = TpchQuery {
    name: "Q7",
    sql: r#"
SELECT
    supp_nation, cust_nation, l_year, SUM(volume) AS revenue
FROM (
    SELECT
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        EXTRACT(YEAR FROM l_shipdate) AS l_year,
        l_extendedprice * (1 - l_discount) AS volume
    FROM
        tpch.main.supplier,
        tpch.main.lineitem,
        tpch.main.orders,
        tpch.main.customer,
        tpch.main.nation n1,
        tpch.main.nation n2
    WHERE
        s_suppkey = l_suppkey
        AND o_orderkey = l_orderkey
        AND c_custkey = o_custkey
        AND s_nationkey = n1.n_nationkey
        AND c_nationkey = n2.n_nationkey
        AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
            OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
        AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year
"#,
};

/// TPC-H Q8: National Market Share
/// Pattern: 8-way join
const TPCH_Q8: TpchQuery = TpchQuery {
    name: "Q8",
    sql: r#"
SELECT
    o_year,
    SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share
FROM (
    SELECT
        EXTRACT(YEAR FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) AS volume,
        n2.n_name AS nation
    FROM
        tpch.main.part,
        tpch.main.supplier,
        tpch.main.lineitem,
        tpch.main.orders,
        tpch.main.customer,
        tpch.main.nation n1,
        tpch.main.nation n2,
        tpch.main.region
    WHERE
        p_partkey = l_partkey
        AND s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey
        AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r_regionkey
        AND r_name = 'AMERICA'
        AND s_nationkey = n2.n_nationkey
        AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        AND p_type = 'ECONOMY ANODIZED STEEL'
) AS all_nations
GROUP BY o_year
ORDER BY o_year
"#,
};

/// TPC-H Q9: Product Type Profit Measure
/// Pattern: 6-way join + LIKE
const TPCH_Q9: TpchQuery = TpchQuery {
    name: "Q9",
    sql: r#"
SELECT
    nation, o_year, SUM(amount) AS sum_profit
FROM (
    SELECT
        n_name AS nation,
        EXTRACT(YEAR FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM
        tpch.main.part,
        tpch.main.supplier,
        tpch.main.lineitem,
        tpch.main.partsupp,
        tpch.main.orders,
        tpch.main.nation
    WHERE
        s_suppkey = l_suppkey
        AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey
        AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey
        AND s_nationkey = n_nationkey
        AND p_name LIKE '%green%'
) AS profit
GROUP BY nation, o_year
ORDER BY nation, o_year DESC
"#,
};

/// TPC-H Q10: Returned Item Reporting
/// Pattern: 4-way join
const TPCH_Q10: TpchQuery = TpchQuery {
    name: "Q10",
    sql: r#"
SELECT
    c_custkey, c_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal, n_name, c_address, c_phone, c_comment
FROM
    tpch.main.customer,
    tpch.main.orders,
    tpch.main.lineitem,
    tpch.main.nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= DATE '1993-10-01'
    AND o_orderdate < DATE '1994-01-01'
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20
"#,
};

/// TPC-H Q11: Important Stock Identification
/// Pattern: Nested subquery
const TPCH_Q11: TpchQuery = TpchQuery {
    name: "Q11",
    sql: r#"
SELECT
    ps_partkey, SUM(ps_supplycost * ps_availqty) AS value
FROM
    tpch.main.partsupp,
    tpch.main.supplier,
    tpch.main.nation
WHERE
    ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING SUM(ps_supplycost * ps_availqty) > (
    SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
    FROM tpch.main.partsupp, tpch.main.supplier, tpch.main.nation
    WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
)
ORDER BY value DESC
"#,
};

/// TPC-H Q12: Shipping Modes and Order Priority
/// Pattern: 3-way join + CASE
const TPCH_Q12: TpchQuery = TpchQuery {
    name: "Q12",
    sql: r#"
SELECT
    l_shipmode,
    SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count,
    SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
FROM
    tpch.main.orders,
    tpch.main.lineitem
WHERE
    o_orderkey = l_orderkey
    AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= DATE '1994-01-01'
    AND l_receiptdate < DATE '1995-01-01'
GROUP BY l_shipmode
ORDER BY l_shipmode
"#,
};

/// TPC-H Q13: Customer Distribution
/// Pattern: LEFT OUTER join
const TPCH_Q13: TpchQuery = TpchQuery {
    name: "Q13",
    sql: r#"
SELECT
    c_count, COUNT(*) AS custdist
FROM (
    SELECT c_custkey, COUNT(o_orderkey) AS c_count
    FROM tpch.main.customer
    LEFT OUTER JOIN tpch.main.orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
    GROUP BY c_custkey
) AS c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC
"#,
};

/// TPC-H Q14: Promotion Effect
/// Pattern: 2-way join
const TPCH_Q14: TpchQuery = TpchQuery {
    name: "Q14",
    sql: r#"
SELECT
    100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    tpch.main.lineitem,
    tpch.main.part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= DATE '1995-09-01'
    AND l_shipdate < DATE '1995-10-01'
"#,
};

/// TPC-H Q15: Top Supplier
/// Pattern: View + MAX subquery (using CTE instead of view)
const TPCH_Q15: TpchQuery = TpchQuery {
    name: "Q15",
    sql: r#"
WITH revenue AS (
    SELECT
        l_suppkey AS supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM tpch.main.lineitem
    WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01'
    GROUP BY l_suppkey
)
SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
FROM tpch.main.supplier, revenue
WHERE s_suppkey = supplier_no
    AND total_revenue = (SELECT MAX(total_revenue) FROM revenue)
ORDER BY s_suppkey
"#,
};

/// TPC-H Q16: Parts/Supplier Relationship
/// Pattern: NOT IN subquery
const TPCH_Q16: TpchQuery = TpchQuery {
    name: "Q16",
    sql: r#"
SELECT
    p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM
    tpch.main.partsupp,
    tpch.main.part
WHERE
    p_partkey = ps_partkey
    AND p_brand <> 'Brand#45'
    AND p_type NOT LIKE 'MEDIUM POLISHED%'
    AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps_suppkey NOT IN (
        SELECT s_suppkey FROM tpch.main.supplier WHERE s_comment LIKE '%Customer%Complaints%'
    )
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
"#,
};

/// TPC-H Q17: Small-Quantity-Order Revenue
/// Pattern: AVG subquery
const TPCH_Q17: TpchQuery = TpchQuery {
    name: "Q17",
    sql: r#"
SELECT
    SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM
    tpch.main.lineitem,
    tpch.main.part
WHERE
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT 0.2 * AVG(l_quantity)
        FROM tpch.main.lineitem
        WHERE l_partkey = p_partkey
    )
"#,
};

/// TPC-H Q18: Large Volume Customer
/// Pattern: IN subquery + HAVING
const TPCH_Q18: TpchQuery = TpchQuery {
    name: "Q18",
    sql: r#"
SELECT
    c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) AS total_qty
FROM
    tpch.main.customer,
    tpch.main.orders,
    tpch.main.lineitem
WHERE
    o_orderkey IN (
        SELECT l_orderkey FROM tpch.main.lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
    )
    AND c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100
"#,
};

/// TPC-H Q19: Discounted Revenue
/// Pattern: OR predicates
const TPCH_Q19: TpchQuery = TpchQuery {
    name: "Q19",
    sql: r#"
SELECT
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    tpch.main.lineitem,
    tpch.main.part
WHERE
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#12'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 1 AND l_quantity <= 11
        AND p_size BETWEEN 1 AND 5
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR (
        p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10 AND l_quantity <= 20
        AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR (
        p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20 AND l_quantity <= 30
        AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
"#,
};

/// TPC-H Q20: Potential Part Promotion
/// Pattern: IN + EXISTS subquery
const TPCH_Q20: TpchQuery = TpchQuery {
    name: "Q20",
    sql: r#"
SELECT
    s_name, s_address
FROM
    tpch.main.supplier,
    tpch.main.nation
WHERE
    s_suppkey IN (
        SELECT ps_suppkey
        FROM tpch.main.partsupp
        WHERE ps_partkey IN (SELECT p_partkey FROM tpch.main.part WHERE p_name LIKE 'forest%')
        AND ps_availqty > (
            SELECT 0.5 * SUM(l_quantity)
            FROM tpch.main.lineitem
            WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
                AND l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
        )
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'CANADA'
ORDER BY s_name
"#,
};

/// TPC-H Q21: Suppliers Who Kept Orders Waiting
/// Pattern: EXISTS + NOT EXISTS
const TPCH_Q21: TpchQuery = TpchQuery {
    name: "Q21",
    sql: r#"
SELECT
    s_name, COUNT(*) AS numwait
FROM
    tpch.main.supplier,
    tpch.main.lineitem l1,
    tpch.main.orders,
    tpch.main.nation
WHERE
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT * FROM tpch.main.lineitem l2
        WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS (
        SELECT * FROM tpch.main.lineitem l3
        WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
GROUP BY s_name
ORDER BY numwait DESC, s_name
LIMIT 100
"#,
};

/// TPC-H Q22: Global Sales Opportunity
/// Pattern: NOT EXISTS + SUBSTRING
const TPCH_Q22: TpchQuery = TpchQuery {
    name: "Q22",
    sql: r#"
SELECT
    cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal
FROM (
    SELECT
        SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode,
        c_acctbal
    FROM tpch.main.customer
    WHERE
        SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
        AND c_acctbal > (
            SELECT AVG(c_acctbal) FROM tpch.main.customer
            WHERE c_acctbal > 0.00 AND SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
        )
        AND NOT EXISTS (
            SELECT * FROM tpch.main.orders WHERE o_custkey = c_custkey
        )
) AS custsale
GROUP BY cntrycode
ORDER BY cntrycode
"#,
};

/// Bloom Filter Test: Point lookup on lineitem by order key
/// Pattern: Equality predicate on high-cardinality column (should benefit from bloom filter)
const BLOOM_Q1: TpchQuery = TpchQuery {
    name: "BF1",
    sql: r#"
SELECT
    l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice
FROM
    tpch.main.lineitem
WHERE
    l_orderkey = 12345
"#,
};

/// Bloom Filter Test: Point lookup on orders by order key
/// Pattern: Equality predicate on primary key
const BLOOM_Q2: TpchQuery = TpchQuery {
    name: "BF2",
    sql: r#"
SELECT
    o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate
FROM
    tpch.main.orders
WHERE
    o_orderkey = 54321
"#,
};

/// Bloom Filter Test: Point lookup on customer by customer key
/// Pattern: Equality predicate on primary key
const BLOOM_Q3: TpchQuery = TpchQuery {
    name: "BF3",
    sql: r#"
SELECT
    c_custkey, c_name, c_address, c_nationkey, c_acctbal
FROM
    tpch.main.customer
WHERE
    c_custkey = 7500
"#,
};

/// Bloom Filter Test: Multiple equality predicates
/// Pattern: Multiple equality conditions that can use bloom filters
const BLOOM_Q4: TpchQuery = TpchQuery {
    name: "BF4",
    sql: r#"
SELECT
    l_orderkey, l_linenumber, l_quantity, l_extendedprice
FROM
    tpch.main.lineitem
WHERE
    l_orderkey = 99999
    AND l_linenumber = 3
"#,
};

/// Distinct Index Test: Equality on low-cardinality column (l_returnflag has 3 values: A, F, R)
const INDEX_Q1: TpchQuery = TpchQuery {
    name: "IX1",
    sql: r#"
SELECT *
FROM tpch.main.lineitem
WHERE l_returnflag = 'R'
"#,
};

/// Distinct Index Test: Equality on order status (3 values: F, O, P)
const INDEX_Q2: TpchQuery = TpchQuery {
    name: "IX2",
    sql: r#"
SELECT *
FROM tpch.main.orders
WHERE o_orderstatus = 'F'
"#,
};

/// Distinct Index Test: Equality on nation name (25 values)
const INDEX_Q3: TpchQuery = TpchQuery {
    name: "IX3",
    sql: r#"
SELECT *
FROM tpch.main.nation
WHERE n_name = 'GERMANY'
"#,
};

/// Distinct Index Test: IN predicate on ship mode (7 values)
const INDEX_Q4: TpchQuery = TpchQuery {
    name: "IX4",
    sql: r#"
SELECT *
FROM tpch.main.lineitem
WHERE l_shipmode IN ('AIR', 'MAIL')
"#,
};

/// All TPC-H queries to run
const TPCH_QUERIES: &[TpchQuery] = &[
    TPCH_Q1, TPCH_Q2, TPCH_Q3, TPCH_Q4, TPCH_Q5, TPCH_Q6, TPCH_Q7, TPCH_Q8, TPCH_Q9, TPCH_Q10,
    TPCH_Q11, TPCH_Q12, TPCH_Q13, TPCH_Q14, TPCH_Q15, TPCH_Q16, TPCH_Q17, TPCH_Q18, TPCH_Q19,
    TPCH_Q20, TPCH_Q21, TPCH_Q22,
];

/// Bloom filter specific queries (equality predicates - bloom filters should help)
const BLOOM_QUERIES: &[TpchQuery] = &[BLOOM_Q1, BLOOM_Q2, BLOOM_Q3, BLOOM_Q4];

/// Distinct index specific queries (equality/IN on low-cardinality columns)
const INDEX_QUERIES: &[TpchQuery] = &[INDEX_Q1, INDEX_Q2, INDEX_Q3, INDEX_Q4];

/// Result of a single query benchmark
struct QueryBenchmarkResult {
    name: &'static str,
    duration: Duration,
    row_count: usize,
    success: bool,
    error: Option<String>,
}

/// Test harness for TPC-H benchmarks
struct TpchBenchmarkHarness {
    engine: Arc<RuntimeEngine>,
    router: Router,
    #[allow(dead_code)]
    temp_dir: TempDir,
    tpch_db_path: String,
    scale_factor: f64,
}

impl TpchBenchmarkHarness {
    /// Create a new harness with TPC-H data at the specified scale factor
    async fn new(scale_factor: f64) -> Result<Self> {
        Self::new_with_projections(scale_factor, false).await
    }

    /// Create a new harness with TPC-H optimized projections enabled
    async fn new_with_projections(scale_factor: f64, enable_projections: bool) -> Result<Self> {
        let temp_dir = tempfile::tempdir()?;

        let mut builder = RuntimeEngine::builder()
            .base_dir(temp_dir.path())
            .secret_key(generate_test_secret_key());

        if enable_projections {
            builder = builder.with_tpch_projections();
        }

        let engine = builder.build().await?;

        let app = AppServer::new(engine);

        // Create TPC-H database
        let tpch_db_path = Self::create_tpch_duckdb(&temp_dir, scale_factor)?;

        Ok(Self {
            engine: app.engine,
            router: app.router,
            temp_dir,
            tpch_db_path,
            scale_factor,
        })
    }

    /// Create a new harness with a custom parquet config.
    async fn new_with_parquet_config(scale_factor: f64, config: ParquetConfig) -> Result<Self> {
        let temp_dir = tempfile::tempdir()?;

        let engine = RuntimeEngine::builder()
            .base_dir(temp_dir.path())
            .secret_key(generate_test_secret_key())
            .parquet_config(config)
            .build()
            .await?;

        let app = AppServer::new(engine);

        let tpch_db_path = Self::create_tpch_duckdb(&temp_dir, scale_factor)?;

        Ok(Self {
            engine: app.engine,
            router: app.router,
            temp_dir,
            tpch_db_path,
            scale_factor,
        })
    }

    /// Create a DuckDB with TPC-H data using the built-in dbgen
    fn create_tpch_duckdb(temp_dir: &TempDir, scale_factor: f64) -> Result<String> {
        let db_path = temp_dir.path().join("tpch.duckdb");
        let conn = duckdb::Connection::open(&db_path)?;

        // Install and load TPC-H extension
        conn.execute("INSTALL tpch", [])?;
        conn.execute("LOAD tpch", [])?;

        // Generate TPC-H data at specified scale factor
        conn.execute(&format!("CALL dbgen(sf = {})", scale_factor), [])?;

        Ok(db_path.to_str().unwrap().to_string())
    }

    /// Create a connection to the TPC-H database via API
    async fn create_connection(&self) -> Result<String> {
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(PATH_CONNECTIONS)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&json!({
                        "name": "tpch",
                        "source_type": "duckdb",
                        "config": {
                            "path": self.tpch_db_path
                        }
                    }))?))?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::CREATED);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let json: serde_json::Value = serde_json::from_slice(&body)?;

        Ok(json["id"].as_str().unwrap().to_string())
    }

    /// Get total size of all parquet files in the cache directory
    fn get_parquet_cache_size(base_dir: &std::path::Path) -> u64 {
        let mut total = 0u64;
        fn walk_dir(dir: &std::path::Path, total: &mut u64) {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        walk_dir(&path, total);
                    } else if path.extension().map_or(false, |ext| ext == "parquet") {
                        if let Ok(meta) = std::fs::metadata(&path) {
                            *total += meta.len();
                        }
                    }
                }
            }
        }
        walk_dir(base_dir, &mut total);
        total
    }

    /// Run a single benchmark query
    async fn run_query(&self, query: &TpchQuery) -> QueryBenchmarkResult {
        let start = Instant::now();

        match self.engine.execute_query(query.sql).await {
            Ok(response) => {
                let duration = start.elapsed();
                let row_count: usize = response.results.iter().map(|b| b.num_rows()).sum();

                QueryBenchmarkResult {
                    name: query.name,
                    duration,
                    row_count,
                    success: true,
                    error: None,
                }
            }
            Err(e) => QueryBenchmarkResult {
                name: query.name,
                duration: start.elapsed(),
                row_count: 0,
                success: false,
                error: Some(e.to_string()),
            },
        }
    }

    /// Run all benchmark queries and print results
    async fn run_benchmark(&self) -> Vec<QueryBenchmarkResult> {
        println!();
        println!("========================================");
        println!("TPC-H Benchmark Results (SF={})", self.scale_factor);
        println!("========================================");
        println!();

        let mut results = Vec::new();

        // Run each query
        println!("Query Results (includes sync time on first run):");
        println!(
            "{:<15} {:>15} {:>10}   {}",
            "Query", "Time (ms)", "Rows", "Status"
        );
        println!("{}", "-".repeat(50));

        for query in TPCH_QUERIES {
            let result = self.run_query(query).await;

            let status = if result.success {
                "OK".to_string()
            } else {
                format!("FAIL: {}", result.error.as_deref().unwrap_or("unknown"))
            };

            println!(
                "{:<15} {:>15.2} {:>10}   {}",
                result.name,
                result.duration.as_secs_f64() * 1000.0,
                result.row_count,
                status
            );

            results.push(result);
        }

        println!("{}", "-".repeat(50));

        // Calculate totals
        let total_time: Duration = results.iter().map(|r| r.duration).sum();
        let successful_count = results.iter().filter(|r| r.success).count();

        println!(
            "Total Query Time: {:.3}s ({}/{} queries succeeded)",
            total_time.as_secs_f64(),
            successful_count,
            results.len()
        );
        println!();

        results
    }

    /// Run benchmark with warmup - first run syncs data, second run measures cached performance
    async fn run_benchmark_with_warmup(
        &self,
    ) -> (Vec<QueryBenchmarkResult>, Vec<QueryBenchmarkResult>) {
        println!();
        println!("========================================");
        println!("TPC-H Benchmark Results (SF={})", self.scale_factor);
        println!("========================================");

        // First pass: warmup (triggers sync from DuckDB to Parquet cache)
        println!();
        println!("--- Warmup Pass (includes sync time) ---");
        println!(
            "{:<15} {:>15} {:>10}   {}",
            "Query", "Time (ms)", "Rows", "Status"
        );
        println!("{}", "-".repeat(50));

        let mut warmup_results = Vec::new();
        for query in TPCH_QUERIES {
            let result = self.run_query(query).await;
            let status = if result.success { "OK" } else { "FAIL" };
            println!(
                "{:<15} {:>15.2} {:>10}   {}",
                result.name,
                result.duration.as_secs_f64() * 1000.0,
                result.row_count,
                status
            );
            warmup_results.push(result);
        }

        let warmup_total: Duration = warmup_results.iter().map(|r| r.duration).sum();
        println!("{}", "-".repeat(50));
        println!("Warmup Total: {:.3}s", warmup_total.as_secs_f64());

        // Print cache size
        let cache_size = Self::get_parquet_cache_size(self.temp_dir.path());
        println!(
            "Parquet Cache Size: {:.2} MB",
            cache_size as f64 / 1024.0 / 1024.0
        );

        // Second pass: cached performance (reads from Parquet cache)
        println!();
        println!("--- Cached Pass (reads from Parquet) ---");
        println!(
            "{:<15} {:>15} {:>10}   {}",
            "Query", "Time (ms)", "Rows", "Status"
        );
        println!("{}", "-".repeat(50));

        let mut cached_results = Vec::new();
        for query in TPCH_QUERIES {
            let result = self.run_query(query).await;
            let status = if result.success { "OK" } else { "FAIL" };
            println!(
                "{:<15} {:>15.2} {:>10}   {}",
                result.name,
                result.duration.as_secs_f64() * 1000.0,
                result.row_count,
                status
            );
            cached_results.push(result);
        }

        let cached_total: Duration = cached_results.iter().map(|r| r.duration).sum();
        let successful_count = cached_results.iter().filter(|r| r.success).count();
        println!("{}", "-".repeat(50));
        println!(
            "Cached Total: {:.3}s ({}/{} queries succeeded)",
            cached_total.as_secs_f64(),
            successful_count,
            cached_results.len()
        );
        println!();

        (warmup_results, cached_results)
    }

    /// Run bloom filter benchmark - tests equality predicates where bloom filters help
    async fn run_bloom_filter_benchmark(&self) -> Vec<QueryBenchmarkResult> {
        println!();
        println!("========================================");
        println!("Bloom Filter Benchmark (SF={})", self.scale_factor);
        println!("========================================");
        println!();

        // First, warmup TPC-H queries to sync the main tables
        println!("--- Syncing tables (warmup) ---");
        for query in TPCH_QUERIES {
            let _ = self.run_query(query).await;
        }
        println!("Tables synced.");

        // Now run bloom filter specific queries (equality predicates)
        println!();
        println!("--- Bloom Filter Queries (equality predicates) ---");
        println!(
            "{:<15} {:>15} {:>10}   {}",
            "Query", "Time (ms)", "Rows", "Status"
        );
        println!("{}", "-".repeat(50));

        let mut results = Vec::new();
        for query in BLOOM_QUERIES {
            let result = self.run_query(query).await;
            let status = if result.success { "OK" } else { "FAIL" };
            println!(
                "{:<15} {:>15.2} {:>10}   {}",
                result.name,
                result.duration.as_secs_f64() * 1000.0,
                result.row_count,
                status
            );
            results.push(result);
        }

        let total: Duration = results.iter().map(|r| r.duration).sum();
        let successful_count = results.iter().filter(|r| r.success).count();
        println!("{}", "-".repeat(50));
        println!(
            "Total: {:.3}s ({}/{} queries succeeded)",
            total.as_secs_f64(),
            successful_count,
            results.len()
        );
        println!();

        results
    }

    /// Run distinct index benchmark - tests equality/IN predicates on low-cardinality columns
    async fn run_distinct_index_benchmark(&self) -> Vec<QueryBenchmarkResult> {
        println!();
        println!("========================================");
        println!("Distinct Index Benchmark (SF={})", self.scale_factor);
        println!("========================================");
        println!();

        // First, warmup TPC-H queries to sync the main tables
        println!("--- Syncing tables (warmup) ---");
        for query in TPCH_QUERIES {
            let _ = self.run_query(query).await;
        }
        println!("Tables synced.");

        // Now run distinct index specific queries
        println!();
        println!("--- Distinct Index Queries (equality/IN on low-cardinality columns) ---");
        println!(
            "{:<15} {:>15} {:>10}   {}",
            "Query", "Time (ms)", "Rows", "Status"
        );
        println!("{}", "-".repeat(50));

        let mut results = Vec::new();
        for query in INDEX_QUERIES {
            let result = self.run_query(query).await;
            let status = if result.success { "OK" } else { "FAIL" };
            println!(
                "{:<15} {:>15.2} {:>10}   {}",
                result.name,
                result.duration.as_secs_f64() * 1000.0,
                result.row_count,
                status
            );
            results.push(result);
        }

        let total: Duration = results.iter().map(|r| r.duration).sum();
        let successful_count = results.iter().filter(|r| r.success).count();
        println!("{}", "-".repeat(50));
        println!(
            "Total: {:.3}s ({}/{} queries succeeded)",
            total.as_secs_f64(),
            successful_count,
            results.len()
        );
        println!();

        results
    }
}

/// Get scale factor from environment variable or use default
fn get_scale_factor(default: f64) -> f64 {
    std::env::var("TPCH_SCALE_FACTOR")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Quick TPC-H benchmark test (default SF=0.01 for CI)
#[tokio::test(flavor = "multi_thread")]
async fn test_tpch_benchmark() -> Result<()> {
    let scale_factor = get_scale_factor(0.01);

    println!("Setting up TPC-H benchmark (SF={})...", scale_factor);
    let setup_start = Instant::now();

    let harness = TpchBenchmarkHarness::new(scale_factor).await?;

    let data_gen_time = setup_start.elapsed();
    println!("  Data Generation: {:.3}s", data_gen_time.as_secs_f64());

    let conn_start = Instant::now();
    let _connection_id = harness.create_connection().await?;
    let conn_time = conn_start.elapsed();
    println!("  Connection Setup: {:.3}s", conn_time.as_secs_f64());

    // Run benchmark with warmup to separate sync time from cached query time
    let (_warmup_results, cached_results) = harness.run_benchmark_with_warmup().await;

    // Verify all queries succeeded
    let failed: Vec<_> = cached_results.iter().filter(|r| !r.success).collect();
    assert!(
        failed.is_empty(),
        "Some queries failed: {:?}",
        failed
            .iter()
            .map(|r| format!("{}: {}", r.name, r.error.as_deref().unwrap_or("unknown")))
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// TPC-H benchmark with projections enabled
/// Run with: TPCH_SCALE_FACTOR=1.0 cargo test tpch_benchmark_projections --release -- --nocapture --test-threads=1
#[tokio::test(flavor = "multi_thread")]
async fn test_tpch_benchmark_projections() -> Result<()> {
    let scale_factor = get_scale_factor(0.01);

    println!(
        "Setting up TPC-H benchmark WITH PROJECTIONS (SF={})...",
        scale_factor
    );
    let setup_start = Instant::now();

    // Enable TPC-H optimized projections
    let harness = TpchBenchmarkHarness::new_with_projections(scale_factor, true).await?;

    let data_gen_time = setup_start.elapsed();
    println!("  Data Generation: {:.3}s", data_gen_time.as_secs_f64());

    let conn_start = Instant::now();
    let _connection_id = harness.create_connection().await?;
    let conn_time = conn_start.elapsed();
    println!("  Connection Setup: {:.3}s", conn_time.as_secs_f64());

    // Run benchmark with warmup to separate sync time from cached query time
    let (_warmup_results, cached_results) = harness.run_benchmark_with_warmup().await;

    // Verify all queries succeeded
    let failed: Vec<_> = cached_results.iter().filter(|r| !r.success).collect();
    assert!(
        failed.is_empty(),
        "Some queries failed: {:?}",
        failed
            .iter()
            .map(|r| format!("{}: {}", r.name, r.error.as_deref().unwrap_or("unknown")))
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// Full TPC-H benchmark test with SF=1.0 (~1GB data)
/// Run with: cargo test tpch_benchmark_full --release -- --ignored --nocapture
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_tpch_benchmark_full() -> Result<()> {
    let scale_factor = 1.0;

    println!("Setting up TPC-H benchmark (SF={})...", scale_factor);
    println!("Note: This may take a while to generate ~1GB of data...");
    let setup_start = Instant::now();

    let harness = TpchBenchmarkHarness::new(scale_factor).await?;

    let data_gen_time = setup_start.elapsed();
    println!("  Data Generation: {:.3}s", data_gen_time.as_secs_f64());

    let conn_start = Instant::now();
    let _connection_id = harness.create_connection().await?;
    let conn_time = conn_start.elapsed();
    println!("  Connection Setup: {:.3}s", conn_time.as_secs_f64());

    // Run benchmark
    let results = harness.run_benchmark().await;

    // Verify all queries succeeded
    let failed: Vec<_> = results.iter().filter(|r| !r.success).collect();
    assert!(
        failed.is_empty(),
        "Some queries failed: {:?}",
        failed
            .iter()
            .map(|r| format!("{}: {}", r.name, r.error.as_deref().unwrap_or("unknown")))
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// Bloom filter benchmark - tests equality predicates where bloom filters should help
/// Run with: TPCH_SCALE_FACTOR=0.1 cargo test bloom_filter_benchmark --release -- --nocapture
#[tokio::test(flavor = "multi_thread")]
async fn test_bloom_filter_benchmark() -> Result<()> {
    let scale_factor = get_scale_factor(0.1);

    println!("Setting up Bloom Filter benchmark (SF={})...", scale_factor);
    let setup_start = Instant::now();

    let harness = TpchBenchmarkHarness::new(scale_factor).await?;

    let data_gen_time = setup_start.elapsed();
    println!("  Data Generation: {:.3}s", data_gen_time.as_secs_f64());

    let conn_start = Instant::now();
    let _connection_id = harness.create_connection().await?;
    let conn_time = conn_start.elapsed();
    println!("  Connection Setup: {:.3}s", conn_time.as_secs_f64());

    // Run bloom filter benchmark (equality predicates)
    let results = harness.run_bloom_filter_benchmark().await;

    // Verify all queries succeeded
    let failed: Vec<_> = results.iter().filter(|r| !r.success).collect();
    assert!(
        failed.is_empty(),
        "Some queries failed: {:?}",
        failed
            .iter()
            .map(|r| format!("{}: {}", r.name, r.error.as_deref().unwrap_or("unknown")))
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// Distinct index benchmark - tests equality/IN predicates on low-cardinality columns
/// Run with: TPCH_SCALE_FACTOR=0.1 cargo test distinct_index_benchmark --release -- --nocapture
#[tokio::test(flavor = "multi_thread")]
async fn test_distinct_index_benchmark() -> Result<()> {
    let scale_factor = get_scale_factor(0.1);

    println!(
        "Setting up Distinct Index benchmark (SF={})...",
        scale_factor
    );
    let setup_start = Instant::now();

    let harness = TpchBenchmarkHarness::new(scale_factor).await?;

    let data_gen_time = setup_start.elapsed();
    println!("  Data Generation: {:.3}s", data_gen_time.as_secs_f64());

    let conn_start = Instant::now();
    let _connection_id = harness.create_connection().await?;
    let conn_time = conn_start.elapsed();
    println!("  Connection Setup: {:.3}s", conn_time.as_secs_f64());

    // Run distinct index benchmark
    let results = harness.run_distinct_index_benchmark().await;

    // Verify all queries succeeded
    let failed: Vec<_> = results.iter().filter(|r| !r.success).collect();
    assert!(
        failed.is_empty(),
        "Some queries failed: {:?}",
        failed
            .iter()
            .map(|r| format!("{}: {}", r.name, r.error.as_deref().unwrap_or("unknown")))
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// Custom query test - compares ORDER BY performance with and without index
/// Run with: TPCH_SCALE_FACTOR=0.1 cargo test test_custom_query --release -- --nocapture
#[tokio::test(flavor = "multi_thread")]
async fn test_custom_query() -> Result<()> {
    let scale_factor = get_scale_factor(0.1);
    let query = "SELECT * FROM tpch.main.orders ORDER BY o_orderdate DESC LIMIT 5";
    let filter_query = "SELECT * FROM tpch.main.orders WHERE o_orderdate >= DATE '1998-07-01' ORDER BY o_orderdate LIMIT 5";
    let iterations = 5;

    let harness = TpchBenchmarkHarness::new(scale_factor).await?;
    let _conn = harness.create_connection().await?;

    // Sync
    let _ = harness.engine.execute_query("SELECT COUNT(*) FROM tpch.main.orders").await?;

    // Warmup
    let _ = harness.engine.execute_query(query).await?;
    let _ = harness.engine.execute_query(filter_query).await?;

    // === No index: timed runs ===
    println!("=== WITHOUT Index ===");
    let mut times = Vec::new();
    for _ in 0..iterations {
        let start = Instant::now();
        let r = harness.engine.execute_query(query).await?;
        let elapsed = start.elapsed();
        let rows: usize = r.results.iter().map(|b| b.num_rows()).sum();
        println!("  ORDER BY DESC LIMIT 5: {:.2}ms, {} rows", elapsed.as_secs_f64() * 1000.0, rows);
        times.push(elapsed);
    }
    times.sort();
    let median_no_idx = times[times.len() / 2];

    let mut times = Vec::new();
    for _ in 0..iterations {
        let start = Instant::now();
        let r = harness.engine.execute_query(filter_query).await?;
        let elapsed = start.elapsed();
        let rows: usize = r.results.iter().map(|b| b.num_rows()).sum();
        println!("  Filter+ORDER BY:       {:.2}ms, {} rows", elapsed.as_secs_f64() * 1000.0, rows);
        times.push(elapsed);
    }
    times.sort();
    let median_filter_no_idx = times[times.len() / 2];

    // === Create index ===
    println!("\nCreating index on o_orderdate...");
    let idx_start = Instant::now();
    harness.engine.execute_query("CREATE INDEX idx_orderdate ON tpch.main.orders (o_orderdate)").await?;
    println!("  Index created in {:.2}ms\n", idx_start.elapsed().as_secs_f64() * 1000.0);

    // Warmup after index creation (stabilize system)
    for _ in 0..3 {
        let _ = harness.engine.execute_query(query).await?;
        let _ = harness.engine.execute_query(filter_query).await?;
    }

    // === With index: timed runs ===
    println!("=== WITH Index ===");
    let mut times = Vec::new();
    for _ in 0..iterations {
        let start = Instant::now();
        let r = harness.engine.execute_query(query).await?;
        let elapsed = start.elapsed();
        let rows: usize = r.results.iter().map(|b| b.num_rows()).sum();
        println!("  ORDER BY DESC LIMIT 5: {:.2}ms, {} rows", elapsed.as_secs_f64() * 1000.0, rows);
        times.push(elapsed);
    }
    times.sort();
    let median_idx = times[times.len() / 2];

    let mut times = Vec::new();
    for _ in 0..iterations {
        let start = Instant::now();
        let r = harness.engine.execute_query(filter_query).await?;
        let elapsed = start.elapsed();
        let rows: usize = r.results.iter().map(|b| b.num_rows()).sum();
        println!("  Filter+ORDER BY:       {:.2}ms, {} rows", elapsed.as_secs_f64() * 1000.0, rows);
        times.push(elapsed);
    }
    times.sort();
    let median_filter_idx = times[times.len() / 2];

    // === EXPLAIN ===
    println!("\n=== EXPLAIN (with index) ===");
    for q in [query, filter_query] {
        println!("Query: {}", q);
        if let Ok(r) = harness.engine.execute_query(&format!("EXPLAIN {}", q)).await {
            for batch in &r.results {
                use datafusion::arrow::array::StringArray;
                if let Some(col) = batch.column(1).as_any().downcast_ref::<StringArray>() {
                    for i in 0..batch.num_rows() {
                        println!("  {}", col.value(i));
                    }
                }
            }
        }
        println!();
    }

    // === Summary ===
    println!("=== Results ===");
    println!("{:<25} {:>12} {:>12} {:>10}", "Query", "No Index", "With Index", "Speedup");
    println!("{}", "-".repeat(62));
    println!(
        "{:<25} {:>10.2}ms {:>10.2}ms {:>9.2}x",
        "ORDER BY DESC LIMIT 5",
        median_no_idx.as_secs_f64() * 1000.0,
        median_idx.as_secs_f64() * 1000.0,
        median_no_idx.as_secs_f64() / median_idx.as_secs_f64(),
    );
    println!(
        "{:<25} {:>10.2}ms {:>10.2}ms {:>9.2}x",
        "Filter+ORDER BY",
        median_filter_no_idx.as_secs_f64() * 1000.0,
        median_filter_idx.as_secs_f64() * 1000.0,
        median_filter_no_idx.as_secs_f64() / median_filter_idx.as_secs_f64(),
    );

    Ok(())
}

/// Test CREATE INDEX SQL command
/// Run with: cargo test test_create_index_sql --release -- --nocapture --test-threads=1
#[tokio::test(flavor = "multi_thread")]
async fn test_create_index_sql() -> Result<()> {
    let scale_factor = get_scale_factor(0.01);

    println!("=== CREATE INDEX SQL Test (SF={}) ===", scale_factor);

    let harness = TpchBenchmarkHarness::new(scale_factor).await?;
    let _connection_id = harness.create_connection().await?;

    // Step 1: First sync a table (run a query to trigger caching)
    println!("\nStep 1: Syncing orders table...");
    let sync_start = Instant::now();
    let result = harness
        .engine
        .execute_query("SELECT COUNT(*) FROM tpch.main.orders")
        .await;
    match &result {
        Ok(r) => {
            let row_count: usize = r.results.iter().map(|b| b.num_rows()).sum();
            println!(
                "  Sync complete: {:.2}ms, {} result rows",
                sync_start.elapsed().as_secs_f64() * 1000.0,
                row_count
            );
        }
        Err(e) => {
            println!("  Sync failed: {}", e);
            return Err(anyhow::anyhow!("Sync failed: {}", e));
        }
    }

    // Step 2: Create an index on o_orderdate
    println!("\nStep 2: Creating index on o_orderdate...");
    let create_start = Instant::now();
    let result = harness
        .engine
        .execute_query("CREATE INDEX idx_orderdate ON tpch.main.orders (o_orderdate)")
        .await;
    match &result {
        Ok(r) => {
            let row_count: usize = r.results.iter().map(|b| b.num_rows()).sum();
            println!(
                "  CREATE INDEX: {:.2}ms, {} result rows",
                create_start.elapsed().as_secs_f64() * 1000.0,
                row_count
            );
            // Print the result
            if let Some(batch) = r.results.first() {
                use datafusion::arrow::array::StringArray;
                if let Some(status_col) = batch.column(0).as_any().downcast_ref::<StringArray>() {
                    println!("  Status: {}", status_col.value(0));
                }
            }
        }
        Err(e) => {
            println!("  CREATE INDEX failed: {}", e);
            return Err(anyhow::anyhow!("CREATE INDEX failed: {}", e));
        }
    }

    // Step 3: List indexes using SHOW INDEXES
    println!("\nStep 3: Listing indexes...");
    let result = harness
        .engine
        .execute_query("SHOW INDEXES ON tpch.main.orders")
        .await;
    match &result {
        Ok(r) => {
            println!("  SHOW INDEXES returned {} rows:", r.results.iter().map(|b| b.num_rows()).sum::<usize>());
            if let Some(batch) = r.results.first() {
                use datafusion::arrow::array::StringArray;
                for i in 0..batch.num_rows() {
                    let name = batch.column(0).as_any().downcast_ref::<StringArray>().map(|a| a.value(i)).unwrap_or("");
                    let cols = batch.column(1).as_any().downcast_ref::<StringArray>().map(|a| a.value(i)).unwrap_or("");
                    println!("    - {} (columns: {})", name, cols);
                }
            }
        }
        Err(e) => {
            println!("  SHOW INDEXES failed: {}", e);
            return Err(anyhow::anyhow!("SHOW INDEXES failed: {}", e));
        }
    }

    // Step 4: Test query performance with the index
    println!("\nStep 4: Testing query with date range filter...");
    let query = "SELECT * FROM tpch.main.orders WHERE o_orderdate >= DATE '1995-01-01' AND o_orderdate < DATE '1995-02-01' LIMIT 10";
    let query_start = Instant::now();
    let result = harness.engine.execute_query(query).await;
    match &result {
        Ok(r) => {
            let row_count: usize = r.results.iter().map(|b| b.num_rows()).sum();
            println!(
                "  Query: {:.2}ms, {} rows",
                query_start.elapsed().as_secs_f64() * 1000.0,
                row_count
            );
        }
        Err(e) => println!("  Query failed: {}", e),
    }

    // Step 5: Drop the index
    println!("\nStep 5: Dropping index...");
    let result = harness
        .engine
        .execute_query("DROP INDEX idx_orderdate ON tpch.main.orders")
        .await;
    match &result {
        Ok(r) => {
            if let Some(batch) = r.results.first() {
                use datafusion::arrow::array::StringArray;
                if let Some(status_col) = batch.column(0).as_any().downcast_ref::<StringArray>() {
                    println!("  Status: {}", status_col.value(0));
                }
            }
        }
        Err(e) => {
            println!("  DROP INDEX failed: {}", e);
            return Err(anyhow::anyhow!("DROP INDEX failed: {}", e));
        }
    }

    // Step 6: Verify index was dropped
    println!("\nStep 6: Verifying index was dropped...");
    let result = harness
        .engine
        .execute_query("SHOW INDEXES ON tpch.main.orders")
        .await;
    match &result {
        Ok(r) => {
            let row_count: usize = r.results.iter().map(|b| b.num_rows()).sum();
            println!("  Indexes remaining: {}", row_count);
            assert_eq!(row_count, 0, "Index should have been dropped");
        }
        Err(e) => {
            println!("  SHOW INDEXES failed: {}", e);
            return Err(anyhow::anyhow!("SHOW INDEXES failed: {}", e));
        }
    }

    println!("\n=== All steps completed successfully! ===");
    Ok(())
}

/// Index performance benchmark — compares TPC-H Q1-Q22 with and without indexes.
///
/// Creates 6 indexes matching the tpch_optimized projection configuration:
///   1. lineitem(l_shipdate)  — range filters in Q1, Q3, Q6, Q7, Q14, Q15, Q20
///   2. orders(o_orderdate)   — range filters in Q3, Q4, Q5, Q8, Q10
///   3. lineitem(l_partkey)   — join key in Q2, Q9, Q14, Q17, Q19, Q20
///   4. orders(o_custkey)     — join key in Q3, Q5, Q8, Q10, Q13, Q18, Q22
///   5. customer(c_nationkey) — join key in Q5, Q7, Q8, Q10
///   6. partsupp(ps_suppkey)  — join key in Q2, Q9, Q11, Q16, Q20
///
/// Run with: TPCH_SCALE_FACTOR=1.0 cargo test test_index_performance --release -- --nocapture --test-threads=1
#[tokio::test(flavor = "multi_thread")]
async fn test_index_performance() -> Result<()> {
    let scale_factor = get_scale_factor(0.1);

    println!("=== Index Performance Benchmark (SF={}) ===", scale_factor);
    let setup_start = Instant::now();

    let harness = TpchBenchmarkHarness::new(scale_factor).await?;

    let data_gen_time = setup_start.elapsed();
    println!("  Data Generation: {:.3}s", data_gen_time.as_secs_f64());

    let _connection_id = harness.create_connection().await?;

    // ── Warmup: sync all tables ──────────────────────────────
    println!("\n--- Syncing tables (warmup) ---");
    for query in TPCH_QUERIES {
        let _ = harness.run_query(query).await;
    }
    let cache_size = TpchBenchmarkHarness::get_parquet_cache_size(harness.temp_dir.path());
    println!(
        "  Parquet Cache Size: {:.2} MB",
        cache_size as f64 / 1024.0 / 1024.0
    );

    // ── Baseline: cached read WITHOUT indexes ────────────────
    println!("\n--- Baseline (no indexes) ---");
    println!(
        "{:<15} {:>15} {:>10}   {}",
        "Query", "Time (ms)", "Rows", "Status"
    );
    println!("{}", "-".repeat(55));

    let mut baseline_results = Vec::new();
    for query in TPCH_QUERIES {
        let result = harness.run_query(query).await;
        let status = if result.success { "OK" } else { "FAIL" };
        println!(
            "{:<15} {:>15.2} {:>10}   {}",
            result.name,
            result.duration.as_secs_f64() * 1000.0,
            result.row_count,
            status
        );
        baseline_results.push(result);
    }
    let baseline_total: Duration = baseline_results.iter().map(|r| r.duration).sum();
    println!("{}", "-".repeat(55));
    println!("Baseline Total: {:.3}s", baseline_total.as_secs_f64());

    // ── Create 6 indexes (matches tpch_optimized projections) ──
    println!("\n--- Creating indexes ---");

    let indexes = [
        ("idx_shipdate", "tpch.main.lineitem", "(l_shipdate)"),
        ("idx_orderdate", "tpch.main.orders", "(o_orderdate)"),
        ("idx_partkey", "tpch.main.lineitem", "(l_partkey)"),
        ("idx_custkey", "tpch.main.orders", "(o_custkey)"),
        ("idx_nationkey", "tpch.main.customer", "(c_nationkey)"),
        ("idx_suppkey", "tpch.main.partsupp", "(ps_suppkey)"),
    ];

    for (name, table, cols) in &indexes {
        let sql = format!("CREATE INDEX {} ON {} {}", name, table, cols);
        let start = Instant::now();
        match harness.engine.execute_query(&sql).await {
            Ok(_) => println!(
                "  {} on {} {}: {:.2}ms",
                name,
                table,
                cols,
                start.elapsed().as_secs_f64() * 1000.0
            ),
            Err(e) => println!("  FAILED {}: {}", name, e),
        }
    }

    // Show total cache size with indexes
    let cache_size_with_idx = TpchBenchmarkHarness::get_parquet_cache_size(harness.temp_dir.path());
    println!(
        "  Cache Size with indexes: {:.2} MB (+{:.2} MB)",
        cache_size_with_idx as f64 / 1024.0 / 1024.0,
        (cache_size_with_idx - cache_size) as f64 / 1024.0 / 1024.0
    );

    // ── Indexed: cached read WITH indexes ────────────────────
    println!("\n--- With Indexes ---");
    println!(
        "{:<15} {:>15} {:>10} {:>12}   {}",
        "Query", "Time (ms)", "Rows", "vs Baseline", "Status"
    );
    println!("{}", "-".repeat(70));

    let mut indexed_results = Vec::new();
    for (i, query) in TPCH_QUERIES.iter().enumerate() {
        let result = harness.run_query(query).await;
        let status = if result.success { "OK" } else { "FAIL" };

        let baseline_ms = baseline_results[i].duration.as_secs_f64() * 1000.0;
        let indexed_ms = result.duration.as_secs_f64() * 1000.0;
        let delta = if baseline_ms > 0.0 {
            let pct = ((indexed_ms - baseline_ms) / baseline_ms) * 100.0;
            if pct < -1.0 {
                format!("{:.0}%", pct)
            } else if pct > 1.0 {
                format!("+{:.0}%", pct)
            } else {
                "~same".to_string()
            }
        } else {
            "N/A".to_string()
        };

        println!(
            "{:<15} {:>15.2} {:>10} {:>12}   {}",
            result.name, indexed_ms, result.row_count, delta, status
        );
        indexed_results.push(result);
    }

    let indexed_total: Duration = indexed_results.iter().map(|r| r.duration).sum();
    let total_speedup = baseline_total.as_secs_f64() / indexed_total.as_secs_f64();
    println!("{}", "-".repeat(70));
    println!(
        "Indexed Total: {:.3}s (baseline {:.3}s, {:.2}x)",
        indexed_total.as_secs_f64(),
        baseline_total.as_secs_f64(),
        total_speedup,
    );

    // ── Summary ──────────────────────────────────────────────
    println!("\n--- Per-Query Comparison ---");
    println!(
        "{:<8} {:>12} {:>12} {:>10}",
        "Query", "Baseline", "Indexed", "Change"
    );
    println!("{}", "-".repeat(45));

    for (i, query) in TPCH_QUERIES.iter().enumerate() {
        let b = baseline_results[i].duration.as_secs_f64() * 1000.0;
        let idx = indexed_results[i].duration.as_secs_f64() * 1000.0;
        let diff = idx - b;
        let sign = if diff < 0.0 { "" } else { "+" };
        println!(
            "{:<8} {:>10.2}ms {:>10.2}ms {:>8}{:.1}ms",
            query.name, b, idx, sign, diff
        );
    }
    println!("{}", "-".repeat(45));
    println!(
        "{:<8} {:>10.0}ms {:>10.0}ms {:>8}{:.0}ms",
        "TOTAL",
        baseline_total.as_secs_f64() * 1000.0,
        indexed_total.as_secs_f64() * 1000.0,
        if indexed_total < baseline_total { "" } else { "+" },
        (indexed_total.as_secs_f64() - baseline_total.as_secs_f64()) * 1000.0,
    );

    // Verify correctness: row counts must match
    for (i, _query) in TPCH_QUERIES.iter().enumerate() {
        assert_eq!(
            baseline_results[i].row_count, indexed_results[i].row_count,
            "Row count mismatch for {}: baseline={} vs indexed={}",
            baseline_results[i].name, baseline_results[i].row_count, indexed_results[i].row_count
        );
    }

    Ok(())
}

/// Old config (1M row groups + bloom filters) vs Current config (100K row groups + 6 indexes).
///
/// Run with: TPCH_SCALE_FACTOR=1.0 cargo test test_old_vs_current_config --release -- --nocapture --test-threads=1
#[tokio::test(flavor = "multi_thread")]
async fn test_old_vs_current_config() -> Result<()> {
    let scale_factor = get_scale_factor(0.1);

    println!("=== Old (1M+bloom) vs Current (100K+indexes) Benchmark (SF={}) ===", scale_factor);

    // ── OLD CONFIG: 1M row groups + bloom filters ──────────────
    println!("\n--- Setting up OLD config (1M row groups + bloom filters) ---");
    let old_setup_start = Instant::now();

    let old_config = ParquetConfig {
        max_row_group_size: 1_000_000,
        bloom_filter_enabled: true,
    };
    let old_harness = TpchBenchmarkHarness::new_with_parquet_config(scale_factor, old_config).await?;
    println!("  Engine setup: {:.3}s", old_setup_start.elapsed().as_secs_f64());

    let _old_conn = old_harness.create_connection().await?;

    // Warmup: sync all tables
    println!("  Syncing tables...");
    for query in TPCH_QUERIES {
        let _ = old_harness.run_query(query).await;
    }
    let old_cache_size = TpchBenchmarkHarness::get_parquet_cache_size(old_harness.temp_dir.path());
    println!(
        "  Cache size: {:.2} MB",
        old_cache_size as f64 / 1024.0 / 1024.0
    );

    // Cached run
    println!("  Running queries (cached)...");
    let mut old_results = Vec::new();
    for query in TPCH_QUERIES {
        old_results.push(old_harness.run_query(query).await);
    }

    // ── CURRENT CONFIG: 100K row groups + 6 indexes ────────────
    println!("\n--- Setting up CURRENT config (100K row groups + indexes) ---");
    let new_setup_start = Instant::now();

    // Use default config (100K, no bloom)
    let new_harness = TpchBenchmarkHarness::new(scale_factor).await?;
    println!("  Engine setup: {:.3}s", new_setup_start.elapsed().as_secs_f64());

    let _new_conn = new_harness.create_connection().await?;

    // Warmup: sync all tables
    println!("  Syncing tables...");
    for query in TPCH_QUERIES {
        let _ = new_harness.run_query(query).await;
    }
    let new_cache_size = TpchBenchmarkHarness::get_parquet_cache_size(new_harness.temp_dir.path());
    println!(
        "  Cache size (before indexes): {:.2} MB",
        new_cache_size as f64 / 1024.0 / 1024.0
    );

    // Create 6 indexes
    println!("  Creating 6 indexes...");
    let indexes = [
        ("idx_shipdate", "tpch.main.lineitem", "(l_shipdate)"),
        ("idx_orderdate", "tpch.main.orders", "(o_orderdate)"),
        ("idx_partkey", "tpch.main.lineitem", "(l_partkey)"),
        ("idx_custkey", "tpch.main.orders", "(o_custkey)"),
        ("idx_nationkey", "tpch.main.customer", "(c_nationkey)"),
        ("idx_suppkey", "tpch.main.partsupp", "(ps_suppkey)"),
    ];

    for (name, table, cols) in &indexes {
        let sql = format!("CREATE INDEX {} ON {} {}", name, table, cols);
        let start = Instant::now();
        match new_harness.engine.execute_query(&sql).await {
            Ok(_) => println!(
                "    {} on {} {}: {:.2}ms",
                name,
                table,
                cols,
                start.elapsed().as_secs_f64() * 1000.0
            ),
            Err(e) => println!("    FAILED {}: {}", name, e),
        }
    }

    let new_cache_with_idx = TpchBenchmarkHarness::get_parquet_cache_size(new_harness.temp_dir.path());
    println!(
        "  Cache size (with indexes): {:.2} MB",
        new_cache_with_idx as f64 / 1024.0 / 1024.0
    );

    // Cached run
    println!("  Running queries (cached)...");
    let mut new_results = Vec::new();
    for query in TPCH_QUERIES {
        new_results.push(new_harness.run_query(query).await);
    }

    // ── COMPARISON TABLE ───────────────────────────────────────
    println!();
    println!("========================================================================");
    println!(
        "  Old (1M+bloom) vs Current (100K+indexes)  SF={}",
        scale_factor
    );
    println!("========================================================================");
    println!(
        "{:<8} {:>12} {:>12} {:>10} {:>8} {:>8}",
        "Query", "Old (ms)", "New (ms)", "Change", "Old Row", "New Row"
    );
    println!("{}", "-".repeat(70));

    let mut old_total = Duration::ZERO;
    let mut new_total = Duration::ZERO;
    let mut old_success = 0;
    let mut new_success = 0;

    for (i, query) in TPCH_QUERIES.iter().enumerate() {
        let old_ms = old_results[i].duration.as_secs_f64() * 1000.0;
        let new_ms = new_results[i].duration.as_secs_f64() * 1000.0;
        old_total += old_results[i].duration;
        new_total += new_results[i].duration;
        if old_results[i].success {
            old_success += 1;
        }
        if new_results[i].success {
            new_success += 1;
        }

        let delta = if old_ms > 0.0 {
            let pct = ((new_ms - old_ms) / old_ms) * 100.0;
            if pct < -1.0 {
                format!("{:.0}%", pct)
            } else if pct > 1.0 {
                format!("+{:.0}%", pct)
            } else {
                "~same".to_string()
            }
        } else {
            "N/A".to_string()
        };

        let old_flag = if old_results[i].success { "" } else { "*" };
        let new_flag = if new_results[i].success { "" } else { "*" };

        println!(
            "{:<8} {:>11.2}{} {:>11.2}{} {:>10} {:>8} {:>8}",
            query.name,
            old_ms,
            old_flag,
            new_ms,
            new_flag,
            delta,
            old_results[i].row_count,
            new_results[i].row_count,
        );
    }

    println!("{}", "-".repeat(70));
    let old_total_ms = old_total.as_secs_f64() * 1000.0;
    let new_total_ms = new_total.as_secs_f64() * 1000.0;
    let overall_speedup = old_total.as_secs_f64() / new_total.as_secs_f64();
    let overall_pct = ((new_total_ms - old_total_ms) / old_total_ms) * 100.0;
    println!(
        "{:<8} {:>12.0} {:>12.0} {:>9.0}%    {}/22    {}/22",
        "TOTAL", old_total_ms, new_total_ms, overall_pct, old_success, new_success,
    );
    println!(
        "Overall speedup: {:.2}x    Cache: {:.1}MB (old) vs {:.1}MB (new+idx)",
        overall_speedup,
        old_cache_size as f64 / 1024.0 / 1024.0,
        new_cache_with_idx as f64 / 1024.0 / 1024.0,
    );
    println!("========================================================================");

    // Verify correctness: row counts must match between configs
    for (i, _query) in TPCH_QUERIES.iter().enumerate() {
        if old_results[i].success && new_results[i].success {
            assert_eq!(
                old_results[i].row_count, new_results[i].row_count,
                "Row count mismatch for {}: old={} vs new={}",
                old_results[i].name, old_results[i].row_count, new_results[i].row_count
            );
        }
    }

    Ok(())
}

/// Pure row group size comparison: 1M (no bloom) vs 100K (no bloom), no indexes.
///
/// Run with: TPCH_SCALE_FACTOR=1.0 cargo test test_rowgroup_size_comparison --release -- --nocapture --test-threads=1
#[tokio::test(flavor = "multi_thread")]
async fn test_rowgroup_size_comparison() -> Result<()> {
    let scale_factor = get_scale_factor(0.1);

    println!("=== Row Group Size: 1M vs 100K (no bloom, no indexes) SF={} ===", scale_factor);

    // ── 1M row groups, no bloom ──────────────
    println!("\n--- Setting up 1M row groups (no bloom) ---");
    let old_start = Instant::now();

    let old_config = ParquetConfig {
        max_row_group_size: 1_000_000,
        bloom_filter_enabled: false,
    };
    let old_harness = TpchBenchmarkHarness::new_with_parquet_config(scale_factor, old_config).await?;
    println!("  Engine setup: {:.3}s", old_start.elapsed().as_secs_f64());

    let _old_conn = old_harness.create_connection().await?;

    println!("  Syncing tables...");
    for query in TPCH_QUERIES {
        let _ = old_harness.run_query(query).await;
    }
    let old_cache_size = TpchBenchmarkHarness::get_parquet_cache_size(old_harness.temp_dir.path());
    println!("  Cache size: {:.2} MB", old_cache_size as f64 / 1024.0 / 1024.0);

    println!("  Running queries (cached)...");
    let mut old_results = Vec::new();
    for query in TPCH_QUERIES {
        old_results.push(old_harness.run_query(query).await);
    }

    // ── 100K row groups, no bloom, NO indexes ──────────────
    println!("\n--- Setting up 100K row groups (no bloom, no indexes) ---");
    let new_start = Instant::now();

    let new_harness = TpchBenchmarkHarness::new(scale_factor).await?;
    println!("  Engine setup: {:.3}s", new_start.elapsed().as_secs_f64());

    let _new_conn = new_harness.create_connection().await?;

    println!("  Syncing tables...");
    for query in TPCH_QUERIES {
        let _ = new_harness.run_query(query).await;
    }
    let new_cache_size = TpchBenchmarkHarness::get_parquet_cache_size(new_harness.temp_dir.path());
    println!("  Cache size: {:.2} MB", new_cache_size as f64 / 1024.0 / 1024.0);

    println!("  Running queries (cached)...");
    let mut new_results = Vec::new();
    for query in TPCH_QUERIES {
        new_results.push(new_harness.run_query(query).await);
    }

    // ── COMPARISON ───────────────────────────────────────
    println!();
    println!("========================================================================");
    println!("  1M row groups vs 100K row groups (no bloom, no indexes)  SF={}", scale_factor);
    println!("========================================================================");
    println!(
        "{:<8} {:>12} {:>12} {:>14} {:>8} {:>8}",
        "Query", "1M (ms)", "100K (ms)", "Improvement", "1M Row", "100K Row"
    );
    println!("{}", "-".repeat(72));

    let mut old_total = Duration::ZERO;
    let mut new_total = Duration::ZERO;

    for (i, query) in TPCH_QUERIES.iter().enumerate() {
        let old_ms = old_results[i].duration.as_secs_f64() * 1000.0;
        let new_ms = new_results[i].duration.as_secs_f64() * 1000.0;
        old_total += old_results[i].duration;
        new_total += new_results[i].duration;

        let improvement = if old_ms > 0.0 {
            let pct = ((old_ms - new_ms) / old_ms) * 100.0;
            if pct > 1.0 {
                format!("{:.0}% faster", pct)
            } else if pct < -1.0 {
                format!("{:.0}% slower", -pct)
            } else {
                "~same".to_string()
            }
        } else {
            "N/A".to_string()
        };

        println!(
            "{:<8} {:>10.2}ms {:>10.2}ms {:>14} {:>8} {:>8}",
            query.name, old_ms, new_ms, improvement,
            old_results[i].row_count, new_results[i].row_count,
        );
    }

    println!("{}", "-".repeat(72));
    let old_total_ms = old_total.as_secs_f64() * 1000.0;
    let new_total_ms = new_total.as_secs_f64() * 1000.0;
    let speedup = old_total.as_secs_f64() / new_total.as_secs_f64();
    let pct = ((old_total_ms - new_total_ms) / old_total_ms) * 100.0;
    println!(
        "{:<8} {:>10.0}ms {:>10.0}ms {:>11.0}% faster",
        "TOTAL", old_total_ms, new_total_ms, pct,
    );
    println!(
        "Overall speedup: {:.2}x    Cache: {:.1}MB (1M) vs {:.1}MB (100K) = +{:.0}%",
        speedup,
        old_cache_size as f64 / 1024.0 / 1024.0,
        new_cache_size as f64 / 1024.0 / 1024.0,
        ((new_cache_size as f64 - old_cache_size as f64) / old_cache_size as f64) * 100.0,
    );
    println!("========================================================================");

    // Verify correctness
    for (i, _) in TPCH_QUERIES.iter().enumerate() {
        if old_results[i].success && new_results[i].success {
            assert_eq!(
                old_results[i].row_count, new_results[i].row_count,
                "Row count mismatch for {}", old_results[i].name
            );
        }
    }

    Ok(())
}
