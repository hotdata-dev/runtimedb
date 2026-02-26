-- Add classification to saved query versions
ALTER TABLE saved_query_versions ADD COLUMN category TEXT;

-- Add user metadata to saved queries
ALTER TABLE saved_queries ADD COLUMN tags TEXT NOT NULL DEFAULT '[]';
ALTER TABLE saved_queries ADD COLUMN description TEXT NOT NULL DEFAULT '';

-- Add rich classification fields to saved query versions
ALTER TABLE saved_query_versions ADD COLUMN num_tables INTEGER;
ALTER TABLE saved_query_versions ADD COLUMN has_predicate INTEGER;
ALTER TABLE saved_query_versions ADD COLUMN has_join INTEGER;
ALTER TABLE saved_query_versions ADD COLUMN has_aggregation INTEGER;
ALTER TABLE saved_query_versions ADD COLUMN has_group_by INTEGER;
ALTER TABLE saved_query_versions ADD COLUMN has_order_by INTEGER;
ALTER TABLE saved_query_versions ADD COLUMN has_limit INTEGER;
ALTER TABLE saved_query_versions ADD COLUMN category_override TEXT;
ALTER TABLE saved_query_versions ADD COLUMN table_size_override TEXT;
