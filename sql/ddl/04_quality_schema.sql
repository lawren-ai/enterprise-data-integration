-- =====================================================
-- DATA QUALITY FRAMEWORK SCHEMA
-- Tracks validation rules, results, and exceptions
-- =====================================================

-- Drop existing quality tables if they exist
DROP TABLE IF EXISTS dq_test_results CASCADE;
DROP TABLE IF EXISTS dq_exceptions CASCADE;
DROP TABLE IF EXISTS dq_rules CASCADE;
DROP TABLE IF EXISTS dq_rule_categories CASCADE;
DROP TABLE IF EXISTS dq_scorecards CASCADE;

-- =====================================================
-- Rule Categories (Dimension table for rule types)
-- =====================================================
CREATE TABLE dq_rule_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL UNIQUE,
    category_description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dq_rule_categories IS 'Categories of data quality rules';

-- Insert standard quality dimensions
INSERT INTO dq_rule_categories (category_name, category_description) VALUES
('Completeness', 'Checks for missing or null values in critical fields'),
('Accuracy', 'Validates data matches expected formats and ranges'),
('Consistency', 'Ensures data is consistent across systems and tables'),
('Validity', 'Confirms data conforms to business rules and constraints'),
('Uniqueness', 'Detects duplicate records'),
('Timeliness', 'Monitors data freshness and SLA compliance'),
('Integrity', 'Validates referential integrity between tables');

-- =====================================================
-- Data Quality Rules (Rule definitions)
-- =====================================================
CREATE TABLE dq_rules (
    rule_id SERIAL PRIMARY KEY,
    rule_name VARCHAR(200) NOT NULL,
    rule_description TEXT,
    category_id INTEGER REFERENCES dq_rule_categories(category_id),
    
    -- Target information
    target_schema VARCHAR(50),
    target_table VARCHAR(100) NOT NULL,
    target_column VARCHAR(100),
    
    -- Rule definition
    rule_type VARCHAR(50) NOT NULL, -- not_null, unique, range, regex, custom_sql, etc.
    rule_sql TEXT, -- SQL query that returns rows violating the rule
    rule_parameters JSON, -- Additional parameters (min, max, pattern, etc.)
    
    -- Thresholds
    severity VARCHAR(20) DEFAULT 'WARNING', -- CRITICAL, WARNING, INFO
    failure_threshold DECIMAL(5,2) DEFAULT 0.0, -- % of failures allowed (0-100)
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    created_by VARCHAR(100) DEFAULT 'system',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dq_rules IS 'Data quality rule definitions';

CREATE INDEX idx_dq_rules_table ON dq_rules(target_table);
CREATE INDEX idx_dq_rules_active ON dq_rules(is_active);
CREATE INDEX idx_dq_rules_severity ON dq_rules(severity);

-- =====================================================
-- Data Quality Test Results (Execution history)
-- =====================================================
CREATE TABLE dq_test_results (
    result_id BIGSERIAL PRIMARY KEY,
    rule_id INTEGER REFERENCES dq_rules(rule_id),
    
    -- Execution details
    execution_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_duration_ms INTEGER,
    
    -- Results
    total_records_checked BIGINT,
    failed_records BIGINT,
    passed_records BIGINT,
    failure_percentage DECIMAL(5,2),
    
    -- Status
    test_status VARCHAR(20), -- PASSED, FAILED, WARNING, ERROR
    test_message TEXT,
    
    -- Metadata
    executed_by VARCHAR(100) DEFAULT 'system'
);

COMMENT ON TABLE dq_test_results IS 'Historical results of data quality test executions';

CREATE INDEX idx_dq_results_rule ON dq_test_results(rule_id);
CREATE INDEX idx_dq_results_date ON dq_test_results(execution_date);
CREATE INDEX idx_dq_results_status ON dq_test_results(test_status);

-- =====================================================
-- Data Quality Exceptions (Failed records)
-- =====================================================
CREATE TABLE dq_exceptions (
    exception_id BIGSERIAL PRIMARY KEY,
    result_id BIGINT REFERENCES dq_test_results(result_id),
    rule_id INTEGER REFERENCES dq_rules(rule_id),
    
    -- Failed record details
    table_name VARCHAR(100),
    record_identifier VARCHAR(500), -- Primary key or unique identifier
    column_name VARCHAR(100),
    failed_value TEXT,
    expected_value TEXT,
    
    -- Issue tracking
    detected_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_date TIMESTAMP,
    resolution_status VARCHAR(20) DEFAULT 'OPEN', -- OPEN, IN_PROGRESS, RESOLVED, IGNORED
    resolution_notes TEXT,
    resolved_by VARCHAR(100),
    
    -- Metadata
    exception_hash VARCHAR(64) -- MD5 hash to detect recurring issues
);

COMMENT ON TABLE dq_exceptions IS 'Detailed log of data quality violations';

CREATE INDEX idx_dq_exceptions_rule ON dq_exceptions(rule_id);
CREATE INDEX idx_dq_exceptions_status ON dq_exceptions(resolution_status);
CREATE INDEX idx_dq_exceptions_table ON dq_exceptions(table_name);
CREATE INDEX idx_dq_exceptions_date ON dq_exceptions(detected_date);
CREATE INDEX idx_dq_exceptions_hash ON dq_exceptions(exception_hash);

-- =====================================================
-- Data Quality Scorecards (Summary metrics)
-- =====================================================
CREATE TABLE dq_scorecards (
    scorecard_id SERIAL PRIMARY KEY,
    
    -- Time period
    report_date DATE NOT NULL,
    report_period VARCHAR(20), -- DAILY, WEEKLY, MONTHLY
    
    -- Overall metrics
    total_rules_executed INTEGER,
    rules_passed INTEGER,
    rules_failed INTEGER,
    rules_warning INTEGER,
    
    -- Quality scores (0-100)
    overall_quality_score DECIMAL(5,2),
    completeness_score DECIMAL(5,2),
    accuracy_score DECIMAL(5,2),
    consistency_score DECIMAL(5,2),
    validity_score DECIMAL(5,2),
    uniqueness_score DECIMAL(5,2),
    timeliness_score DECIMAL(5,2),
    integrity_score DECIMAL(5,2),
    
    -- Record counts
    total_records_checked BIGINT,
    total_failed_records BIGINT,
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(report_date, report_period)
);

COMMENT ON TABLE dq_scorecards IS 'Aggregated data quality metrics by time period';

CREATE INDEX idx_dq_scorecards_date ON dq_scorecards(report_date);
CREATE INDEX idx_dq_scorecards_period ON dq_scorecards(report_period);

-- =====================================================
-- Views for Easy Access
-- =====================================================

-- Active rules summary
CREATE OR REPLACE VIEW vw_active_rules AS
SELECT 
    r.rule_id,
    r.rule_name,
    c.category_name,
    r.target_table,
    r.target_column,
    r.severity,
    r.is_active,
    COUNT(tr.result_id) as execution_count,
    MAX(tr.execution_date) as last_execution
FROM dq_rules r
LEFT JOIN dq_rule_categories c ON r.category_id = c.category_id
LEFT JOIN dq_test_results tr ON r.rule_id = tr.rule_id
WHERE r.is_active = TRUE
GROUP BY r.rule_id, r.rule_name, c.category_name, r.target_table, 
         r.target_column, r.severity, r.is_active;

-- Latest quality scores
CREATE OR REPLACE VIEW vw_latest_quality_scores AS
SELECT 
    report_date,
    report_period,
    overall_quality_score,
    completeness_score,
    accuracy_score,
    consistency_score,
    validity_score,
    uniqueness_score,
    timeliness_score,
    integrity_score,
    total_rules_executed,
    rules_passed,
    rules_failed,
    rules_warning
FROM dq_scorecards
WHERE report_date = (SELECT MAX(report_date) FROM dq_scorecards);

-- Open exceptions summary
CREATE OR REPLACE VIEW vw_open_exceptions AS
SELECT 
    e.exception_id,
    r.rule_name,
    c.category_name,
    e.table_name,
    e.column_name,
    e.resolution_status,
    e.detected_date,
    CURRENT_DATE - e.detected_date::date as days_open
FROM dq_exceptions e
JOIN dq_rules r ON e.rule_id = r.rule_id
JOIN dq_rule_categories c ON r.category_id = c.category_id
WHERE e.resolution_status IN ('OPEN', 'IN_PROGRESS')
ORDER BY e.detected_date;