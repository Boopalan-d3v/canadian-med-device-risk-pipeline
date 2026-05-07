CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS fact;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

CREATE OR REPLACE FUNCTION public.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS dim.dim_manufacturer (
    manufacturer_key        BIGSERIAL PRIMARY KEY,
    company_id              VARCHAR(100) NOT NULL,
    company_name            VARCHAR(500) NOT NULL,
    doing_business_as       VARCHAR(500),
    address_line1           VARCHAR(500),
    address_line2           VARCHAR(500),
    city                    VARCHAR(200),
    province_state          VARCHAR(200),
    postal_code             VARCHAR(20),
    country                 VARCHAR(100),
    phone                   VARCHAR(50),
    email                   VARCHAR(255),
    website                 VARCHAR(500),
    regulatory_status       VARCHAR(50),
    licence_holder_type     VARCHAR(100),
    source_system           VARCHAR(50),
    source_id               VARCHAR(100),
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_manufacturer_source UNIQUE (company_id, source_system)
);

CREATE TABLE IF NOT EXISTS dim.dim_device (
    device_key              BIGSERIAL PRIMARY KEY,
    device_id               VARCHAR(100) NOT NULL,
    licence_number          VARCHAR(50),
    trade_name              VARCHAR(500),
    generic_name            VARCHAR(500),
    risk_class              SMALLINT CHECK (risk_class BETWEEN 1 AND 4),
    risk_class_description  VARCHAR(100),
    device_type             VARCHAR(200),
    manufacturer_key        BIGINT REFERENCES dim.dim_manufacturer(manufacturer_key),
    licence_status          VARCHAR(50),
    first_issued_date       DATE,
    cancellation_date       DATE,
    is_active               BOOLEAN DEFAULT TRUE,
    source_system           VARCHAR(50) NOT NULL,
    source_id               VARCHAR(100),
    effective_date          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expiry_date             TIMESTAMP,
    is_current              BOOLEAN DEFAULT TRUE,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_device_source UNIQUE (device_id, source_system)
);

CREATE TABLE IF NOT EXISTS dim.dim_adverse_event_type (
    ae_type_key             BIGSERIAL PRIMARY KEY,
    meddra_code             VARCHAR(20),
    meddra_level            VARCHAR(50),
    term_name               VARCHAR(500) NOT NULL,
    term_name_lower         VARCHAR(500) GENERATED ALWAYS AS (LOWER(term_name)) STORED,
    parent_term_key         BIGINT REFERENCES dim.dim_adverse_event_type(ae_type_key),
    is_standardized         BOOLEAN DEFAULT FALSE,
    source_system           VARCHAR(50),
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_meddra_term UNIQUE (meddra_code, meddra_level)
);

CREATE TABLE IF NOT EXISTS dim.dim_outcome (
    outcome_key             BIGSERIAL PRIMARY KEY,
    outcome_code            VARCHAR(50),
    outcome_description     VARCHAR(500) NOT NULL,
    severity_level          SMALLINT CHECK (severity_level BETWEEN 1 AND 5),
    is_serious              BOOLEAN DEFAULT FALSE,
    is_fatal                BOOLEAN DEFAULT FALSE,
    source_system           VARCHAR(50),
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_outcome UNIQUE (outcome_code, source_system)
);

CREATE TABLE IF NOT EXISTS dim.dim_date (
    date_key                INTEGER PRIMARY KEY,
    full_date               DATE NOT NULL,
    day_of_week             SMALLINT,
    day_name                VARCHAR(20),
    day_of_month            SMALLINT,
    day_of_year             SMALLINT,
    week_of_year            SMALLINT,
    month_number            SMALLINT,
    month_name              VARCHAR(20),
    quarter                 SMALLINT,
    year                    INTEGER,
    is_weekend              BOOLEAN,
    is_holiday              BOOLEAN DEFAULT FALSE,
    fiscal_quarter          SMALLINT,
    fiscal_year             INTEGER
);

INSERT INTO dim.dim_date
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d,
    EXTRACT(DOW FROM d)::SMALLINT,
    TO_CHAR(d, 'Day'),
    EXTRACT(DAY FROM d)::SMALLINT,
    EXTRACT(DOY FROM d)::SMALLINT,
    EXTRACT(WEEK FROM d)::SMALLINT,
    EXTRACT(MONTH FROM d)::SMALLINT,
    TO_CHAR(d, 'Month'),
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(YEAR FROM d)::INTEGER,
    (EXTRACT(DOW FROM d) IN (0, 6)),
    FALSE,
    EXTRACT(QUARTER FROM d)::SMALLINT,
    CASE WHEN EXTRACT(MONTH FROM d) <= 3 THEN EXTRACT(YEAR FROM d) - 1 ELSE EXTRACT(YEAR FROM d) END::INTEGER
FROM generate_series('2020-01-01'::DATE, '2040-12-31'::DATE, '1 day'::INTERVAL) d
ON CONFLICT (date_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS fact.fact_adverse_event (
    adverse_event_key       BIGSERIAL PRIMARY KEY,
    report_id               VARCHAR(100) NOT NULL,
    report_number           VARCHAR(100),
    device_key              BIGINT REFERENCES dim.dim_device(device_key),
    date_received_key       INTEGER REFERENCES dim.dim_date(date_key),
    date_occurred_key       INTEGER REFERENCES dim.dim_date(date_key),
    outcome_key             BIGINT REFERENCES dim.dim_outcome(outcome_key),
    manufacturer_key        BIGINT REFERENCES dim.dim_manufacturer(manufacturer_key),
    patient_age             SMALLINT,
    patient_age_unit        VARCHAR(20),
    patient_gender          VARCHAR(20),
    is_serious              BOOLEAN DEFAULT FALSE,
    is_death                BOOLEAN DEFAULT FALSE,
    is_life_threatening     BOOLEAN DEFAULT FALSE,
    is_hospitalization      BOOLEAN DEFAULT FALSE,
    is_disability           BOOLEAN DEFAULT FALSE,
    report_type             VARCHAR(100),
    reporter_type           VARCHAR(100),
    source_country          VARCHAR(100),
    source_system           VARCHAR(50) NOT NULL,
    source_id               VARCHAR(100),
    raw_data                JSONB,
    extracted_at            TIMESTAMP NOT NULL,
    loaded_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_report_source UNIQUE (report_id, source_system)
);

CREATE TABLE IF NOT EXISTS fact.fact_adverse_event_reaction (
    ae_reaction_key         BIGSERIAL PRIMARY KEY,
    adverse_event_key       BIGINT NOT NULL REFERENCES fact.fact_adverse_event(adverse_event_key) ON DELETE CASCADE,
    ae_type_key             BIGINT NOT NULL REFERENCES dim.dim_adverse_event_type(ae_type_key),
    reaction_sequence       SMALLINT,
    reaction_outcome        VARCHAR(200),
    seriousness             VARCHAR(100),
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_ae_reaction UNIQUE (adverse_event_key, ae_type_key, reaction_sequence)
);

CREATE TABLE IF NOT EXISTS fact.fact_recall (
    recall_key              BIGSERIAL PRIMARY KEY,
    recall_id               VARCHAR(100) NOT NULL,
    recall_number           VARCHAR(100),
    device_key              BIGINT REFERENCES dim.dim_device(device_key),
    recall_date_key         INTEGER REFERENCES dim.dim_date(date_key),
    manufacturer_key        BIGINT REFERENCES dim.dim_manufacturer(manufacturer_key),
    recall_class            VARCHAR(20),
    recall_class_numeric    SMALLINT CHECK (recall_class_numeric BETWEEN 1 AND 3),
    recall_reason           TEXT,
    recall_type             VARCHAR(100),
    recall_status           VARCHAR(50),
    product_description     TEXT,
    product_codes           TEXT[],
    lot_numbers             TEXT[],
    corrective_action       TEXT,
    instructions_to_consumer TEXT,
    health_hazard_description TEXT,
    risk_level_score        DECIMAL(5,2),
    source_system           VARCHAR(50) NOT NULL,
    source_id               VARCHAR(100),
    source_url              VARCHAR(1000),
    raw_data                JSONB,
    extracted_at            TIMESTAMP NOT NULL,
    loaded_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_recall_source UNIQUE (recall_id, source_system)
);

CREATE TABLE IF NOT EXISTS fact.fact_device_risk_score (
    risk_score_key          BIGSERIAL PRIMARY KEY,
    device_key              BIGINT NOT NULL REFERENCES dim.dim_device(device_key),
    calculation_date_key    INTEGER NOT NULL REFERENCES dim.dim_date(date_key),
    composite_risk_score    DECIMAL(5,2) NOT NULL CHECK (composite_risk_score BETWEEN 0 AND 10),
    risk_category           VARCHAR(20) CHECK (risk_category IN ('MINIMAL', 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    adverse_event_score     DECIMAL(5,2) DEFAULT 0,
    recall_score            DECIMAL(5,2) DEFAULT 0,
    device_class_score      SMALLINT DEFAULT 0,
    manufacturer_score      DECIMAL(5,2) DEFAULT 0,
    total_adverse_events    INTEGER DEFAULT 0,
    serious_adverse_events  INTEGER DEFAULT 0,
    death_events            INTEGER DEFAULT 0,
    total_recalls           INTEGER DEFAULT 0,
    class_i_recalls         INTEGER DEFAULT 0,
    class_ii_recalls        INTEGER DEFAULT 0,
    class_iii_recalls       INTEGER DEFAULT 0,
    analysis_window_days    INTEGER DEFAULT 365,
    window_start_date       DATE,
    window_end_date         DATE,
    calculation_method      VARCHAR(100),
    model_version           VARCHAR(50),
    confidence_score        DECIMAL(3,2),
    source_system           VARCHAR(50) DEFAULT 'RISK_ENGINE',
    calculated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_device_date UNIQUE (device_key, calculation_date_key)
);

CREATE TABLE IF NOT EXISTS staging.bridge_device_recall (
    bridge_key              BIGSERIAL PRIMARY KEY,
    device_key              BIGINT NOT NULL REFERENCES dim.dim_device(device_key),
    recall_key              BIGINT NOT NULL REFERENCES fact.fact_recall(recall_key),
    match_confidence        DECIMAL(3,2) DEFAULT 1.0,
    match_method            VARCHAR(50),
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_device_recall UNIQUE (device_key, recall_key)
);

CREATE TABLE IF NOT EXISTS audit.etl_run_log (
    run_id                  BIGSERIAL PRIMARY KEY,
    pipeline_name           VARCHAR(100) NOT NULL,
    dag_id                  VARCHAR(100),
    task_id                 VARCHAR(100),
    execution_date          TIMESTAMP NOT NULL,
    start_time              TIMESTAMP NOT NULL,
    end_time                TIMESTAMP,
    status                  VARCHAR(20) CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'RETRY')),
    records_extracted       INTEGER DEFAULT 0,
    records_transformed     INTEGER DEFAULT 0,
    records_loaded          INTEGER DEFAULT 0,
    records_failed          INTEGER DEFAULT 0,
    error_message           TEXT,
    warning_message         TEXT,
    metadata                JSONB,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit.data_quality_log (
    dq_check_id             BIGSERIAL PRIMARY KEY,
    run_id                  BIGINT REFERENCES audit.etl_run_log(run_id),
    table_name              VARCHAR(100) NOT NULL,
    column_name             VARCHAR(100),
    check_type              VARCHAR(50) NOT NULL,
    check_description       TEXT,
    records_checked         INTEGER,
    records_failed          INTEGER,
    failure_rate            DECIMAL(5,2),
    severity                VARCHAR(20),
    status                  VARCHAR(20) DEFAULT 'PASS',
    failed_sample           JSONB,
    checked_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit.data_lineage (
    lineage_id              BIGSERIAL PRIMARY KEY,
    source_system           VARCHAR(100) NOT NULL,
    source_table            VARCHAR(100),
    source_record_id        VARCHAR(200),
    target_schema           VARCHAR(50) NOT NULL,
    target_table            VARCHAR(100) NOT NULL,
    target_record_id        BIGINT,
    transformation_rule     TEXT,
    extracted_at            TIMESTAMP,
    loaded_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_hash               VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS raw.raw_mdall_licence (
    raw_licence_key         BIGSERIAL PRIMARY KEY,
    source_record_id        VARCHAR(200) NOT NULL,
    source_system           VARCHAR(50) NOT NULL DEFAULT 'MDALL',
    payload                 JSONB NOT NULL,
    extracted_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    loaded_at               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_raw_mdall_licence UNIQUE (source_system, source_record_id)
);

CREATE TABLE IF NOT EXISTS raw.raw_mdall_company (
    raw_company_key         BIGSERIAL PRIMARY KEY,
    source_record_id        VARCHAR(200) NOT NULL,
    source_system           VARCHAR(50) NOT NULL DEFAULT 'MDALL',
    payload                 JSONB NOT NULL,
    extracted_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    loaded_at               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_raw_mdall_company UNIQUE (source_system, source_record_id)
);

CREATE TABLE IF NOT EXISTS raw.raw_recall_listing (
    raw_recall_key          BIGSERIAL PRIMARY KEY,
    source_record_id        VARCHAR(200) NOT NULL,
    source_system           VARCHAR(50) NOT NULL DEFAULT 'HC_RECALLS',
    payload                 JSONB NOT NULL,
    extracted_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    loaded_at               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_raw_recall_listing UNIQUE (source_system, source_record_id)
);

CREATE TABLE IF NOT EXISTS raw.raw_canada_vigilance_report (
    raw_vigilance_key       BIGSERIAL PRIMARY KEY,
    source_record_id        VARCHAR(200) NOT NULL,
    source_system           VARCHAR(50) NOT NULL DEFAULT 'CANADA_VIGILANCE',
    payload                 JSONB NOT NULL,
    extracted_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    loaded_at               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_raw_canada_vigilance_report UNIQUE (source_system, source_record_id)
);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_device_risk_summary AS
SELECT
    d.device_key,
    d.device_id,
    d.trade_name,
    d.licence_number,
    d.risk_class,
    m.company_name AS manufacturer_name,
    m.country,
    COALESCE(r.composite_risk_score, 0) AS current_risk_score,
    r.risk_category,
    r.calculation_date_key,
    COUNT(DISTINCT ae.adverse_event_key) AS total_ae,
    COUNT(DISTINCT rec.recall_key) AS total_recalls,
    MAX(ae.extracted_at) AS last_ae_date,
    MAX(rec.extracted_at) AS last_recall_date,
    CURRENT_TIMESTAMP AS refresh_time
FROM dim.dim_device d
LEFT JOIN dim.dim_manufacturer m ON d.manufacturer_key = m.manufacturer_key
LEFT JOIN fact.fact_device_risk_score r ON d.device_key = r.device_key
LEFT JOIN fact.fact_adverse_event ae ON d.device_key = ae.device_key
LEFT JOIN fact.fact_recall rec ON d.device_key = rec.device_key
WHERE d.is_active = TRUE
GROUP BY
    d.device_key, d.device_id, d.trade_name, d.licence_number, d.risk_class,
    m.company_name, m.country, r.composite_risk_score, r.risk_category, r.calculation_date_key;

CREATE INDEX IF NOT EXISTS idx_device_licence ON dim.dim_device(licence_number);
CREATE INDEX IF NOT EXISTS idx_device_risk_class ON dim.dim_device(risk_class);
CREATE INDEX IF NOT EXISTS idx_manufacturer_name ON dim.dim_manufacturer(company_name);
CREATE INDEX IF NOT EXISTS idx_ae_term_name ON dim.dim_adverse_event_type(term_name_lower);
CREATE INDEX IF NOT EXISTS idx_fact_ae_device_date ON fact.fact_adverse_event(device_key, date_received_key);
CREATE INDEX IF NOT EXISTS idx_fact_recall_device_date ON fact.fact_recall(device_key, recall_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_risk_score ON fact.fact_device_risk_score(composite_risk_score DESC);
CREATE INDEX IF NOT EXISTS idx_run_log_pipeline_date ON audit.etl_run_log(pipeline_name, execution_date DESC);

DROP TRIGGER IF EXISTS trg_dim_manufacturer_updated_at ON dim.dim_manufacturer;
CREATE TRIGGER trg_dim_manufacturer_updated_at
BEFORE UPDATE ON dim.dim_manufacturer
FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();

DROP TRIGGER IF EXISTS trg_dim_device_updated_at ON dim.dim_device;
CREATE TRIGGER trg_dim_device_updated_at
BEFORE UPDATE ON dim.dim_device
FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();
