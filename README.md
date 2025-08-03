# EMR Data Pipeline - Caregiver Analytics Platform

A comprehensive data analytics platform for healthcare agencies to monitor caregiver performance, identify reliability issues, and optimize workforce management through advanced SQL analytics.

## Schema Design Rationale

### Database Architecture Overview
The schema is designed with a **normalized structure** to handle multi-tenant healthcare data while maintaining data integrity and query performance. The design addresses several key complexities:

#### Multi-Agency Support
- **Franchisor → Agency → Caregiver hierarchy**: Supports healthcare franchises with multiple agencies
- **Composite unique constraints**: `[agencyId, franchisorId]` ensures data isolation between agencies
- **Flexible location management**: Optional location tracking for agencies with multiple facilities

#### Caregiver Performance Tracking
- **Comprehensive time tracking**: Separate scheduled vs actual clock-in/out times to calculate overtime
- **Documentation quality metrics**: `generalCommentCharCount` for automated quality assessment
- **Status tracking**: Multiple status fields for applicant, caregiver, and visit states

#### Data Normalization Benefits
- **Eliminates redundancy**: Agency and location data normalized to prevent duplication
- **Referential integrity**: Foreign key constraints ensure data consistency
- **Scalable relationships**: Easy to add new agencies, locations, or caregiver attributes

### Key Design Decisions

1. **CamelCase naming convention**: Aligns with modern JavaScript/TypeScript practices
2. **UUID primary keys**: Ensures uniqueness across distributed systems
3. **Nullable fields**: Handles incomplete data gracefully (e.g., optional email/phone)
4. **Timestamp precision**: Uses DateTime for accurate time tracking and analytics

## Assumptions & Edge Cases

### Data Quality Assumptions

#### Reliability Issues Definition
- **Absenteeism**: Missing both clock-in and clock-out for scheduled visits
- **Late arrivals**: Clock-in time > scheduled start time
- **Early departures**: Clock-out time < scheduled end time
- **Completion rate**: At least one clock event (in or out) recorded

#### Time Handling Assumptions
- **Valid time ranges**: Clock-out must be after clock-in
- **Scheduled vs actual**: Scheduled times may differ from actual times
- **Overnight shifts**: Handled by comparing dates, not just times
- **Missing timestamps**: Treated as incomplete visits, not errors

#### Documentation Quality Metrics
- **Character count validation**: `generalCommentCharCount` must match actual text length
- **Minimum thresholds**: Comments < 5 characters flagged as suspicious
- **Generic content detection**: Identifies copy-paste or template usage

### Edge Case Handling

#### ETL Pipeline Robustness
```typescript
// Data validation in transformers
- Null timestamp handling
- Invalid date range detection
- Duplicate record identification
- Character encoding issues
```

#### Query-Level Safeguards
- **NULLIF() functions**: Prevent division by zero in percentage calculations
- **GREATEST() functions**: Ensure overtime calculations don't go negative
- **CASE statements**: Handle missing or invalid data gracefully
- **Window functions**: Provide context for outlier detection

### Ambiguous Data Resolution

1. **Inconsistent timestamps**: Use actual times when available, fall back to scheduled
2. **Missing documentation**: Treat as quality issue, not data error
3. **Partial visits**: Count as completed if at least one clock event exists
4. **Duplicate records**: Use latest timestamp or most complete record

## Scalability & Performance

### Database Performance Optimizations

#### Indexing Strategy
```sql
-- Primary performance indexes
CREATE INDEX idx_caregiver_franchisor_agency ON Caregiver(franchisorId, agencyId);
CREATE INDEX idx_carelog_caregiver_time ON Carelog(caregiverId, clockInActualDatetime);
CREATE INDEX idx_carelog_scheduled_time ON Carelog(startDatetime, endDatetime);
CREATE INDEX idx_carelog_status ON Carelog(status);
```

#### Query Optimization Techniques
- **CTEs for complex analytics**: Break down complex queries into manageable parts
- **Window functions**: Efficient ranking and percentile calculations
- **Aggregation optimization**: Pre-calculate metrics at caregiver level
- **Partitioning strategy**: Consider date-based partitioning for large datasets

### Large Dataset Considerations

#### Performance Scaling
- **Batch processing**: Process data in chunks to manage memory usage
- **Incremental updates**: Only process new/modified records
- **Materialized views**: Pre-compute common analytics queries
- **Query result caching**: Cache frequently accessed analytics

#### Memory and Storage
- **Efficient data types**: Use appropriate field sizes (VARCHAR vs TEXT)
- **Compression**: Enable database compression for historical data
- **Archiving strategy**: Move old data to separate storage
- **Connection pooling**: Manage database connections efficiently

### Key Performance Trade-offs

1. **Normalization vs Denormalization**
   - **Chose normalization**: Better data integrity, slightly more complex queries
   - **Trade-off**: Additional JOINs but cleaner data structure

2. **Real-time vs Batch Processing**
   - **Batch approach**: Better for complex analytics, reduced system load
   - **Trade-off**: Slight delay in data availability

3. **Indexing Strategy**
   - **Selective indexing**: Balance query performance with write overhead
   - **Trade-off**: More indexes = faster reads, slower writes

## Analytics Capabilities

### Core Analytics Modules

1. **Reliability Issues Analysis** (`ReliabilityIssues.sql`)
   - Absenteeism rate calculation
   - Late arrival/early departure patterns
   - Severity scoring and risk assessment

2. **Overtime Pattern Analysis** (`OvertimeInsight.sql`)
   - Multi-dimensional overtime analysis
   - Time-based pattern recognition
   - Risk scoring and intervention recommendations

3. **Documentation Quality Assessment** (`DataQualityCheck.sql`)
   - Automated quality scoring
   - Suspicious pattern detection
   - Professionalism metrics

4. **Performance Analytics** (`TopPerformance.sql`)
   - Completion rate analysis
   - Work efficiency metrics
   - Punctuality assessment

### Advanced Features

- **Statistical outlier detection**: Z-scores and percentile-based analysis
- **Temporal pattern recognition**: Day-of-week, time-of-day analysis
- **Risk scoring algorithms**: Multi-factor weighted scoring
- **Automated insights**: Natural language descriptions of patterns

## Project Structure

```
emr-data-pipeline/
├── prisma/                 # Database schema and migrations
├── src/                    # TypeScript source code
│   ├── service/           # ETL and data processing services
│   ├── transformers/      # Data transformation logic
│   └── utils/             # Utility functions
├── sql/                   # Analytics queries
├── data/                  # CSV data files
└── README.md             # This file
```

## Getting Started

1. **Setup Database**: Run Prisma migrations
2. **Load Data**: Place CSV files in `data/` directory
3. **Run ETL**: Execute the data processing pipeline
4. **Run Analytics**: Execute SQL queries in `sql/` folder

## Technology Stack

- **Database**: PostgreSQL with Prisma ORM
- **Backend**: Node.js with TypeScript
- **Analytics**: Advanced SQL with window functions
- **Data Processing**: Custom ETL pipeline with validation

This platform provides healthcare agencies with comprehensive insights into caregiver performance, enabling data-driven workforce management and quality improvement initiatives.