WITH overtime_analysis AS (
    SELECT 
        c."caregiverId",
        c."firstName" || ' ' || c."lastName" AS caregiver_name,
        c.email,
        c."phoneNumber",
        c.status,
        cl."carelogId",
        
        cl."startDatetime" AS scheduled_start,
        cl."endDatetime" AS scheduled_end,
        cl."clockInActualDatetime" AS actual_start,
        cl."clockOutActualDatetime" AS actual_end,
        DATE(cl."clockInActualDatetime") AS visit_date,
        EXTRACT(DOW FROM cl."clockInActualDatetime") AS day_of_week,
        EXTRACT(HOUR FROM cl."clockInActualDatetime") AS start_hour,
        EXTRACT(MONTH FROM cl."clockInActualDatetime") AS visit_month,
        
        CASE 
            WHEN cl."startDatetime" IS NOT NULL AND cl."endDatetime" IS NOT NULL
            THEN EXTRACT(EPOCH FROM (cl."endDatetime" - cl."startDatetime"))/3600
            ELSE NULL 
        END AS scheduled_hours,
        
        CASE 
            WHEN cl."clockInActualDatetime" IS NOT NULL 
                 AND cl."clockOutActualDatetime" IS NOT NULL
                 AND cl."clockInActualDatetime" < cl."clockOutActualDatetime"
            THEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/3600
            ELSE NULL 
        END AS actual_hours,
        
        CASE 
            WHEN cl."startDatetime" IS NOT NULL AND cl."endDatetime" IS NOT NULL
                 AND cl."clockInActualDatetime" IS NOT NULL 
                 AND cl."clockOutActualDatetime" IS NOT NULL
                 AND cl."clockInActualDatetime" < cl."clockOutActualDatetime"
            THEN GREATEST(0, EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/3600 - 
                              EXTRACT(EPOCH FROM (cl."endDatetime" - cl."startDatetime"))/3600)
            ELSE NULL 
        END AS overtime_hours,
        
        CASE 
            WHEN cl."startDatetime" IS NOT NULL AND cl."endDatetime" IS NOT NULL
                 AND cl."clockInActualDatetime" IS NOT NULL 
                 AND cl."clockOutActualDatetime" IS NOT NULL
                 AND cl."clockInActualDatetime" < cl."clockOutActualDatetime"
                 AND EXTRACT(EPOCH FROM (cl."endDatetime" - cl."startDatetime"))/3600 > 0
            THEN ROUND(100.0 * GREATEST(0, EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/3600 - 
                                              EXTRACT(EPOCH FROM (cl."endDatetime" - cl."startDatetime"))/3600) / 
                       EXTRACT(EPOCH FROM (cl."endDatetime" - cl."startDatetime"))/3600, 2)
            ELSE NULL 
        END AS overtime_percentage,
        
        CASE 
            WHEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/3600 > 12
            THEN 'EXTENDED_SHIFT'
            
            WHEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/3600 > 8
            THEN 'LONG_SHIFT'
            
            WHEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/3600 > 4
            THEN 'NORMAL_SHIFT'
            
            ELSE 'SHORT_SHIFT'
        END AS shift_type,
        
        CASE 
            WHEN EXTRACT(HOUR FROM cl."clockInActualDatetime") < 6
            THEN 'EARLY_MORNING'
            
            WHEN EXTRACT(HOUR FROM cl."clockInActualDatetime") < 12
            THEN 'MORNING'
            
            WHEN EXTRACT(HOUR FROM cl."clockInActualDatetime") < 18
            THEN 'AFTERNOON'
            
            WHEN EXTRACT(HOUR FROM cl."clockInActualDatetime") < 24
            THEN 'EVENING'
            
            ELSE 'NIGHT'
        END AS time_period,
        
        CASE 
            WHEN EXTRACT(DOW FROM cl."clockInActualDatetime") IN (0, 6)
            THEN 'WEEKEND'
            ELSE 'WEEKDAY'
        END AS day_type
        
    FROM 
        "Caregiver" c
    JOIN 
        "Carelog" cl ON c."caregiverId" = cl."caregiverId"
    WHERE 
        cl."clockInActualDatetime" IS NOT NULL 
        AND cl."clockOutActualDatetime" IS NOT NULL
        AND cl."clockInActualDatetime" < cl."clockOutActualDatetime"
        AND cl."startDatetime" IS NOT NULL 
        AND cl."endDatetime" IS NOT NULL
        AND cl."startDatetime" < cl."endDatetime"
),

caregiver_overtime_patterns AS (
    SELECT 
        "caregiverId",
        caregiver_name,
        email,
        "phoneNumber",
        status,
        
        COUNT(*) AS total_visits,
        COUNT(CASE WHEN overtime_hours > 0 THEN 1 END) AS overtime_visits,
        COUNT(CASE WHEN overtime_hours = 0 THEN 1 END) AS on_time_visits,
        
        ROUND(AVG(CASE WHEN overtime_hours > 0 THEN overtime_hours END), 2) AS avg_overtime_hours,
        ROUND(MAX(overtime_hours), 2) AS max_overtime_hours,
        ROUND(SUM(overtime_hours), 2) AS total_overtime_hours,
        ROUND(AVG(CASE WHEN overtime_percentage > 0 THEN overtime_percentage END), 2) AS avg_overtime_percentage,
        
        ROUND(100.0 * COUNT(CASE WHEN overtime_hours > 0 THEN 1 END) / COUNT(*), 2) AS overtime_frequency,
        ROUND(100.0 * COUNT(CASE WHEN overtime_hours > 2 THEN 1 END) / COUNT(*), 2) AS severe_overtime_frequency,
        ROUND(100.0 * COUNT(CASE WHEN overtime_hours > 4 THEN 1 END) / COUNT(*), 2) AS extreme_overtime_frequency,
        
        COUNT(CASE WHEN day_type = 'WEEKEND' THEN 1 END) AS weekend_visits,
        COUNT(CASE WHEN day_type = 'WEEKEND' AND overtime_hours > 0 THEN 1 END) AS weekend_overtime,
        ROUND(100.0 * COUNT(CASE WHEN day_type = 'WEEKEND' AND overtime_hours > 0 THEN 1 END) / 
              NULLIF(COUNT(CASE WHEN day_type = 'WEEKEND' THEN 1 END), 0), 2) AS weekend_overtime_rate,
        
        COUNT(CASE WHEN shift_type = 'EXTENDED_SHIFT' THEN 1 END) AS extended_shifts,
        COUNT(CASE WHEN shift_type = 'LONG_SHIFT' THEN 1 END) AS long_shifts,
        COUNT(CASE WHEN shift_type = 'NORMAL_SHIFT' THEN 1 END) AS normal_shifts,
        COUNT(CASE WHEN shift_type = 'SHORT_SHIFT' THEN 1 END) AS short_shifts,
        
        COUNT(CASE WHEN time_period = 'EARLY_MORNING' AND overtime_hours > 0 THEN 1 END) AS early_morning_overtime,
        COUNT(CASE WHEN time_period = 'EVENING' AND overtime_hours > 0 THEN 1 END) AS evening_overtime,
        COUNT(CASE WHEN time_period = 'NIGHT' AND overtime_hours > 0 THEN 1 END) AS night_overtime,
        
        COUNT(CASE WHEN overtime_hours > 0 THEN 1 END) AS consecutive_overtime_days,
        
        MIN(visit_date) AS first_visit_date,
        MAX(visit_date) AS last_visit_date,
        COUNT(DISTINCT visit_date) AS active_days,
        COUNT(DISTINCT visit_month) AS active_months
        
    FROM overtime_analysis
    
    GROUP BY "caregiverId", caregiver_name, email, "phoneNumber", status
    
    HAVING COUNT(*) >= 5
),

overtime_insights AS (
    SELECT 
        *,
        
        ROUND(
            (overtime_frequency * 0.3) + 
            (GREATEST(0, avg_overtime_hours - 1) * 15) + 
            (severe_overtime_frequency * 0.4) + 
            (weekend_overtime_rate * 0.2) +
            (GREATEST(0, max_overtime_hours - 4) * 3),
            2
        ) AS overtime_risk_score,
        
        CASE 
            WHEN overtime_frequency >= 60 AND avg_overtime_hours >= 2
            THEN 'CHRONIC_OVERTIME_WORKER'
            
            WHEN overtime_frequency >= 40 AND avg_overtime_hours >= 1.5
            THEN 'FREQUENT_OVERTIME_WORKER'
            
            WHEN severe_overtime_frequency >= 30
            THEN 'SEVERE_OVERTIME_PATTERN'
            
            WHEN weekend_overtime_rate >= 50
            THEN 'WEEKEND_OVERTIME_SPECIALIST'
            
            WHEN extended_shifts > 0 AND extended_shifts >= total_visits * 0.2
            THEN 'EXTENDED_SHIFT_WORKER'
            
            WHEN evening_overtime > 0 AND evening_overtime >= overtime_visits * 0.5
            THEN 'EVENING_OVERTIME_PATTERN'
            
            WHEN night_overtime > 0 AND night_overtime >= overtime_visits * 0.3
            THEN 'NIGHT_OVERTIME_PATTERN'
            
            WHEN overtime_frequency >= 20
            THEN 'MODERATE_OVERTIME_WORKER'
            
            ELSE 'OCCASIONAL_OVERTIME_WORKER'
        END AS overtime_pattern_type,
        
        CASE 
            WHEN (overtime_frequency * 0.3) + (GREATEST(0, avg_overtime_hours - 1) * 15) + (severe_overtime_frequency * 0.4) + (weekend_overtime_rate * 0.2) + (GREATEST(0, max_overtime_hours - 4) * 3) >= 80
            THEN 'CRITICAL_RISK'
            
            WHEN (overtime_frequency * 0.3) + (GREATEST(0, avg_overtime_hours - 1) * 15) + (severe_overtime_frequency * 0.4) + (weekend_overtime_rate * 0.2) + (GREATEST(0, max_overtime_hours - 4) * 3) >= 60
            THEN 'HIGH_RISK'
            
            WHEN (overtime_frequency * 0.3) + (GREATEST(0, avg_overtime_hours - 1) * 15) + (severe_overtime_frequency * 0.4) + (weekend_overtime_rate * 0.2) + (GREATEST(0, max_overtime_hours - 4) * 3) >= 40
            THEN 'MODERATE_RISK'
            
            WHEN (overtime_frequency * 0.3) + (GREATEST(0, avg_overtime_hours - 1) * 15) + (severe_overtime_frequency * 0.4) + (weekend_overtime_rate * 0.2) + (GREATEST(0, max_overtime_hours - 4) * 3) >= 20
            THEN 'LOW_RISK'
            
            ELSE 'MINIMAL_RISK'
        END AS risk_level,
        
        CASE 
            WHEN (overtime_frequency * 0.3) + (GREATEST(0, avg_overtime_hours - 1) * 15) + (severe_overtime_frequency * 0.4) + (weekend_overtime_rate * 0.2) + (GREATEST(0, max_overtime_hours - 4) * 3) >= 80
            THEN 'IMMEDIATE_INTERVENTION_REQUIRED'
            
            WHEN (overtime_frequency * 0.3) + (GREATEST(0, avg_overtime_hours - 1) * 15) + (severe_overtime_frequency * 0.4) + (weekend_overtime_rate * 0.2) + (GREATEST(0, max_overtime_hours - 4) * 3) >= 60
            THEN 'SCHEDULE_ADJUSTMENT_NEEDED'
            
            WHEN (overtime_frequency * 0.3) + (GREATEST(0, avg_overtime_hours - 1) * 15) + (severe_overtime_frequency * 0.4) + (weekend_overtime_rate * 0.2) + (GREATEST(0, max_overtime_hours - 4) * 3) >= 40
            THEN 'MONITOR_CLOSELY'
            
            WHEN (overtime_frequency * 0.3) + (GREATEST(0, avg_overtime_hours - 1) * 15) + (severe_overtime_frequency * 0.4) + (weekend_overtime_rate * 0.2) + (GREATEST(0, max_overtime_hours - 4) * 3) >= 20
            THEN 'PERIODIC_REVIEW'
            
            ELSE 'NORMAL_MONITORING'
        END AS recommended_action
        
    FROM caregiver_overtime_patterns
)

SELECT 
    "caregiverId",
    caregiver_name,
    email,
    "phoneNumber",
    status,
    
    total_visits,
    overtime_visits,
    on_time_visits,
    ROUND(avg_overtime_hours, 2) AS avg_overtime_hours,
    ROUND(max_overtime_hours, 2) AS max_overtime_hours,
    ROUND(total_overtime_hours, 2) AS total_overtime_hours,
    ROUND(avg_overtime_percentage, 2) AS avg_overtime_percentage,
    
    overtime_frequency,
    severe_overtime_frequency,
    extreme_overtime_frequency,
    
    weekend_visits,
    weekend_overtime,
    weekend_overtime_rate,
    extended_shifts,
    long_shifts,
    normal_shifts,
    short_shifts,
    early_morning_overtime,
    evening_overtime,
    night_overtime,
    
    first_visit_date,
    last_visit_date,
    active_days,
    active_months,
    
    overtime_pattern_type,
    risk_level,
    recommended_action,
    ROUND(overtime_risk_score, 2) AS overtime_risk_score,
    
    CASE 
        WHEN overtime_pattern_type = 'CHRONIC_OVERTIME_WORKER'
        THEN 'Chronic overtime worker with ' || overtime_frequency || '% overtime frequency and average ' || ROUND(avg_overtime_hours, 1) || ' hours overtime per shift'
        
        WHEN overtime_pattern_type = 'FREQUENT_OVERTIME_WORKER'
        THEN 'Frequent overtime worker with ' || overtime_frequency || '% overtime frequency'
        
        WHEN overtime_pattern_type = 'SEVERE_OVERTIME_PATTERN'
        THEN 'Shows severe overtime pattern with ' || severe_overtime_frequency || '% of shifts exceeding 2 hours overtime'
        
        WHEN overtime_pattern_type = 'WEEKEND_OVERTIME_SPECIALIST'
        THEN 'Specializes in weekend overtime with ' || weekend_overtime_rate || '% weekend overtime rate'
        
        WHEN overtime_pattern_type = 'EXTENDED_SHIFT_WORKER'
        THEN 'Frequently works extended shifts (>12 hours) with ' || extended_shifts || ' extended shifts'
        
        WHEN overtime_pattern_type = 'EVENING_OVERTIME_PATTERN'
        THEN 'Shows evening overtime pattern with ' || evening_overtime || ' evening overtime shifts'
        
        WHEN overtime_pattern_type = 'NIGHT_OVERTIME_PATTERN'
        THEN 'Shows night overtime pattern with ' || night_overtime || ' night overtime shifts'
        
        WHEN overtime_pattern_type = 'MODERATE_OVERTIME_WORKER'
        THEN 'Moderate overtime worker with ' || overtime_frequency || '% overtime frequency'
        
        ELSE 'Occasional overtime worker with ' || overtime_frequency || '% overtime frequency'
    END AS pattern_insight,
    
    CASE 
        WHEN weekend_overtime_rate > 50
        THEN 'High weekend overtime rate suggests potential scheduling issues or weekend staffing shortages'
        
        WHEN severe_overtime_frequency > 30
        THEN 'High severe overtime frequency indicates potential workload management issues'
        
        WHEN evening_overtime > overtime_visits * 0.6
        THEN 'Evening overtime pattern suggests potential shift handover or evening workload issues'
        
        WHEN night_overtime > overtime_visits * 0.4
        THEN 'Night overtime pattern suggests potential overnight care requirements'
        
        WHEN extended_shifts > total_visits * 0.3
        THEN 'High number of extended shifts suggests potential staffing shortages or complex care needs'
        
        WHEN avg_overtime_hours > 3
        THEN 'High average overtime hours suggests potential workload or scheduling inefficiencies'
        
        ELSE 'Standard overtime patterns observed'
    END AS key_insight

FROM overtime_insights

WHERE overtime_visits > 0 

ORDER BY 
    overtime_risk_score DESC,
    overtime_frequency DESC,
    total_overtime_hours DESC

LIMIT 100;
