
WITH visit_stats AS (

    SELECT 
        AVG(CASE 
            WHEN cl."clockInActualDatetime" IS NOT NULL 
                 AND cl."clockOutActualDatetime" IS NOT NULL
                 AND cl."clockInActualDatetime" < cl."clockOutActualDatetime"
                 AND EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 BETWEEN 15 AND 1440
            THEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60
            ELSE NULL 
        END) AS overall_avg_duration_minutes,
        
        STDDEV(CASE 
            WHEN cl."clockInActualDatetime" IS NOT NULL 
                 AND cl."clockOutActualDatetime" IS NOT NULL
                 AND cl."clockInActualDatetime" < cl."clockOutActualDatetime"
                 AND EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 BETWEEN 15 AND 1440
            THEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60
            ELSE NULL 
        END) AS overall_stddev_duration_minutes,
        
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY 
            CASE 
                WHEN cl."clockInActualDatetime" IS NOT NULL 
                     AND cl."clockOutActualDatetime" IS NOT NULL
                     AND cl."clockInActualDatetime" < cl."clockOutActualDatetime"
                     AND EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 BETWEEN 15 AND 1440
                THEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60
                ELSE NULL 
            END) AS q1_duration_minutes,
        
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY 
            CASE 
                WHEN cl."clockInActualDatetime" IS NOT NULL 
                     AND cl."clockOutActualDatetime" IS NOT NULL
                     AND cl."clockInActualDatetime" < cl."clockOutActualDatetime"
                     AND EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 BETWEEN 15 AND 1440
                THEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60
                ELSE NULL 
            END) AS q3_duration_minutes
        
    FROM "Carelog" cl
    WHERE cl."clockInActualDatetime" IS NOT NULL 
          AND cl."clockOutActualDatetime" IS NOT NULL
),

atypical_visits AS (
    SELECT 
        c."caregiverId",
        c."firstName" || ' ' || c."lastName" AS caregiver_name,
        c.email,
        c."phoneNumber",
        cl."carelogId",
        cl."startDatetime" AS scheduled_start,
        cl."endDatetime" AS scheduled_end,
        cl."clockInActualDatetime" AS actual_start,
        cl."clockOutActualDatetime" AS actual_end,
        

        EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 AS actual_duration_minutes,
        

        CASE 
            WHEN cl."startDatetime" IS NOT NULL AND cl."endDatetime" IS NOT NULL
            THEN EXTRACT(EPOCH FROM (cl."endDatetime" - cl."startDatetime"))/60
            ELSE NULL 
        END AS scheduled_duration_minutes,
        

        CASE 
            WHEN cl."startDatetime" IS NOT NULL AND cl."endDatetime" IS NOT NULL
            THEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 - 
                 EXTRACT(EPOCH FROM (cl."endDatetime" - cl."startDatetime"))/60
            ELSE NULL 
        END AS duration_difference_minutes,
        

        CASE 
            WHEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 < 15
            THEN 'EXTREMELY_SHORT'
            
            WHEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 > 1440
            THEN 'EXTREMELY_LONG'
            
            WHEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 < 
                 (SELECT q1_duration_minutes - 1.5 * (q3_duration_minutes - q1_duration_minutes) FROM visit_stats)
            THEN 'STATISTICALLY_SHORT'
            
            WHEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 > 
                 (SELECT q3_duration_minutes + 1.5 * (q3_duration_minutes - q1_duration_minutes) FROM visit_stats)
            THEN 'STATISTICALLY_LONG'
            
            WHEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 < 
                 (SELECT overall_avg_duration_minutes - 2 * overall_stddev_duration_minutes FROM visit_stats)
            THEN 'SIGNIFICANTLY_SHORT'
            
            WHEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 > 
                 (SELECT overall_avg_duration_minutes + 2 * overall_stddev_duration_minutes FROM visit_stats)
            THEN 'SIGNIFICANTLY_LONG'
            
            ELSE 'NORMAL'
        END AS atypical_type,
        

        CASE 
            WHEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 < 15
                 OR EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 > 1440
            THEN 'CRITICAL'
            
            WHEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 < 
                 (SELECT q1_duration_minutes - 1.5 * (q3_duration_minutes - q1_duration_minutes) FROM visit_stats)
                 OR EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 > 
                    (SELECT q3_duration_minutes + 1.5 * (q3_duration_minutes - q1_duration_minutes) FROM visit_stats)
            THEN 'HIGH'
            
            WHEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 < 
                 (SELECT overall_avg_duration_minutes - 2 * overall_stddev_duration_minutes FROM visit_stats)
                 OR EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 > 
                    (SELECT overall_avg_duration_minutes + 2 * overall_stddev_duration_minutes FROM visit_stats)
            THEN 'MEDIUM'
            
            ELSE 'LOW'
        END AS severity_level,
        

        CASE 
            WHEN (SELECT overall_stddev_duration_minutes FROM visit_stats) > 0
            THEN ABS(EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/60 - 
                    (SELECT overall_avg_duration_minutes FROM visit_stats)) / 
                   (SELECT overall_stddev_duration_minutes FROM visit_stats)
            ELSE NULL 
        END AS z_score
        
    FROM 
        "Caregiver" c
    JOIN 
        "Carelog" cl ON c."caregiverId" = cl."caregiverId"
    WHERE 
        cl."clockInActualDatetime" IS NOT NULL 
        AND cl."clockOutActualDatetime" IS NOT NULL
        AND cl."clockInActualDatetime" < cl."clockOutActualDatetime"
)

SELECT 
    "caregiverId",
    caregiver_name,
    email,
    "phoneNumber",
    "carelogId",
    scheduled_start,
    scheduled_end,
    actual_start,
    actual_end,
    

    ROUND(actual_duration_minutes, 2) AS actual_duration_minutes,
    ROUND(actual_duration_minutes / 60, 2) AS actual_duration_hours,
    ROUND(scheduled_duration_minutes, 2) AS scheduled_duration_minutes,
    ROUND(duration_difference_minutes, 2) AS duration_difference_minutes,
    

    atypical_type,
    severity_level,
    ROUND(CAST(z_score AS NUMERIC), 2) AS z_score,
    

    CASE 
        WHEN atypical_type = 'EXTREMELY_SHORT'
        THEN 'Extremely short visit (less than 15 minutes)'
        
        WHEN atypical_type = 'EXTREMELY_LONG'
        THEN 'Extremely long visit (more than 24 hours)'
        
        WHEN atypical_type = 'STATISTICALLY_SHORT'
        THEN 'Statistically short visit (below Q1 - 1.5*IQR)'
        
        WHEN atypical_type = 'STATISTICALLY_LONG'
        THEN 'Statistically long visit (above Q3 + 1.5*IQR)'
        
        WHEN atypical_type = 'SIGNIFICANTLY_SHORT'
        THEN 'Significantly short visit (more than 2 standard deviations below mean)'
        
        WHEN atypical_type = 'SIGNIFICANTLY_LONG'
        THEN 'Significantly long visit (more than 2 standard deviations above mean)'
        
        ELSE 'Normal duration'
    END AS atypical_description

FROM atypical_visits

WHERE atypical_type != 'NORMAL' 

ORDER BY 
    severity_level DESC,  
    z_score DESC,  
    actual_duration_minutes DESC 

LIMIT 100; 