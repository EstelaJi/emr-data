
WITH visit_durations AS (
    SELECT 
        c."caregiverId",
        c."firstName" || ' ' || c."lastName" AS caregiver_name,
        cl."carelogId",
        cl."startDatetime" AS scheduled_start,
        cl."endDatetime" AS scheduled_end,
        cl."clockInActualDatetime" AS actual_start,
        cl."clockOutActualDatetime" AS actual_end,
        

        CASE 
            WHEN cl."clockInActualDatetime" IS NOT NULL 
                 AND cl."clockOutActualDatetime" IS NOT NULL
                 AND cl."clockInActualDatetime" < cl."clockOutActualDatetime"
            THEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/3600
            ELSE NULL 
        END AS actual_duration_hours,
        

        CASE 
            WHEN cl."startDatetime" IS NOT NULL 
                 AND cl."endDatetime" IS NOT NULL
                 AND cl."startDatetime" < cl."endDatetime"
            THEN EXTRACT(EPOCH FROM (cl."endDatetime" - cl."startDatetime"))/3600
            ELSE NULL 
        END AS scheduled_duration_hours,
        
        -- 数据质量标记
        CASE 
            WHEN cl."clockInActualDatetime" IS NULL AND cl."clockOutActualDatetime" IS NULL
            THEN 'NO_ACTUAL_TIMES'
            
            WHEN cl."clockInActualDatetime" IS NULL AND cl."clockOutActualDatetime" IS NOT NULL
            THEN 'MISSING_CLOCK_IN'
            
            WHEN cl."clockInActualDatetime" IS NOT NULL AND cl."clockOutActualDatetime" IS NULL
            THEN 'MISSING_CLOCK_OUT'
            
            WHEN cl."clockInActualDatetime" IS NOT NULL 
                 AND cl."clockOutActualDatetime" IS NOT NULL
                 AND cl."clockInActualDatetime" >= cl."clockOutActualDatetime"
            THEN 'INVALID_DURATION'
            
            WHEN cl."clockInActualDatetime" IS NOT NULL 
                 AND cl."clockOutActualDatetime" IS NOT NULL
                 AND EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/3600 > 24
            THEN 'EXCESSIVE_DURATION'
            
            WHEN cl."clockInActualDatetime" IS NOT NULL 
                 AND cl."clockOutActualDatetime" IS NOT NULL
                 AND EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/3600 < 0.1
            THEN 'TOO_SHORT_DURATION'
            
            ELSE 'VALID_DURATION'
        END AS data_quality_flag,
        

        CASE 
            WHEN cl."clockInActualDatetime" IS NOT NULL 
                 AND cl."clockOutActualDatetime" IS NOT NULL
                 AND cl."clockInActualDatetime" < cl."clockOutActualDatetime"
                 AND cl."startDatetime" IS NOT NULL 
                 AND cl."endDatetime" IS NOT NULL
                 AND cl."startDatetime" < cl."endDatetime"
            THEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/3600 - 
                 EXTRACT(EPOCH FROM (cl."endDatetime" - cl."startDatetime"))/3600
            ELSE NULL 
        END AS duration_difference_hours
        
    FROM 
        "Caregiver" c
    JOIN 
        "Carelog" cl ON c."caregiverId" = cl."caregiverId"
    WHERE 

        (cl."clockInActualDatetime" IS NOT NULL OR cl."clockOutActualDatetime" IS NOT NULL)
)

SELECT 
    "caregiverId",
    caregiver_name,
    
    COUNT(*) AS total_visits,
    COUNT(CASE WHEN data_quality_flag = 'VALID_DURATION' THEN 1 END) AS valid_duration_visits,
    COUNT(CASE WHEN data_quality_flag != 'VALID_DURATION' THEN 1 END) AS problematic_visits,
    

    COUNT(CASE WHEN data_quality_flag = 'NO_ACTUAL_TIMES' THEN 1 END) AS no_actual_times,
    COUNT(CASE WHEN data_quality_flag = 'MISSING_CLOCK_IN' THEN 1 END) AS missing_clock_in,
    COUNT(CASE WHEN data_quality_flag = 'MISSING_CLOCK_OUT' THEN 1 END) AS missing_clock_out,
    COUNT(CASE WHEN data_quality_flag = 'INVALID_DURATION' THEN 1 END) AS invalid_duration,
    COUNT(CASE WHEN data_quality_flag = 'EXCESSIVE_DURATION' THEN 1 END) AS excessive_duration,
    COUNT(CASE WHEN data_quality_flag = 'TOO_SHORT_DURATION' THEN 1 END) AS too_short_duration,
    

    ROUND(AVG(CASE WHEN data_quality_flag = 'VALID_DURATION' THEN actual_duration_hours END), 2) AS avg_actual_duration_hours,
    

    ROUND(AVG(CASE WHEN scheduled_duration_hours IS NOT NULL THEN scheduled_duration_hours END), 2) AS avg_scheduled_duration_hours,
    

    ROUND(AVG(CASE WHEN duration_difference_hours IS NOT NULL THEN duration_difference_hours END), 2) AS avg_duration_difference_hours,
    

    ROUND(100.0 * COUNT(CASE WHEN data_quality_flag = 'VALID_DURATION' THEN 1 END) / 
          NULLIF(COUNT(*), 0), 2) AS data_quality_percentage,
    

    ROUND(MIN(CASE WHEN data_quality_flag = 'VALID_DURATION' THEN actual_duration_hours END), 2) AS min_actual_duration_hours,
    ROUND(MAX(CASE WHEN data_quality_flag = 'VALID_DURATION' THEN actual_duration_hours END), 2) AS max_actual_duration_hours,
    ROUND(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN data_quality_flag = 'VALID_DURATION' THEN actual_duration_hours END) AS NUMERIC), 2) AS median_actual_duration_hours,
    

    ROUND(CAST(STDDEV(CASE WHEN data_quality_flag = 'VALID_DURATION' THEN actual_duration_hours END) AS NUMERIC), 2) AS stddev_actual_duration_hours,
    

    COUNT(CASE WHEN data_quality_flag = 'VALID_DURATION' AND actual_duration_hours < 1 THEN 1 END) AS visits_under_1_hour,
    COUNT(CASE WHEN data_quality_flag = 'VALID_DURATION' AND actual_duration_hours BETWEEN 1 AND 4 THEN 1 END) AS visits_1_to_4_hours,
    COUNT(CASE WHEN data_quality_flag = 'VALID_DURATION' AND actual_duration_hours BETWEEN 4 AND 8 THEN 1 END) AS visits_4_to_8_hours,
    COUNT(CASE WHEN data_quality_flag = 'VALID_DURATION' AND actual_duration_hours > 8 THEN 1 END) AS visits_over_8_hours

FROM visit_durations

GROUP BY 
    "caregiverId", caregiver_name

HAVING 
    COUNT(*) >= 3 

ORDER BY 
    avg_actual_duration_hours DESC NULLS LAST,   
      data_quality_percentage DESC                

LIMIT 50; 