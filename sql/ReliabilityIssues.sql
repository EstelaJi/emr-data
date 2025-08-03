-- based on the Absenteeism Rate, Late Arrival Rate, Early Departure Rate, Completion Rate

SELECT 
    c."caregiverId",
    c."firstName" || ' ' || c."lastName" AS caregiver_name,
    c.email,
    c."phoneNumber",
    c.status AS caregiver_status,
    
    COUNT(cl.id) AS total_scheduled_visits,
    SUM(CASE WHEN cl."clockInActualDatetime" IS NOT NULL OR cl."clockOutActualDatetime" IS NOT NULL THEN 1 ELSE 0 END) AS completed_visits,
    
    SUM(CASE WHEN (cl."startDatetime" IS NOT NULL OR cl."endDatetime" IS NOT NULL)
              AND cl."clockInActualDatetime" IS NULL 
              AND cl."clockOutActualDatetime" IS NULL
             THEN 1 ELSE 0 END) AS missed_visits_count,
    
    SUM(CASE WHEN cl."startDatetime" IS NOT NULL 
              AND cl."clockInActualDatetime" IS NOT NULL
              AND cl."clockInActualDatetime" > cl."startDatetime"
             THEN 1 ELSE 0 END) AS late_arrivals_count,
    
    SUM(CASE WHEN cl."endDatetime" IS NOT NULL 
              AND cl."clockOutActualDatetime" IS NOT NULL
              AND cl."clockOutActualDatetime" < cl."endDatetime"
             THEN 1 ELSE 0 END) AS early_departures_count,
    
    ROUND(100.0 * SUM(CASE WHEN (cl."startDatetime" IS NOT NULL OR cl."endDatetime" IS NOT NULL)
                            AND cl."clockInActualDatetime" IS NULL 
                            AND cl."clockOutActualDatetime" IS NULL
                           THEN 1 ELSE 0 END) / 
          NULLIF(COUNT(cl.id), 0), 2) AS absenteeism_rate,    
    ROUND(100.0 * SUM(CASE WHEN cl."startDatetime" IS NOT NULL 
                            AND cl."clockInActualDatetime" IS NOT NULL
                            AND cl."clockInActualDatetime" > cl."startDatetime"
                           THEN 1 ELSE 0 END) / 
          NULLIF(SUM(CASE WHEN cl."startDatetime" IS NOT NULL AND cl."clockInActualDatetime" IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS late_arrival_rate,
    
    ROUND(100.0 * SUM(CASE WHEN cl."endDatetime" IS NOT NULL 
                            AND cl."clockOutActualDatetime" IS NOT NULL
                            AND cl."clockOutActualDatetime" < cl."endDatetime"
                           THEN 1 ELSE 0 END) / 
          NULLIF(SUM(CASE WHEN cl."endDatetime" IS NOT NULL AND cl."clockOutActualDatetime" IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS early_departure_rate,
    

    ROUND(100.0 * SUM(CASE WHEN cl."clockInActualDatetime" IS NOT NULL OR cl."clockOutActualDatetime" IS NOT NULL THEN 1 ELSE 0 END) / 
          NULLIF(COUNT(cl.id), 0), 2) AS completion_rate,
  
    ROUND(
        (100.0 * SUM(CASE WHEN (cl."startDatetime" IS NOT NULL OR cl."endDatetime" IS NOT NULL)
                           AND cl."clockInActualDatetime" IS NULL 
                           AND cl."clockOutActualDatetime" IS NULL
                          THEN 1 ELSE 0 END) / 
         NULLIF(COUNT(cl.id), 0)) * 0.5 +  -- 缺勤率权重50%
        (100.0 * SUM(CASE WHEN cl."startDatetime" IS NOT NULL 
                           AND cl."clockInActualDatetime" IS NOT NULL
                           AND cl."clockInActualDatetime" > cl."startDatetime"
                          THEN 1 ELSE 0 END) / 
         NULLIF(SUM(CASE WHEN cl."startDatetime" IS NOT NULL AND cl."clockInActualDatetime" IS NOT NULL THEN 1 ELSE 0 END), 0)) * 0.3 +         (100.0 * SUM(CASE WHEN cl."endDatetime" IS NOT NULL 
                           AND cl."clockOutActualDatetime" IS NOT NULL
                           AND cl."clockOutActualDatetime" < cl."endDatetime"
                          THEN 1 ELSE 0 END) / 
         NULLIF(SUM(CASE WHEN cl."endDatetime" IS NOT NULL AND cl."clockOutActualDatetime" IS NOT NULL THEN 1 ELSE 0 END), 0)) * 0.2, 2) AS severity_score,     

    CASE 
        WHEN (100.0 * SUM(CASE WHEN (cl."startDatetime" IS NOT NULL OR cl."endDatetime" IS NOT NULL)
                               AND cl."clockInActualDatetime" IS NULL 
                               AND cl."clockOutActualDatetime" IS NULL
                              THEN 1 ELSE 0 END) / 
             NULLIF(COUNT(cl.id), 0)) > 20
        THEN 'CRITICAL'
        
        WHEN (100.0 * SUM(CASE WHEN (cl."startDatetime" IS NOT NULL OR cl."endDatetime" IS NOT NULL)
                               AND cl."clockInActualDatetime" IS NULL 
                               AND cl."clockOutActualDatetime" IS NULL
                              THEN 1 ELSE 0 END) / 
             NULLIF(COUNT(cl.id), 0)) > 10
        THEN 'HIGH'
        
        WHEN (100.0 * SUM(CASE WHEN (cl."startDatetime" IS NOT NULL OR cl."endDatetime" IS NOT NULL)
                               AND cl."clockInActualDatetime" IS NULL 
                               AND cl."clockOutActualDatetime" IS NULL
                              THEN 1 ELSE 0 END) / 
             NULLIF(COUNT(cl.id), 0)) > 5
        THEN 'MEDIUM'
        
        ELSE 'LOW'
    END AS severity_level,
    

    CASE 
        WHEN (100.0 * SUM(CASE WHEN (cl."startDatetime" IS NOT NULL OR cl."endDatetime" IS NOT NULL)
                               AND cl."clockInActualDatetime" IS NULL 
                               AND cl."clockOutActualDatetime" IS NULL
                              THEN 1 ELSE 0 END) / 
             NULLIF(COUNT(cl.id), 0)) > 15
        THEN 'HIGH_ABSENTEEISM'
        
        WHEN (100.0 * SUM(CASE WHEN cl."startDatetime" IS NOT NULL 
                               AND cl."clockInActualDatetime" IS NOT NULL
                               AND cl."clockInActualDatetime" > cl."startDatetime"
                              THEN 1 ELSE 0 END) / 
             NULLIF(SUM(CASE WHEN cl."startDatetime" IS NOT NULL AND cl."clockInActualDatetime" IS NOT NULL THEN 1 ELSE 0 END), 0)) > 30
        THEN 'FREQUENT_LATE_ARRIVALS'
        
        WHEN (100.0 * SUM(CASE WHEN cl."endDatetime" IS NOT NULL 
                               AND cl."clockOutActualDatetime" IS NOT NULL
                               AND cl."clockOutActualDatetime" < cl."endDatetime"
                              THEN 1 ELSE 0 END) / 
             NULLIF(SUM(CASE WHEN cl."endDatetime" IS NOT NULL AND cl."clockOutActualDatetime" IS NOT NULL THEN 1 ELSE 0 END), 0)) > 30
        THEN 'FREQUENT_EARLY_DEPARTURES'
        
        WHEN (100.0 * SUM(CASE WHEN cl."clockInActualDatetime" IS NOT NULL OR cl."clockOutActualDatetime" IS NOT NULL THEN 1 ELSE 0 END) / 
              NULLIF(COUNT(cl.id), 0)) < 70
        THEN 'LOW_COMPLETION_RATE'
        
        ELSE 'MIXED_ISSUES'
    END AS primary_issue_type,

    MAX(CASE WHEN (cl."startDatetime" IS NOT NULL OR cl."endDatetime" IS NOT NULL)
             AND cl."clockInActualDatetime" IS NULL 
             AND cl."clockOutActualDatetime" IS NULL
        THEN cl."startDatetime" END) AS last_missed_visit,
    
    MAX(CASE WHEN cl."startDatetime" IS NOT NULL 
             AND cl."clockInActualDatetime" IS NOT NULL
             AND cl."clockInActualDatetime" > cl."startDatetime"
        THEN cl."clockInActualDatetime" END) AS last_late_arrival,
    
    MAX(CASE WHEN cl."endDatetime" IS NOT NULL 
             AND cl."clockOutActualDatetime" IS NOT NULL
             AND cl."clockOutActualDatetime" < cl."endDatetime"
        THEN cl."clockOutActualDatetime" END) AS last_early_departure

FROM 
    "Caregiver" c
JOIN 
    "Carelog" cl ON c."caregiverId" = cl."caregiverId"

GROUP BY 
    c."caregiverId", c."firstName", c."lastName", c.email, c."phoneNumber", c.status

HAVING 
    COUNT(cl.id) >= 5
    
    AND (

        (100.0 * SUM(CASE WHEN (cl."startDatetime" IS NOT NULL OR cl."endDatetime" IS NOT NULL)
                           AND cl."clockInActualDatetime" IS NULL 
                           AND cl."clockOutActualDatetime" IS NULL
                          THEN 1 ELSE 0 END) / 
         NULLIF(COUNT(cl.id), 0)) > 5
        OR

        (100.0 * SUM(CASE WHEN cl."startDatetime" IS NOT NULL 
                           AND cl."clockInActualDatetime" IS NOT NULL
                           AND cl."clockInActualDatetime" > cl."startDatetime"
                          THEN 1 ELSE 0 END) / 
         NULLIF(SUM(CASE WHEN cl."startDatetime" IS NOT NULL AND cl."clockInActualDatetime" IS NOT NULL THEN 1 ELSE 0 END), 0)) > 20
        OR
    
        (100.0 * SUM(CASE WHEN cl."endDatetime" IS NOT NULL 
                           AND cl."clockOutActualDatetime" IS NOT NULL
                           AND cl."clockOutActualDatetime" < cl."endDatetime"
                          THEN 1 ELSE 0 END) / 
         NULLIF(SUM(CASE WHEN cl."endDatetime" IS NOT NULL AND cl."clockOutActualDatetime" IS NOT NULL THEN 1 ELSE 0 END), 0)) > 20
        OR
       
        (100.0 * SUM(CASE WHEN cl."clockInActualDatetime" IS NOT NULL OR cl."clockOutActualDatetime" IS NOT NULL THEN 1 ELSE 0 END) / 
         NULLIF(COUNT(cl.id), 0)) < 70
    )

ORDER BY 
    severity_score DESC,    
    absenteeism_rate DESC,
    late_arrival_rate DESC, 
    early_departure_rate DESC
LIMIT 50; 