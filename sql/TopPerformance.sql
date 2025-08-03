-- based on the actual clock in and clock out time
SELECT 
    c."caregiverId",
    c."firstName" || ' ' || c."lastName" AS caregiver_name,
    c.email,
    c."phoneNumber",
    c.status AS caregiver_status,
    

    COUNT(cl.id) AS total_scheduled_visits,
    SUM(CASE WHEN cl."clockInActualDatetime" IS NOT NULL OR cl."clockOutActualDatetime" IS NOT NULL THEN 1 ELSE 0 END) AS actual_visits,
    

    SUM(CASE WHEN cl."clockInActualDatetime" IS NOT NULL AND cl."clockOutActualDatetime" IS NOT NULL THEN 1 ELSE 0 END) AS complete_visits,
    

    SUM(CASE WHEN (cl."clockInActualDatetime" IS NOT NULL AND cl."clockOutActualDatetime" IS NULL) 
              OR (cl."clockInActualDatetime" IS NULL AND cl."clockOutActualDatetime" IS NOT NULL) 
         THEN 1 ELSE 0 END) AS partial_visits,
    

    SUM(CASE WHEN cl."clockInActualDatetime" IS NULL AND cl."clockOutActualDatetime" IS NULL THEN 1 ELSE 0 END) AS no_show_visits,
    

    ROUND(100.0 * SUM(CASE WHEN cl."clockInActualDatetime" IS NOT NULL OR cl."clockOutActualDatetime" IS NOT NULL THEN 1 ELSE 0 END) / 
          NULLIF(COUNT(cl.id), 0), 2) AS completion_rate,
    
    ROUND(100.0 * SUM(CASE WHEN cl."clockInActualDatetime" IS NOT NULL AND cl."clockOutActualDatetime" IS NOT NULL THEN 1 ELSE 0 END) / 
          NULLIF(COUNT(cl.id), 0), 2) AS complete_visit_rate,
    

    ROUND(SUM(CASE 
        WHEN cl."clockInActualDatetime" IS NOT NULL AND cl."clockOutActualDatetime" IS NOT NULL 
        THEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/3600
        ELSE 0 
    END), 2) AS total_work_hours,
    
    ROUND(AVG(CASE 
        WHEN cl."clockInActualDatetime" IS NOT NULL AND cl."clockOutActualDatetime" IS NOT NULL 
        THEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/3600
        ELSE NULL 
    END), 2) AS avg_work_hours,
    

    SUM(CASE WHEN cl."startDatetime" IS NOT NULL AND cl."endDatetime" IS NOT NULL
              AND cl."clockInActualDatetime" IS NOT NULL 
              AND cl."clockOutActualDatetime" IS NOT NULL
              AND cl."clockInActualDatetime" <= cl."startDatetime"
              AND cl."clockOutActualDatetime" >= cl."endDatetime"
             THEN 1 ELSE 0 END) AS punctual_visits,

    MIN(cl."clockInActualDatetime") AS first_actual_visit,
    MAX(cl."clockOutActualDatetime") AS last_actual_visit,
    

    COUNT(DISTINCT DATE(cl."clockInActualDatetime")) AS actual_active_days,
    

    ROUND(
        SUM(CASE 
            WHEN cl."clockInActualDatetime" IS NOT NULL AND cl."clockOutActualDatetime" IS NOT NULL 
            THEN EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/3600
            ELSE 0 
        END) / NULLIF(COUNT(DISTINCT DATE(cl."clockInActualDatetime")), 0), 2
    ) AS avg_hours_per_day

FROM 
    "Caregiver" c
JOIN 
    "Carelog" cl ON c."caregiverId" = cl."caregiverId"

GROUP BY 
    c."caregiverId", c."firstName", c."lastName", c.email, c."phoneNumber", c.status

HAVING 
    COUNT(cl.id) >= 3 

ORDER BY 
    actual_visits DESC,     
    complete_visits DESC,  
    total_work_hours DESC 

LIMIT 50;