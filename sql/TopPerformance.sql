-- based on the actual clock in and clock out time
SELECT 
    c.caregiver_id,
    c.first_name || ' ' || c.last_name AS caregiver_name,
    c.email,
    c.phone_number,
    c.status AS caregiver_status,
    

    COUNT(cl.id) AS total_scheduled_visits,
    SUM(CASE WHEN cl.clock_in_actual_datetime IS NOT NULL OR cl.clock_out_actual_datetime IS NOT NULL THEN 1 ELSE 0 END) AS actual_visits,
    

    SUM(CASE WHEN cl.clock_in_actual_datetime IS NOT NULL AND cl.clock_out_actual_datetime IS NOT NULL THEN 1 ELSE 0 END) AS complete_visits,
    

    SUM(CASE WHEN (cl.clock_in_actual_datetime IS NOT NULL AND cl.clock_out_actual_datetime IS NULL) 
              OR (cl.clock_in_actual_datetime IS NULL AND cl.clock_out_actual_datetime IS NOT NULL) 
         THEN 1 ELSE 0 END) AS partial_visits,
    

    SUM(CASE WHEN cl.clock_in_actual_datetime IS NULL AND cl.clock_out_actual_datetime IS NULL THEN 1 ELSE 0 END) AS no_show_visits,
    

    ROUND(100.0 * SUM(CASE WHEN cl.clock_in_actual_datetime IS NOT NULL OR cl.clock_out_actual_datetime IS NOT NULL THEN 1 ELSE 0 END) / 
          NULLIF(COUNT(cl.id), 0), 2) AS completion_rate,
    
    ROUND(100.0 * SUM(CASE WHEN cl.clock_in_actual_datetime IS NOT NULL AND cl.clock_out_actual_datetime IS NOT NULL THEN 1 ELSE 0 END) / 
          NULLIF(COUNT(cl.id), 0), 2) AS complete_visit_rate,
    

    ROUND(SUM(CASE 
        WHEN cl.clock_in_actual_datetime IS NOT NULL AND cl.clock_out_actual_datetime IS NOT NULL 
        THEN EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/3600
        ELSE 0 
    END), 2) AS total_work_hours,
    
    ROUND(AVG(CASE 
        WHEN cl.clock_in_actual_datetime IS NOT NULL AND cl.clock_out_actual_datetime IS NOT NULL 
        THEN EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/3600
        ELSE NULL 
    END), 2) AS avg_work_hours,
    

    SUM(CASE WHEN cl.start_datetime IS NOT NULL AND cl.end_datetime IS NOT NULL
              AND cl.clock_in_actual_datetime IS NOT NULL 
              AND cl.clock_out_actual_datetime IS NOT NULL
              AND cl.clock_in_actual_datetime <= cl.start_datetime
              AND cl.clock_out_actual_datetime >= cl.end_datetime
             THEN 1 ELSE 0 END) AS punctual_visits,

    MIN(cl.clock_in_actual_datetime) AS first_actual_visit,
    MAX(cl.clock_out_actual_datetime) AS last_actual_visit,
    

    COUNT(DISTINCT DATE(cl.clock_in_actual_datetime)) AS actual_active_days,
    

    ROUND(
        SUM(CASE 
            WHEN cl.clock_in_actual_datetime IS NOT NULL AND cl.clock_out_actual_datetime IS NOT NULL 
            THEN EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/3600
            ELSE 0 
        END) / NULLIF(COUNT(DISTINCT DATE(cl.clock_in_actual_datetime)), 0), 2
    ) AS avg_hours_per_day

FROM 
    "Caregivers" c
JOIN 
    "Carelog" cl ON c.caregiver_id = cl.caregiver_id

GROUP BY 
    c.caregiver_id, c.first_name, c.last_name, c.email, c.phone_number, c.status

HAVING 
    COUNT(cl.id) >= 3 

ORDER BY 
    actual_visits DESC,     
    complete_visits DESC,  
    total_work_hours DESC 

LIMIT 50;