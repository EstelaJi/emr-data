-- 识别经常产生加班时间的护理人员
WITH overtime_visits AS (
    SELECT 
        c.caregiver_id,
        c.first_name || ' ' || c.last_name AS caregiver_name,
        c.email,
        c.phone_number,
        c.status,
        cl.carelog_id,
        cl.start_datetime AS scheduled_start,
        cl.end_datetime AS scheduled_end,
        cl.clock_in_actual_datetime AS actual_start,
        cl.clock_out_actual_datetime AS actual_end,
       
        CASE 
            WHEN cl.start_datetime IS NOT NULL AND cl.end_datetime IS NOT NULL
            THEN EXTRACT(EPOCH FROM (cl.end_datetime - cl.start_datetime))/3600
            ELSE NULL 
        END AS scheduled_hours,
        

        CASE 
            WHEN cl.clock_in_actual_datetime IS NOT NULL 
                 AND cl.clock_out_actual_datetime IS NOT NULL
                 AND cl.clock_in_actual_datetime < cl.clock_out_actual_datetime
            THEN EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/3600
            ELSE NULL 
        END AS actual_hours,
        

        CASE 
            WHEN cl.start_datetime IS NOT NULL AND cl.end_datetime IS NOT NULL
                 AND cl.clock_in_actual_datetime IS NOT NULL 
                 AND cl.clock_out_actual_datetime IS NOT NULL
                 AND cl.clock_in_actual_datetime < cl.clock_out_actual_datetime
            THEN GREATEST(0, EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/3600 - 
                              EXTRACT(EPOCH FROM (cl.end_datetime - cl.start_datetime))/3600)
            ELSE NULL 
        END AS overtime_hours,
        
        -- 加班百分比
        CASE 
            WHEN cl.start_datetime IS NOT NULL AND cl.end_datetime IS NOT NULL
                 AND cl.clock_in_actual_datetime IS NOT NULL 
                 AND cl.clock_out_actual_datetime IS NOT NULL
                 AND cl.clock_in_actual_datetime < cl.clock_out_actual_datetime
                 AND EXTRACT(EPOCH FROM (cl.end_datetime - cl.start_datetime))/3600 > 0
            THEN ROUND(100.0 * GREATEST(0, EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/3600 - 
                                              EXTRACT(EPOCH FROM (cl.end_datetime - cl.start_datetime))/3600) / 
                       EXTRACT(EPOCH FROM (cl.end_datetime - cl.start_datetime))/3600, 2)
            ELSE NULL 
        END AS overtime_percentage,
        

        DATE(cl.clock_in_actual_datetime) AS visit_date,
        

        DATE_TRUNC('month', cl.clock_in_actual_datetime) AS visit_month
        
    FROM 
        "Caregivers" c
    JOIN 
        "Carelog" cl ON c.caregiver_id = cl.caregiver_id
    WHERE 
        cl.clock_in_actual_datetime IS NOT NULL 
        AND cl.clock_out_actual_datetime IS NOT NULL
        AND cl.clock_in_actual_datetime < cl.clock_out_actual_datetime
        AND cl.start_datetime IS NOT NULL 
        AND cl.end_datetime IS NOT NULL
        AND cl.start_datetime < cl.end_datetime
),

caregiver_overtime_stats AS (
    SELECT 
        caregiver_id,
        caregiver_name,
        email,
        phone_number,
        status,
        

        COUNT(*) AS total_visits,
        COUNT(CASE WHEN overtime_hours > 0 THEN 1 END) AS overtime_visits,
        COUNT(CASE WHEN overtime_hours = 0 THEN 1 END) AS on_time_visits,

        ROUND(AVG(CASE WHEN overtime_hours > 0 THEN overtime_hours END), 2) AS avg_overtime_hours,
        ROUND(MAX(overtime_hours), 2) AS max_overtime_hours,
        ROUND(SUM(overtime_hours), 2) AS total_overtime_hours,
        

        ROUND(AVG(scheduled_hours), 2) AS avg_scheduled_hours,
        ROUND(AVG(actual_hours), 2) AS avg_actual_hours,
        ROUND(AVG(actual_hours - scheduled_hours), 2) AS avg_hours_difference,
        
  
        ROUND(100.0 * COUNT(CASE WHEN overtime_hours > 0 THEN 1 END) / COUNT(*), 2) AS overtime_frequency_percentage,
        

        ROUND(100.0 * COUNT(CASE WHEN overtime_hours > 2 THEN 1 END) / COUNT(*), 2) AS severe_overtime_frequency,
        ROUND(100.0 * COUNT(CASE WHEN overtime_hours > 4 THEN 1 END) / COUNT(*), 2) AS extreme_overtime_frequency,
        

        ROUND(AVG(CASE WHEN overtime_percentage > 0 THEN overtime_percentage END), 2) AS avg_overtime_percentage,
        ROUND(MAX(overtime_percentage), 2) AS max_overtime_percentage,

        MIN(visit_date) AS first_visit_date,
        MAX(visit_date) AS last_visit_date,
        COUNT(DISTINCT visit_date) AS active_days,
        COUNT(DISTINCT visit_month) AS active_months,
        

        MAX(CASE WHEN overtime_hours > 0 THEN visit_date END) AS last_overtime_date,

        COUNT(CASE WHEN overtime_hours > 0 THEN 1 END) AS consecutive_overtime_days
        
    FROM overtime_visits
    
    GROUP BY 
        caregiver_id, caregiver_name, email, phone_number, status
    
    HAVING 
        COUNT(*) >= 5 )

SELECT 
    caregiver_id,
    caregiver_name,
    email,
    phone_number,
    status,
    

    total_visits,
    overtime_visits,
    on_time_visits,
    ROUND(avg_scheduled_hours, 2) AS avg_scheduled_hours,
    ROUND(avg_actual_hours, 2) AS avg_actual_hours,
    ROUND(avg_hours_difference, 2) AS avg_hours_difference,
    

    ROUND(avg_overtime_hours, 2) AS avg_overtime_hours,
    ROUND(max_overtime_hours, 2) AS max_overtime_hours,
    ROUND(total_overtime_hours, 2) AS total_overtime_hours,
    

    overtime_frequency_percentage,
    severe_overtime_frequency,
    extreme_overtime_frequency,
    

    ROUND(avg_overtime_percentage, 2) AS avg_overtime_percentage,
    ROUND(max_overtime_percentage, 2) AS max_overtime_percentage,
    

    first_visit_date,
    last_visit_date,
    active_days,
    active_months,
    last_overtime_date,
    

    CASE 
        WHEN overtime_frequency_percentage >= 50 AND avg_overtime_hours >= 2
        THEN 'CRITICAL'
        
        WHEN overtime_frequency_percentage >= 30 AND avg_overtime_hours >= 1.5
        THEN 'HIGH'
        
        WHEN overtime_frequency_percentage >= 20 AND avg_overtime_hours >= 1
        THEN 'MEDIUM'
        
        WHEN overtime_frequency_percentage >= 10
        THEN 'LOW'
        
        ELSE 'MINIMAL'
    END AS overtime_severity,
    

    CASE 
        WHEN overtime_frequency_percentage >= 50
        THEN 'FREQUENT_OVERTIME'
        
        WHEN severe_overtime_frequency >= 20
        THEN 'SEVERE_OVERTIME'
        
        WHEN extreme_overtime_frequency >= 10
        THEN 'EXTREME_OVERTIME'
        
        WHEN avg_overtime_hours >= 2
        THEN 'LONG_OVERTIME'
        
        WHEN overtime_frequency_percentage >= 20
        THEN 'MODERATE_OVERTIME'
        
        ELSE 'OCCASIONAL_OVERTIME'
    END AS overtime_pattern,
    

    ROUND(
        (overtime_frequency_percentage * 0.4) + 
        (GREATEST(0, avg_overtime_hours - 1) * 20) +         (severe_overtime_frequency * 0.3) + 
        (GREATEST(0, max_overtime_hours - 4) * 2),
        2
    ) AS overtime_risk_score

FROM caregiver_overtime_stats

WHERE overtime_visits > 0 
ORDER BY 
    overtime_risk_score DESC,    overtime_frequency_percentage DESC,     total_overtime_hours DESC 

LIMIT 50; 