
WITH detailed_comments AS (
    SELECT 
        c.caregiver_id,
        c.first_name || ' ' || c.last_name AS caregiver_name,
        c.email,
        c.phone_number,
        c.status,
        cl.carelog_id,
        

        cl.documentation AS comment_text,
        cl.general_comment_char_count AS comment_length,
        

        DATE(cl.clock_in_actual_datetime) AS visit_date,
        cl.clock_in_actual_datetime AS visit_start,
        cl.clock_out_actual_datetime AS visit_end,
        

        EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/3600 AS visit_duration_hours,
        

        CASE 
            WHEN cl.documentation IS NULL OR cl.documentation = ''
            THEN 'NO_COMMENT'
            
            WHEN cl.general_comment_char_count IS NULL OR cl.general_comment_char_count = 0
            THEN 'NO_COMMENT'
            
            WHEN cl.general_comment_char_count < 10
            THEN 'VERY_SHORT'
            
            WHEN cl.general_comment_char_count < 50
            THEN 'SHORT'
            
            WHEN cl.general_comment_char_count < 100
            THEN 'MEDIUM'
            
            WHEN cl.general_comment_char_count < 200
            THEN 'LONG'
            
            ELSE 'VERY_LONG'
        END AS comment_category,
        

        CASE 
            WHEN cl.documentation IS NULL OR cl.documentation = ''
            THEN 0
            
            WHEN cl.general_comment_char_count IS NULL OR cl.general_comment_char_count = 0
            THEN 0
            
            WHEN cl.general_comment_char_count < 10
            THEN 1
            
            WHEN cl.general_comment_char_count < 50
            THEN 2
            
            WHEN cl.general_comment_char_count < 100
            THEN 3
            
            WHEN cl.general_comment_char_count < 200
            THEN 4
            
            ELSE 5
        END AS comment_detail_score,
        

        CASE 
            WHEN cl.documentation IS NULL OR cl.documentation = ''
            THEN FALSE
            ELSE TRUE
        END AS has_comment,
        

        CASE 
            WHEN EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/3600 > 0
            THEN ROUND(cl.general_comment_char_count / (EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/3600), 2)
            ELSE NULL 
        END AS chars_per_hour,
        

        cl.status AS visit_status,
        

        CASE 
            WHEN DATE(cl.clock_in_actual_datetime) != DATE(cl.clock_out_actual_datetime)
            THEN TRUE ELSE FALSE 
        END AS overnight_shift,
        

        CASE 
            WHEN EXTRACT(DOW FROM cl.clock_in_actual_datetime) IN (0, 6)
            THEN TRUE ELSE FALSE 
        END AS weekend_shift
        
    FROM 
        "Caregivers" c
    JOIN 
        "Carelog" cl ON c.caregiver_id = cl.caregiver_id
    WHERE 
        cl.clock_in_actual_datetime IS NOT NULL 
        AND cl.clock_out_actual_datetime IS NOT NULL
        AND cl.clock_in_actual_datetime < cl.clock_out_actual_datetime
        AND cl.documentation IS NOT NULL 
        AND cl.documentation != ''
        AND cl.general_comment_char_count > 0 
),

caregiver_detailed_stats AS (
    SELECT 
        caregiver_id,
        caregiver_name,
        email,
        phone_number,
        status,
    
        COUNT(*) AS total_commented_visits,
        ROUND(AVG(comment_length), 2) AS avg_comment_length,
        ROUND(MAX(comment_length), 2) AS max_comment_length,
        ROUND(MIN(comment_length), 2) AS min_comment_length,
        ROUND(SUM(comment_length), 2) AS total_comment_chars,

        COUNT(CASE WHEN comment_category = 'VERY_LONG' THEN 1 END) AS very_long_comments,
        COUNT(CASE WHEN comment_category = 'LONG' THEN 1 END) AS long_comments,
        COUNT(CASE WHEN comment_category = 'MEDIUM' THEN 1 END) AS medium_comments,
        COUNT(CASE WHEN comment_category = 'SHORT' THEN 1 END) AS short_comments,
        COUNT(CASE WHEN comment_category = 'VERY_SHORT' THEN 1 END) AS very_short_comments,
        

        ROUND(100.0 * COUNT(CASE WHEN comment_category IN ('LONG', 'VERY_LONG') THEN 1 END) / COUNT(*), 2) AS detailed_comment_percentage,
        ROUND(100.0 * COUNT(CASE WHEN comment_category = 'MEDIUM' THEN 1 END) / COUNT(*), 2) AS medium_comment_percentage,
        ROUND(100.0 * COUNT(CASE WHEN comment_category IN ('SHORT', 'VERY_SHORT') THEN 1 END) / COUNT(*), 2) AS brief_comment_percentage,
        

        ROUND(AVG(comment_detail_score), 2) AS avg_comment_detail_score,
        

        ROUND(AVG(visit_duration_hours), 2) AS avg_visit_duration_hours,
        ROUND(MAX(visit_duration_hours), 2) AS max_visit_duration_hours,
        ROUND(MIN(visit_duration_hours), 2) AS min_visit_duration_hours,
        

        ROUND(AVG(chars_per_hour), 2) AS avg_chars_per_hour,
        ROUND(MAX(chars_per_hour), 2) AS max_chars_per_hour,
        

        COUNT(CASE WHEN overnight_shift THEN 1 END) AS overnight_comments,
        COUNT(CASE WHEN weekend_shift THEN 1 END) AS weekend_comments,

        MIN(visit_date) AS first_comment_date,
        MAX(visit_date) AS last_comment_date,
        COUNT(DISTINCT visit_date) AS comment_active_days,
        

        ROUND(
            (COUNT(CASE WHEN comment_category IN ('LONG', 'VERY_LONG') THEN 1 END) * 2) +
            (COUNT(CASE WHEN comment_category = 'MEDIUM' THEN 1 END) * 1.5) +
            (COUNT(CASE WHEN comment_category = 'SHORT' THEN 1 END) * 1) +
            (COUNT(CASE WHEN comment_category = 'VERY_SHORT' THEN 1 END) * 0.5)
            / COUNT(*), 2
        ) AS comment_consistency_score,
        
 
        ROUND(
            (COALESCE(AVG(comment_length), 0) * 0.4) +  
            (COALESCE(100.0 * COUNT(CASE WHEN comment_category IN ('LONG', 'VERY_LONG') THEN 1 END) / COUNT(*), 0) * 0.3) +             (COALESCE(
                ((COUNT(CASE WHEN comment_category IN ('LONG', 'VERY_LONG') THEN 1 END) * 2) +
                 (COUNT(CASE WHEN comment_category = 'MEDIUM' THEN 1 END) * 1.5) +
                 (COUNT(CASE WHEN comment_category = 'SHORT' THEN 1 END) * 1) +
                 (COUNT(CASE WHEN comment_category = 'VERY_SHORT' THEN 1 END) * 0.5))
                / COUNT(*), 0) * 10) +             (GREATEST(0, COALESCE(AVG(chars_per_hour), 0) - 10) * 2), 
            2
        ) AS comment_professionalism_score
        
    FROM detailed_comments
    
    GROUP BY caregiver_id, caregiver_name, email, phone_number, status
    
    HAVING COUNT(*) >= 3  )

SELECT 
    caregiver_id,
    caregiver_name,
    email,
    phone_number,
    status,
  
    total_commented_visits,
    avg_comment_length,
    max_comment_length,
    min_comment_length,
    total_comment_chars,
    

    very_long_comments,
    long_comments,
    medium_comments,
    short_comments,
    very_short_comments,

    detailed_comment_percentage,
    medium_comment_percentage,
    brief_comment_percentage,
    

    avg_comment_detail_score,
    comment_consistency_score,
    comment_professionalism_score,
    

    avg_visit_duration_hours,
    max_visit_duration_hours,
    min_visit_duration_hours,
    

    avg_chars_per_hour,
    max_chars_per_hour,
    

    overnight_comments,
    weekend_comments,
    

    first_comment_date,
    last_comment_date,
    comment_active_days,
    

    CASE 
        WHEN detailed_comment_percentage >= 70 AND avg_comment_length >= 100
        THEN 'HIGHLY_DETAILED_DOCUMENTER'
        
        WHEN detailed_comment_percentage >= 50 AND avg_comment_length >= 75
        THEN 'DETAILED_DOCUMENTER'
        
        WHEN detailed_comment_percentage >= 30 AND avg_comment_length >= 50
        THEN 'REGULAR_DOCUMENTER'
        
        WHEN avg_comment_length >= 100
        THEN 'LONG_COMMENT_SPECIALIST'
        
        WHEN detailed_comment_percentage >= 50
        THEN 'FREQUENT_COMMENTER'
        
        ELSE 'OCCASIONAL_COMMENTER'
    END AS documentation_pattern,
    

    CASE 
        WHEN avg_comment_detail_score >= 4 AND comment_consistency_score >= 3
        THEN 'EXCELLENT'
        
        WHEN avg_comment_detail_score >= 3 AND comment_consistency_score >= 2
        THEN 'GOOD'
        
        WHEN avg_comment_detail_score >= 2 AND comment_consistency_score >= 1
        THEN 'AVERAGE'
        
        WHEN avg_comment_detail_score >= 1
        THEN 'POOR'
        
        ELSE 'VERY_POOR'
    END AS comment_quality_rating,
    

    CASE 
        WHEN avg_chars_per_hour >= 50
        THEN 'VERY_EFFICIENT'
        
        WHEN avg_chars_per_hour >= 30
        THEN 'EFFICIENT'
        
        WHEN avg_chars_per_hour >= 15
        THEN 'MODERATE'
        
        WHEN avg_chars_per_hour >= 5
        THEN 'SLOW'
        
        ELSE 'VERY_SLOW'
    END AS comment_efficiency_rating,

    CASE 
        WHEN detailed_comment_percentage >= 70 AND avg_comment_length >= 100
        THEN 'Consistently provides detailed documentation in ' || detailed_comment_percentage || '% of visits with average length of ' || ROUND(avg_comment_length, 0) || ' characters'
        
        WHEN detailed_comment_percentage >= 50 AND avg_comment_length >= 75
        THEN 'Frequently provides detailed documentation in ' || detailed_comment_percentage || '% of visits with average length of ' || ROUND(avg_comment_length, 0) || ' characters'
        
        WHEN detailed_comment_percentage >= 30 AND avg_comment_length >= 50
        THEN 'Regularly provides documentation in ' || detailed_comment_percentage || '% of visits with average length of ' || ROUND(avg_comment_length, 0) || ' characters'
        
        WHEN avg_comment_length >= 100
        THEN 'Specializes in long comments with average length of ' || ROUND(avg_comment_length, 0) || ' characters'
        
        WHEN detailed_comment_percentage >= 50
        THEN 'Frequently comments but with moderate detail, average length of ' || ROUND(avg_comment_length, 0) || ' characters'
        
        ELSE 'Occasional commenter with average length of ' || ROUND(avg_comment_length, 0) || ' characters'
    END AS insight_description

FROM caregiver_detailed_stats

ORDER BY 
    comment_professionalism_score DESC,     
    avg_comment_length DESC,            
     detailed_comment_percentage DESC   

LIMIT 50; 