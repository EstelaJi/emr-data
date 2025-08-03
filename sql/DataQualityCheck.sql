-- 可疑文档模式分析（仅分析有文档内容的记录）
WITH suspicious_analysis AS (
    SELECT 
        c."caregiverId",
        c."firstName" || ' ' || c."lastName" AS caregiver_name,
        c.email,
        c."phoneNumber",
        c.status,
        cl."carelogId",
        
        -- 文档内容
        cl.documentation AS comment_text,
        cl."generalCommentCharCount" AS comment_length,
        
        -- 时间信息
        DATE(cl."clockInActualDatetime") AS visit_date,
        cl."clockInActualDatetime" AS visit_start,
        cl."clockOutActualDatetime" AS visit_end,
        
        -- 访问时长
        EXTRACT(EPOCH FROM (cl."clockOutActualDatetime" - cl."clockInActualDatetime"))/3600 AS visit_duration_hours,
        
        -- 可疑模式检测
        CASE 
            WHEN cl.documentation IS NULL OR cl.documentation = ''
            THEN 'MISSING_DOCUMENTATION'
            
            WHEN cl."generalCommentCharCount" < 5
            THEN 'VERY_SHORT_DOCUMENTATION'
            
            WHEN cl."generalCommentCharCount" < 10
            THEN 'SHORT_DOCUMENTATION'
            
            WHEN cl."generalCommentCharCount" > 1000
            THEN 'EXCESSIVELY_LONG'
            
            WHEN LOWER(cl.documentation) IN ('ok', 'fine', 'good', 'completed', 'done', 'finished')
            THEN 'GENERIC_COMMENT'
            
            WHEN LENGTH(cl.documentation) > 0 AND cl."generalCommentCharCount" = 0
            THEN 'LENGTH_MISMATCH'
            
            WHEN cl.documentation LIKE '%copy%' OR cl.documentation LIKE '%paste%'
            THEN 'SUSPECTED_COPY_PASTE'
            
            WHEN cl.documentation LIKE '%same%' OR cl.documentation LIKE '%identical%'
            THEN 'DUPLICATE_CONTENT_SUSPECTED'
            
            WHEN cl.documentation ~ '^[A-Za-z\s]+$' AND LENGTH(cl.documentation) < 20
            THEN 'TOO_GENERIC'
            
            ELSE 'NORMAL_DOCUMENTATION'
        END AS anomaly_type,
        
        -- 可疑程度评分
        CASE 
            WHEN cl.documentation IS NULL OR cl.documentation = ''
            THEN 10
            
            WHEN cl."generalCommentCharCount" < 5
            THEN 8
            
            WHEN cl."generalCommentCharCount" < 10
            THEN 6
            
            WHEN cl."generalCommentCharCount" > 1000
            THEN 7
            
            WHEN LOWER(cl.documentation) IN ('ok', 'fine', 'good', 'completed', 'done', 'finished')
            THEN 5
            
            WHEN LENGTH(cl.documentation) > 0 AND cl."generalCommentCharCount" = 0
            THEN 9
            
            WHEN cl.documentation LIKE '%copy%' OR cl.documentation LIKE '%paste%'
            THEN 8
            
            WHEN cl.documentation LIKE '%same%' OR cl.documentation LIKE '%identical%'
            THEN 7
            
            WHEN cl.documentation ~ '^[A-Za-z\s]+$' AND LENGTH(cl.documentation) < 20
            THEN 4
            
            ELSE 1
        END AS suspicion_score,
        
        -- 访问状态
        cl.status AS visit_status
        
    FROM 
        "Caregiver" c
    JOIN 
        "Carelog" cl ON c."caregiverId" = cl."caregiverId"
    WHERE 
        cl."clockInActualDatetime" IS NOT NULL 
        AND cl."clockOutActualDatetime" IS NOT NULL
        AND cl."clockInActualDatetime" < cl."clockOutActualDatetime"
        AND cl.documentation IS NOT NULL 
        AND cl.documentation != ''
        AND cl."generalCommentCharCount" > 0  -- 只分析有实际文档内容的记录
),

caregiver_suspicious_stats AS (
    SELECT 
        "caregiverId",
        caregiver_name,
        email,
        "phoneNumber",
        status,
        

        COUNT(*) AS total_documented_visits,

        COUNT(CASE WHEN anomaly_type != 'NORMAL_DOCUMENTATION' THEN 1 END) AS suspicious_records,
        COUNT(CASE WHEN anomaly_type = 'MISSING_DOCUMENTATION' THEN 1 END) AS missing_docs,
        COUNT(CASE WHEN anomaly_type = 'VERY_SHORT_DOCUMENTATION' THEN 1 END) AS very_short_docs,
        COUNT(CASE WHEN anomaly_type = 'SHORT_DOCUMENTATION' THEN 1 END) AS short_docs,
        COUNT(CASE WHEN anomaly_type = 'EXCESSIVELY_LONG' THEN 1 END) AS excessively_long_docs,
        COUNT(CASE WHEN anomaly_type = 'GENERIC_COMMENT' THEN 1 END) AS generic_comments,
        COUNT(CASE WHEN anomaly_type = 'LENGTH_MISMATCH' THEN 1 END) AS length_mismatches,
        COUNT(CASE WHEN anomaly_type = 'SUSPECTED_COPY_PASTE' THEN 1 END) AS copy_paste_suspected,
        COUNT(CASE WHEN anomaly_type = 'DUPLICATE_CONTENT_SUSPECTED' THEN 1 END) AS duplicate_suspected,
        COUNT(CASE WHEN anomaly_type = 'TOO_GENERIC' THEN 1 END) AS too_generic,
        

        ROUND(100.0 * COUNT(CASE WHEN anomaly_type != 'NORMAL_DOCUMENTATION' THEN 1 END) / COUNT(*), 2) AS suspicious_percentage,
        ROUND(100.0 * COUNT(CASE WHEN anomaly_type = 'MISSING_DOCUMENTATION' THEN 1 END) / COUNT(*), 2) AS missing_percentage,
        ROUND(100.0 * COUNT(CASE WHEN anomaly_type IN ('VERY_SHORT_DOCUMENTATION', 'SHORT_DOCUMENTATION') THEN 1 END) / COUNT(*), 2) AS short_percentage,
        ROUND(100.0 * COUNT(CASE WHEN anomaly_type = 'GENERIC_COMMENT' THEN 1 END) / COUNT(*), 2) AS generic_percentage,
        
  
        ROUND(AVG(suspicion_score), 2) AS avg_suspicion_score,
        ROUND(MAX(suspicion_score), 2) AS max_suspicion_score,
        
 
        MIN(visit_date) AS first_doc_date,
        MAX(visit_date) AS last_doc_date,
        COUNT(DISTINCT visit_date) AS doc_active_days
        
    FROM suspicious_analysis
    
    GROUP BY "caregiverId", caregiver_name, email, "phoneNumber", status
    
    HAVING COUNT(*) >= 3 
)

SELECT 
    "caregiverId",
    caregiver_name,
    email,
    "phoneNumber",
    status,
    

    total_documented_visits,
    suspicious_records,
    suspicious_percentage,
    

    missing_docs,
    very_short_docs,
    short_docs,
    excessively_long_docs,
    generic_comments,
    length_mismatches,
    copy_paste_suspected,
    duplicate_suspected,
    too_generic,
    

    missing_percentage,
    short_percentage,
    generic_percentage,
    

    avg_suspicion_score,
    max_suspicion_score,
    

    first_doc_date,
    last_doc_date,
    doc_active_days,
    

    CASE 
        WHEN suspicious_percentage >= 80
        THEN 'HIGHLY_SUSPICIOUS'
        
        WHEN suspicious_percentage >= 60
        THEN 'VERY_SUSPICIOUS'
        
        WHEN suspicious_percentage >= 40
        THEN 'SUSPICIOUS'
        
        WHEN suspicious_percentage >= 20
        THEN 'MODERATELY_SUSPICIOUS'
        
        WHEN suspicious_percentage >= 10
        THEN 'SLIGHTLY_SUSPICIOUS'
        
        ELSE 'NORMAL'
    END AS suspicion_level,
    

    CASE 
        WHEN missing_percentage >= 50
        THEN 'FREQUENT_MISSING_DOCS'
        
        WHEN short_percentage >= 60
        THEN 'FREQUENT_SHORT_DOCS'
        
        WHEN generic_percentage >= 40
        THEN 'FREQUENT_GENERIC_COMMENTS'
        
        WHEN copy_paste_suspected > 0
        THEN 'COPY_PASTE_DETECTED'
        
        WHEN duplicate_suspected > 0
        THEN 'DUPLICATE_CONTENT_DETECTED'
        
        WHEN length_mismatches > 0
        THEN 'LENGTH_MISMATCH_DETECTED'
        
        WHEN excessively_long_docs > 0
        THEN 'EXCESSIVELY_LONG_DOCS'
        
        ELSE 'MINOR_ISSUES'
    END AS primary_issue_type,
    

    ROUND(
        (suspicious_percentage * 0.4) + 
        (avg_suspicion_score * 5) +     
        (GREATEST(0, suspicious_records - 5) * 2) + 
        (CASE WHEN max_suspicion_score >= 8 THEN 10 ELSE 0 END),         2
    ) AS suspicion_risk_score,

    CASE 
        WHEN ((suspicious_percentage * 0.4) + (avg_suspicion_score * 5) + (GREATEST(0, suspicious_records - 5) * 2) + (CASE WHEN max_suspicion_score >= 8 THEN 10 ELSE 0 END)) >= 80
        THEN 'IMMEDIATE_INVESTIGATION_REQUIRED'
        
        WHEN ((suspicious_percentage * 0.4) + (avg_suspicion_score * 5) + (GREATEST(0, suspicious_records - 5) * 2) + (CASE WHEN max_suspicion_score >= 8 THEN 10 ELSE 0 END)) >= 60
        THEN 'DETAILED_REVIEW_NEEDED'
        
        WHEN ((suspicious_percentage * 0.4) + (avg_suspicion_score * 5) + (GREATEST(0, suspicious_records - 5) * 2) + (CASE WHEN max_suspicion_score >= 8 THEN 10 ELSE 0 END)) >= 40
        THEN 'MONITOR_CLOSELY'
        
        WHEN ((suspicious_percentage * 0.4) + (avg_suspicion_score * 5) + (GREATEST(0, suspicious_records - 5) * 2) + (CASE WHEN max_suspicion_score >= 8 THEN 10 ELSE 0 END)) >= 20
        THEN 'PERIODIC_REVIEW'
        
        ELSE 'NORMAL_MONITORING'
    END AS recommended_action,
    

    CASE 
        WHEN suspicious_percentage >= 80
        THEN 'Highly suspicious documentation patterns with ' || suspicious_percentage || '% of records showing issues'
        
        WHEN suspicious_percentage >= 60
        THEN 'Very suspicious documentation patterns with ' || suspicious_percentage || '% of records showing issues'
        
        WHEN suspicious_percentage >= 40
        THEN 'Suspicious documentation patterns with ' || suspicious_percentage || '% of records showing issues'
        
        WHEN missing_percentage >= 50
        THEN 'Frequently missing documentation in ' || missing_percentage || '% of visits'
        
        WHEN short_percentage >= 60
        THEN 'Frequently provides very short documentation in ' || short_percentage || '% of visits'
        
        WHEN generic_percentage >= 40
        THEN 'Frequently uses generic comments in ' || generic_percentage || '% of visits'
        
        WHEN copy_paste_suspected > 0
        THEN 'Suspected copy-paste behavior detected in documentation'
        
        WHEN duplicate_suspected > 0
        THEN 'Suspected duplicate content in documentation'
        
        ELSE 'Minor documentation issues detected'
    END AS insight_description

FROM caregiver_suspicious_stats

ORDER BY 
    ((suspicious_percentage * 0.4) + (avg_suspicion_score * 5) + (GREATEST(0, suspicious_records - 5) * 2) + (CASE WHEN max_suspicion_score >= 8 THEN 10 ELSE 0 END)) DESC,  
    suspicious_percentage DESC, 
    suspicious_records DESC   

LIMIT 50; 