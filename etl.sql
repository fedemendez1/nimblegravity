--create table python_challenge.df_restructured AS
WITH
temp0 AS (
    SELECT
        'drop_last' AS drop_last,
        userid,
        CAST(eventtimestamp AS TIMESTAMP_NTZ(9)) AS eventtimestamp,
        referrerurl,
        targeturl
    FROM python_challenge.data
),
temp1 AS (
    SELECT
        *,
        LAG(userid) OVER (PARTITION BY userid ORDER BY userid, eventtimestamp) AS lag_userid,
        LAG(eventtimestamp) OVER (PARTITION BY userid ORDER BY userid, eventtimestamp) AS lag_eventtimestamp,
        ROW_NUMBER() OVER (PARTITION BY drop_last ORDER BY userid, eventtimestamp) AS transition_id
    FROM temp0
    WHERE TO_DATE(CAST(eventtimestamp AS TIMESTAMP_NTZ(9))) >= '2000-01-01'
    ORDER BY userid,
             eventtimestamp
),
temp2 AS (
    SELECT
        *,
        CASE
            WHEN TIMESTAMPDIFF('seconds',lag_eventtimestamp,eventtimestamp) > 600 OR userid != lag_userid
            THEN 'New Session'
            ELSE 'Continued Session'
        END AS session_indicator
    FROM temp1
    ORDER BY userid,
             eventtimestamp
),
temp3 AS (
    SELECT
        *,
        CASE WHEN session_indicator = 'New Session' THEN 'Start' ELSE 'Filter' END AS start_session,
        CASE WHEN lead(session_indicator) OVER (PARTITION BY userid ORDER BY userid, eventtimestamp) = 'New Session' THEN 'End' ELSE 'Filter' END AS end_session,
        SUM(CASE WHEN session_indicator = 'New Session' THEN 1 ELSE 0 END) OVER (PARTITION BY drop_last ORDER BY userid, eventtimestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) + 1 AS session_id
    FROM temp2
    ORDER BY userid,
             eventtimestamp
),
temp4 AS(
    SELECT
        userid,
        eventtimestamp,
        session_id,
        transition_id,
        url_type,
        url,
        domain
FROM (
    SELECT
        userid,
        eventtimestamp,
        session_id,
        transition_id,
        'referrerurl' AS url_type,
        referrerurl AS url,
        TO_VARCHAR(GET_PATH(PARSE_URL(referrerurl,1), 'host')) AS domain
    FROM temp3
    UNION ALL
    SELECT
        userid,
        eventtimestamp,
        session_id,
        transition_id,
        'targeturl' AS url_type,
        targeturl AS url,
        TO_VARCHAR(GET_PATH(PARSE_URL(targeturl,1), 'host')) AS domain
    FROM temp3
    UNION ALL
    SELECT
        userid,
        eventtimestamp,
        session_id,
        transition_id,
        'start_session' AS url_type,
        start_session AS url,
        'Start' AS domain
    FROM temp3
    UNION ALL
    SELECT
        userid,
        eventtimestamp,
        session_id,
        transition_id,
        'end_session' AS url_type,
        end_session AS url,
        'End' AS domain
    FROM temp3
) t
WHERE NOT (url_type = 'end_session' AND url = 'Filter')
AND NOT (url_type = 'referrerurl' AND url = '')
AND NOT (url_type = 'start_session' AND url = 'Filter')
ORDER BY userid,
         session_id,
         eventtimestamp,
         transition_id,
         CASE url_type
            WHEN 'start_session' THEN 1
            WHEN 'referrerurl' THEN 2
            WHEN 'targeturl' THEN 3
            WHEN 'end_session' THEN 4
         ELSE null
         END
),
temp5 AS (
    SELECT
        *,
        LAG(userid) OVER (PARTITION BY userid, session_id ORDER BY userid, session_id, eventtimestamp, transition_id, CASE url_type WHEN 'start_session' THEN 1 WHEN 'referrerurl' THEN 2 WHEN 'targeturl' THEN 3 WHEN 'end_session' THEN 4 ELSE null END) AS lag_userid,
        LAG(session_id) OVER (PARTITION BY userid, session_id ORDER BY userid, session_id, eventtimestamp, transition_id, CASE url_type WHEN 'start_session' THEN 1 WHEN 'referrerurl' THEN 2 WHEN 'targeturl' THEN 3 WHEN 'end_session' THEN 4 ELSE null END) AS lag_session_id,
        LAG(domain) OVER (PARTITION BY userid, session_id ORDER BY userid, session_id, eventtimestamp, transition_id, CASE url_type WHEN 'start_session' THEN 1 WHEN 'referrerurl' THEN 2 WHEN 'targeturl' THEN 3 WHEN 'end_session' THEN 4 ELSE null END) AS lag_domain,
        LAG(url) OVER (PARTITION BY userid, session_id ORDER BY userid, session_id, eventtimestamp, transition_id, CASE url_type WHEN 'start_session' THEN 1 WHEN 'referrerurl' THEN 2 WHEN 'targeturl' THEN 3 WHEN 'end_session' THEN 4 ELSE null END) AS lag_url,
        LAG(url_type) OVER (PARTITION BY userid, session_id ORDER BY userid, session_id, eventtimestamp, transition_id, CASE url_type WHEN 'start_session' THEN 1 WHEN 'referrerurl' THEN 2 WHEN 'targeturl' THEN 3 WHEN 'end_session' THEN 4 ELSE null END) AS lag_url_type,
        LAG(transition_id) OVER (PARTITION BY userid, session_id ORDER BY userid, session_id, eventtimestamp, transition_id, CASE url_type WHEN 'start_session' THEN 1 WHEN 'referrerurl' THEN 2 WHEN 'targeturl' THEN 3 WHEN 'end_session' THEN 4 ELSE null END) AS lag_transition_id
    FROM temp4
),
temp6 AS (
    SELECT 
        *,
        CASE WHEN userid = lag_userid AND session_id = lag_session_id AND domain = lag_domain THEN 1 ELSE 0 END AS domain_indicator
    FROM temp5
    WHERE NOT ((url_type = 'referrerurl') AND (url = lag_url) AND (lag_url_type = 'targeturl') AND (transition_id = lag_transition_id + 1))
    ORDER BY userid,
         session_id,
         eventtimestamp,
         transition_id,
         CASE url_type
            WHEN 'start_session' THEN 1
            WHEN 'referrerurl' THEN 2
            WHEN 'targeturl' THEN 3
            WHEN 'end_session' THEN 4
         ELSE null
         END
),
temp7 AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY userid, session_id ORDER BY userid, session_id, eventtimestamp, transition_id, CASE url_type WHEN 'start_session' THEN 1 WHEN 'referrerurl' THEN 2 WHEN 'targeturl' THEN 3 WHEN 'end_session' THEN 4 ELSE null END) - ROW_NUMBER() OVER (PARTITION BY userid, session_id, domain_indicator ORDER BY userid, session_id, eventtimestamp, transition_id, CASE url_type WHEN 'start_session' THEN 1 WHEN 'referrerurl' THEN 2 WHEN 'targeturl' THEN 3 WHEN 'end_session' THEN 4 ELSE null END) AS grp
    FROM temp6
),
temp8 AS (
    SELECT
        *,
        CASE WHEN domain_indicator = 0 THEN 1 ELSE ROW_NUMBER() OVER (PARTITION BY userid, session_id, domain_indicator, grp ORDER BY userid, session_id, eventtimestamp, transition_id, CASE url_type WHEN 'start_session' THEN 1 WHEN 'referrerurl' THEN 2 WHEN 'targeturl' THEN 3 WHEN 'end_session' THEN 4 ELSE null END) END AS cum_clicks_aux
    FROM temp7
),
temp9 AS (
    SELECT
        *,
        CASE WHEN domain_indicator = 1 THEN cum_clicks_aux + 1 ELSE cum_clicks_aux END AS cum_clicks
    FROM temp8
)

SELECT
    userid,
    eventtimestamp,
    session_id,
    transition_id,
    url_type,
    url,
    domain,
    cum_clicks
FROM temp9
ORDER BY userid,
         session_id,
         eventtimestamp,
         transition_id,
         CASE url_type
            WHEN 'start_session' THEN 1
            WHEN 'referrerurl' THEN 2
            WHEN 'targeturl' THEN 3
            WHEN 'end_session' THEN 4
        ELSE null
        END





