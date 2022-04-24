INSERT INTO EFIR.MOEX_SEC_SES_HIST (
    MOEX_SEC_SES_ID ,
    SECURITYID ,
    BOARDID ,
    ID_ISS ,
    SHORTNAME ,
    TRADINGSESSION ,
    TYPECODE ,
    MATDATE ,
    BEGIN_SESSION_DATE ,
    END_SESSION_DATE ,
    MAX_LASTTRADEDATE ,
    IS_TRADED ,
    LISTED_FROM ,
    LISTED_TILL ,
    ADD_DATE ,
    UPDATE_DATE ,
    DT
    )
SELECT
    mss.id,
    mss.SECURITYID ,
    mss.BOARDID ,
    mss.ID_ISS ,
    mss.SHORTNAME ,
    mss.TRADINGSESSION ,
    mss.TYPECODE ,
    mss.MATDATE ,
    mss.BEGIN_SESSION_DATE ,
    mss.END_SESSION_DATE ,
    mss.MAX_LASTTRADEDATE ,
    mss.IS_TRADED ,
    mss.LISTED_FROM ,
    mss.LISTED_TILL ,
    mss.ADD_DATE ,
    mss.UPDATE_DATE ,
    SYSDATE --DT
FROM EFIR.MOEX_SECURITIES_SESSIONS mss
JOIN (
    --EFIR.TP_CBONDS_MICEX_OFFICIAL
    SELECT
        f.securityid,
        f.boardid,
        f.tradingsession,
        f.id_iss,
        f.matdate,
        f.max_lasttradedate,
        b.listed_till
    FROM (
        SELECT * FROM (
                SELECT
                    securityid,
                    boardid,
                    tradingsession,
                    id_iss,
                    matdate,
                    dt,
                    MIN(dt) OVER(PARTITION BY boardid, securityid, tradingsession) begin_session_date,
                    MAX(lasttradedate) OVER(PARTITION BY boardid, securityid, tradingsession) max_lasttradedate
                FROM EFIR.TP_CBONDS_MICEX_OFFICIAL
                )
        WHERE dt = begin_session_date
        ) f
    LEFT JOIN EFIR.MOEX_SECURITIES_BOARDS b ON b.secid = f.securityid
                                      AND b.boardid = f.boardid
) f ON (mss.securityid = f.securityid
    AND mss.boardid = f.boardid
    AND mss.id_iss = f.id_iss
    AND COALESCE(mss.tradingsession,-99) = COALESCE(f.tradingsession,-99)
    )
WHERE 1=1
--AND (mss.max_lasttradedate > TRUNC(SYSDATE)-3 OR mss.listed_till > TRUNC(SYSDATE)-3) AND mss.end_session_date is not null -- не Пойму о чем это условие
AND ( mss.end_session_date is not null AND (mss.MATDATE > TRUNC(SYSDATE) or mss.MATDATE is NULL) AND (mss.max_lasttradedate != f.max_lasttradedate or mss.listed_till != f.listed_till) )
;