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
    MAX_DAYOFTRADE ,
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
    mss.MAX_DAYOFTRADE ,
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
        f.max_dayoftrade,
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
--Правка 23.04.22
                    MIN(dt) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) begin_session_date, --Добавил id_iss
                    MAX(dt) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) max_dayoftrade, --Была пропущена строчка, и добавил id_iss
                    MAX(lasttradedate) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) max_lasttradedate --Добавил id_iss
                FROM (
                    SELECT * FROM EFIR.TP_CBONDS_MICEX_OFFICIAL
                    UNION ALL
                    SELECT * FROM EFIR.TP_CBONDS_MICEX_OFFICIAL_EVN
                    )
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
--Правка 24.04.22
AND mss.MATDATE != f.MATDATE -- Добавил строчку, если вдруг решат "вечную" облигацию ограничить какой-то датой, или наоборот убрать дату погашения.
AND ( mss.end_session_date is not null AND (mss.MATDATE > TRUNC(SYSDATE) or mss.MATDATE is NULL) AND (mss.max_dayoftrade != f.max_dayoftrade) ) --Добавил dayoftrade ~ max(DT)
;