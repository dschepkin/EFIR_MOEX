INSERT INTO EFIR.MOEX_SEC_SES_HIST (
    MOEX_SEC_SES_ID ,
    SECURITYID ,
    BOARDID ,
    ID_ISS ,
    SHORTNAME ,
    TRADINGSESSION ,
    TYPECODE ,
    BEGIN_SESSION_DATE ,
    END_SESSION_DATE ,
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
    mss.BEGIN_SESSION_DATE ,
    mss.END_SESSION_DATE ,
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
        f.max_dayoftrade,
        b.listed_till
    FROM (
        SELECT * FROM (
                SELECT
                    securityid,
                    boardid,
                    tradingsession,
                    id_iss,
                    dt,
--Правка 23.04.22
                    MIN(dt) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) begin_session_date, --Добавил id_iss
                    MAX(dt) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) max_dayoftrade
                FROM (
                    SELECT * FROM EFIR.TP_SHARES_MICEX_OFFICIAL
                    UNION ALL
                    SELECT * FROM EFIR.TP_SHARES_MICEX_OFFICIAL_MON
                    UNION ALL
                    SELECT * FROM EFIR.TP_SHARES_MICEX_OFFICIAL_EVN
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
and mss.end_session_date is not null
and f.max_dayoftrade > mss.max_dayoftrade
;