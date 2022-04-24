MERGE INTO EFIR.MOEX_SECURITIES_SESSIONS mss
USING (
    --EFIR.TP_CBONDS_MICEX_OFFICIAL
    SELECT
        f.securityid,
        f.boardid,
        f.tradingsession,
        f.id_iss,
        f.shortname,
        f.typecode,
        f.matdate,
        f.begin_session_date,
        CASE
            WHEN matdate = TRUNC(SYSDATE) THEN dayoftrade
            WHEN matdate > TRUNC(SYSDATE) OR matdate is null THEN
                CASE
                  WHEN max_lasttradedate < TRUNC(SYSDATE)-3 OR listed_till < TRUNC(SYSDATE)-3 THEN dayoftrade
                  ELSE null
                END
        END end_session_date,
        f.max_lasttradedate,
        f.dayoftrade,
        b.is_traded,
        b.listed_from,
        b.listed_till
    FROM (
        SELECT * FROM (
                SELECT
                    securityid,
                    boardid,
                    tradingsession,
                    id_iss,
                    shortname,
                    typecode,
                    matdate,
                    dt,
                    MIN(dt) OVER(PARTITION BY boardid, securityid, tradingsession) begin_session_date,
                    MAX(dt) OVER(PARTITION BY boardid, securityid, tradingsession) dayoftrade,
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
WHEN NOT MATCHED THEN
    INSERT (
        securityid ,
        boardid ,
        tradingsession ,
        id_iss,
        shortname ,
        typecode ,
        matdate ,
        begin_session_date,
        end_session_date ,
        max_lasttradedate,
        is_traded,
        listed_from ,
        listed_till,
        add_date,
        update_date
    ) VALUES (
        f.securityid ,
        f.boardid ,
        f.tradingsession ,
        f.id_iss,
        f.shortname ,
        f.typecode ,
        f.matdate ,
        f.begin_session_date,
        f.end_session_date , --!?
        f.max_lasttradedate,
        f.is_traded,
        f.listed_from ,
        f.listed_till,
        SYSDATE,
        SYSDATE
    )
WHEN MATCHED THEN -- Не понял, где-то стоит mss. к end_session_date , где-то не стоит
    UPDATE SET
    end_session_date =
        CASE
            WHEN f.matdate <= TRUNC(SYSDATE) AND mss.end_session_date is null THEN f.dayoftrade
            WHEN ( f.matdate > TRUNC(SYSDATE) OR f.matdate is null) AND (f.max_lasttradedate < TRUNC(SYSDATE)-3 OR f.listed_till < TRUNC(SYSDATE)-3 ) AND mss.end_session_date is null THEN f.dayoftrade
            WHEN ( f.matdate > TRUNC(SYSDATE) OR f.matdate is null) AND (f.max_lasttradedate > TRUNC(SYSDATE)-3 OR f.listed_till > TRUNC(SYSDATE)-3 ) AND mss.end_session_date is null THEN null
         -- WHEN (mss.max_lasttradedate > TRUNC(SYSDATE)-3 OR mss.listed_till > TRUNC(SYSDATE)-3) AND mss.end_session_date is not null THEN null -- Мне показалось это условие непонятным и ненужным, оно похоже на недоделанное следующее
            WHEN (f.matdate > TRUNC(SYSDATE) OR f.matdate is null) AND (mss.max_lasttradedate != f.max_lasttradedate or mss.listed_till != f.listed_till) AND mss.end_session_date is not null THEN
                CASE
                    WHEN f.max_lasttradedate > TRUNC(SYSDATE)-3 OR f.listed_till > TRUNC(SYSDATE)-3 THEN null
                    WHEN f.max_lasttradedate < TRUNC(SYSDATE)-3 OR f.listed_till < TRUNC(SYSDATE)-3 THEN f.dayoftrade
                    WHEN f.max_lasttradedate is NULL OR f.listed_till is NULL THEN f.dayoftrade --Если прилетит зануление (т.е. ценную бумагу уберут), то хотя бы останется последняя максимальная дата из таблицы SESSIONS
                END
            ELSE end_session_date -- т.е. остается старой
        END,
    update_date =
      CASE
            WHEN f.matdate <= TRUNC(SYSDATE) AND mss.end_session_date is null THEN SYSDATE
            WHEN ( f.matdate > TRUNC(SYSDATE) OR f.matdate is null) AND (f.max_lasttradedate < TRUNC(SYSDATE)-3 OR f.listed_till < TRUNC(SYSDATE)-3 ) AND mss.end_session_date is null THEN SYSDATE
            WHEN ( f.matdate > TRUNC(SYSDATE) OR f.matdate is null) AND (f.max_lasttradedate > TRUNC(SYSDATE)-3 OR mss.listed_till > TRUNC(SYSDATE)-3 ) AND mss.end_session_date is not null THEN SYSDATE
            WHEN (mss.matdate > TRUNC(SYSDATE) OR mss.matdate is null) AND (mss.max_lasttradedate != f.max_lasttradedate or mss.listed_till != f.listed_till ) AND mss.end_session_date is not null THEN
                CASE
                    WHEN f.max_lasttradedate > TRUNC(SYSDATE)-3 OR f.listed_till > TRUNC(SYSDATE)-3 THEN SYSDATE
                    WHEN f.max_lasttradedate < TRUNC(SYSDATE)-3 OR f.listed_till < TRUNC(SYSDATE)-3 THEN SYSDATE
                    WHEN f.max_lasttradedate is NULL OR f.listed_till is NULL THEN SYSDATE --Если прилетит зануление (т.е. ценную бумагу уберут),то дата апдейта отразит попытку зануления.
                END

            WHEN f.max_lasttradedate > mss.max_lasttradedate THEN SYSDATE --Добавил от max_lasttradedate
            WHEN mss.max_lasttradedate is null AND f.max_lasttradedate is not null THEN SYSDATE --Добавил от max_lasttradedate
            WHEN mss.max_lasttradedate is not null AND f.max_lasttradedate is null THEN SYSDATE --Если прилетит зануление (т.е. ценную бумагу уберут),то дата апдейта отразит попытку зануления.

            WHEN f.listed_from > mss.listed_from THEN SYSDATE --Добавил от listed_from
            WHEN mss.listed_from is null AND f.listed_from is not null THEN SYSDATE
            WHEN mss.listed_from is not null AND f.listed_from is null THEN SYSDATE --Если прилетит зануление (т.е. ценную бумагу уберут),то дата апдейта отразит попытку зануления.

            WHEN f.listed_till > mss.listed_till THEN SYSDATE --Добавил от listed_till
            WHEN mss.listed_till is null AND f.listed_till is not null THEN SYSDATE
            WHEN mss.listed_till is not null AND f.listed_till is null THEN SYSDATE --Если прилетит зануление (т.е. ценную бумагу уберут),то дата апдейта отразит попытку зануления.

          ELSE mss.update_date -- т.е. остается старой
        END,

--Что ещё важно отслеживать и менять при update-ах

    max_lasttradedate =
       CASE
            WHEN f.max_lasttradedate > mss.max_lasttradedate THEN f.max_lasttradedate
            WHEN mss.max_lasttradedate is null AND f.max_lasttradedate is not null THEN f.max_lasttradedate
            WHEN mss.max_lasttradedate is not null AND f.max_lasttradedate is null THEN mss.max_lasttradedate --Если прилетит зануление (т.е. ценную бумагу уберут), то дата останется максимальной от SESSIONS
         ELSE mss.max_lasttradedate -- т.е. остается старой
       END,

    listed_from =
        CASE
            WHEN f.listed_from > mss.listed_from THEN f.listed_from
            WHEN mss.listed_from is null AND f.listed_from is not null THEN f.listed_from
            WHEN mss.listed_from is not null AND f.listed_from is null THEN mss.listed_from --Если прилетит зануление (т.е. ценную бумагу уберут), то дата останется максимальной от SESSIONS
         ELSE mss.listed_from -- т.е. остается старой
       END,

    listed_till =
        CASE
            WHEN f.listed_till > mss.listed_till THEN f.listed_till
            WHEN mss.listed_till is null AND f.listed_till is not null THEN f.listed_till
            WHEN mss.listed_till is not null AND f.listed_till is null THEN mss.listed_till --Если прилетит зануление (т.е. ценную бумагу уберут), то дата останется максимальной от SESSIONS
         ELSE mss.listed_till -- т.е. остается старой
       END