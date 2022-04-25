set serveroutput on
DECLARE
    l_sysdate DATE := SYSDATE;
    l_ins_row INTEGER;
    l_error VARCHAR2(300);
BEGIN
    INSERT INTO EFIR.MOEX_SECURITIES_SESSIONS (
        SECURITYID ,
        BOARDID ,
        TRADINGSESSION ,
        ID_ISS,
        SHORTNAME ,
        TYPECODE ,
        MATDATE ,
        BEGIN_SESSION_DATE,
        MAX_DAYOFTRADE,      --ДОБАВИЛ 25.04.22
        END_SESSION_DATE ,
        MAX_LASTTRADEDATE,
        IS_TRADED,
        LISTED_FROM ,
        LISTED_TILL,
        ADD_DATE,
        UPDATE_DATE
    ) select
        securityid          SECURITYID,
        boardid             BOARDID,
        tradingsession      TRADINGSESSION,
        id_iss              ID_ISS,
        shortname           SHORTNAME,
        typecode            TYPECODE,
        matdate             MATDATE,
        begin_session_date  BEGIN_SESSION_DATE,
        MAX_DAYOFTRADE,                            --!!! Тоже нужно как-то добавить, который max(DT) из родительских таблиц
        end_session_date    END_SESSION_DATE,
        MAX_LASTTRADEDATE,
        IS_TRADED,
        listed_from         LISTED_FROM,
        listed_till         LISTED_TILL,
        l_sysdate            ADD_DATE,
        l_sysdate            UPDATE_DATE
    from (
        --EFIR.TP_CBONDS_MICEX_OFFICIAL
        SELECT
            f.securityid,
            f.boardid,
            f.tradingsession,
            f.id_iss,
            f.shortname,
            f.typecode,
            f.matdate,
            f.begin_session_date, -- Может быть такое, что begin_session_date > чем последняя дата торговли. Например SECURITYID-XS0810596832 BOARD-PTOD TRADINGSESSION-2 (такое однозначно к Анатолию), Возможно, нужно будет прописывать отдельные case's  для begin_session_date под такое
            f.max_dayoftrade, -- ДОБАВИЛ 25.04.22
            case
               when matdate <= trunc (SYSDATE) then max_dayoftrade --Я упростил условие, схлопнув два предыдущих
               when matdate > trunc (SYSDATE) then
                 case
-- Правка 25.04.22
                   when max_dayoftrade > trunc (SYSDATE)-3 then NULL --Смотреть остаётся только по max(DT)
                   when max_dayoftrade < trunc (SYSDATE)-3 then max_dayoftrade
                  end
               when matdate is NULL then --Пока не объединяем с кейсом "> trunc (SYSDATE)", т.к. Анатолий решит по итогу, что делать с "Вечными" облигациями. Мы пока только покажем, что многие из них "заморожены" с крайней датой торговли
                 case
                   when max_dayoftrade > trunc (SYSDATE)-3 then NULL --Смотреть остаётся только по max(DT)
                   when max_dayoftrade < trunc (SYSDATE)-3 then max_dayoftrade
                end
            end end_session_date,
            max_lasttradedate,
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
-- Правка 25.04.22
                      MIN(dt) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) begin_session_date, --разве в ключ не включили id_iss? Добавлен id_iss
                      MAX(dt) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) max_dayoftrade, --Добавлен id_iss
                      MAX(lasttradedate) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) max_lasttradedate --Добавлен id_iss
                    FROM EFIR.TP_CBONDS_MICEX_OFFICIAL
                    )
            WHERE dt = begin_session_date
            ) f
        LEFT JOIN EFIR.MOEX_SECURITIES_BOARDS b ON b.secid = f.securityid
                                                AND b.boardid = f.boardid
        UNION ALL
        --EFIR.TP_CBONDS_MICEX_OFFICIAL_EVN
        SELECT
            f.securityid,
            f.boardid,
            f.tradingsession,
            f.id_iss,
            f.shortname,
            f.typecode,
            f.matdate,
            f.begin_session_date,
            f.max_dayoftrade, -- ДОБАВИЛ 25.04.22
            case
             when matdate <= trunc (SYSDATE)-3 then max_dayoftrade --Я упростил условие, схлопнув два предыдущих
             when matdate > trunc (SYSDATE) then
                 case
-- Правка 25.04.22
                   when max_dayoftrade > trunc (SYSDATE)-3 then NULL --Смотреть остаётся только по max(DT)
                   when max_dayoftrade < trunc (SYSDATE)-3 then max_dayoftrade
                end
             when matdate is NULL then --Пока не объединяем с кейсом "> trunc (SYSDATE)", т.к. Анатолий решит по итогу, что делать с "Вечными" облигациями. Мы пока только покажем, что многие из них "заморожены" с крайней датой торговли
                 case
                    when max_dayoftrade > trunc (SYSDATE)-3 then NULL --Смотреть остаётся только по max(DT)
                   when max_dayoftrade < trunc (SYSDATE)-3 then max_dayoftrade
                end
            end end_session_date,
            max_lasttradedate,
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
-- Правка 25.04.22
                      MIN(dt) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) begin_session_date, --Добавлен id_iss
                      MAX(dt) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) max_dayoftrade, --Добавлен id_ISS
                      MAX(lasttradedate) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) max_lasttradedate --Добавлен id_ISS
                    FROM EFIR.TP_CBONDS_MICEX_OFFICIAL_EVN
                    )
            WHERE dt = begin_session_date
            ) f
        LEFT JOIN EFIR.MOEX_SECURITIES_BOARDS b ON b.secid = f.securityid
                                                AND b.boardid = f.boardid
        UNION ALL --ДЛЯ АКЦИЙ (SHARES)
        --EFIR.TP_SHARES_MICEX_OFFICIAL
        SELECT
            f.securityid,
            f.boardid,
            f.tradingsession,
            f.id_iss,
            f.shortname,
            f.typecode,
            null matdate,
            f.begin_session_date,
            f.max_dayoftrade,  -- ДОБАВИЛ 25.04.22
            case
                when max_dayoftrade > trunc (SYSDATE)-3 then NULL
                when max_dayoftrade < trunc (SYSDATE)-3 then max_dayoftrade
            end end_session_date,
            null max_lasttradedate,
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
                  dt,
-- Правка 25.04.22
                  MIN(dt) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) begin_session_date, --Добавлен id_iss
                  MAX(dt) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) max_dayoftrade --Добавлен id_iss
                FROM EFIR.TP_SHARES_MICEX_OFFICIAL
                ) --ДЛЯ АКЦИЙ (SHARES)
            WHERE dt = begin_session_date
            ) f
        LEFT JOIN EFIR.MOEX_SECURITIES_BOARDS b ON b.secid = f.securityid
                                               AND b.boardid = f.boardid
        UNION ALL
        --EFIR.TP_SHARES_MICEX_OFFICIAL_MON
        SELECT
            f.securityid,
            f.boardid,
            f.tradingsession,
            f.id_iss,
            f.shortname,
            f.typecode,
            null matdate,
            f.begin_session_date,
            f.max_dayoftrade,  -- ДОБАВИЛ 25.04.22
            case
                when max_dayoftrade > trunc (SYSDATE)-3 then NULL
                when max_dayoftrade < trunc (SYSDATE)-3 then max_dayoftrade
            end end_session_date,
            null max_lasttradedate,
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
                  dt,
                  MIN(dt) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) begin_session_date, --Добавлен id_iss
                  MAX(dt) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) max_dayoftrade --Добавлен id_iss
                FROM EFIR.TP_SHARES_MICEX_OFFICIAL_MON
                ) --ДЛЯ АКЦИЙ (SHARES)
            WHERE dt = begin_session_date
            ) f
        LEFT JOIN EFIR.MOEX_SECURITIES_BOARDS b ON b.secid = f.securityid
                                               AND b.boardid = f.boardid
        UNION ALL
        --EFIR.TP_SHARES_MICEX_OFFICIAL_EVN
        SELECT
            f.securityid,
            f.boardid,
            f.tradingsession,
            f.id_iss,
            f.shortname,
            f.typecode,
            null matdate,
            f.begin_session_date,
            f.max_dayoftrade,  -- ДОБАВИЛ 25.04.22
            case
                when max_dayoftrade > trunc (SYSDATE)-3 then NULL
                when max_dayoftrade < trunc (SYSDATE)-3 then max_dayoftrade
            end end_session_date,
            null max_lasttradedate,
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
                  dt,
-- Правка 25.04.22
                  MIN(dt) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) begin_session_date, --Добавлен id_iss
                  MAX(dt) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) max_dayoftrade --Добавлен id_iss
                FROM EFIR.TP_SHARES_MICEX_OFFICIAL_EVN
                ) --ДЛЯ АКЦИЙ (SHARES)
            WHERE dt = begin_session_date
            ) f
        LEFT JOIN EFIR.MOEX_SECURITIES_BOARDS b ON b.secid = f.securityid
                                               AND b.boardid = f.boardid
    );

    l_ins_row := SQL%ROWCOUNT;
    dbms_output.put_line('Inserted '||l_ins_row||' rows.');

    EXCEPTION
    WHEN OTHERS THEN
        l_error := SQLERRM;
        RAISE_APPLICATION_ERROR(-20001,'Insert failed. Error: '|| l_error);
END;
/
COMMIT
/