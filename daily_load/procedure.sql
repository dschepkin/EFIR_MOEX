CREATE OR REPLACE PROCEDURE EFIR.MOEX_SEC_SESSION
IS
    l_sysdate   DATE := SYSDATE;
    l_error     VARCHAR2(300);
BEGIN
    --Шаг 1: находим строки, которые будут обновляться на шаге 2, и копируем их в историческую таблицу
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
    --Правка 24.04.22
    AND mss.MATDATE != f.MATDATE -- Добавил строчку, если вдруг решат "вечную" облигацию ограничить какой-то датой, или наоборот убрать дату погашения.
    AND ( mss.end_session_date is not null AND (mss.MATDATE > TRUNC(SYSDATE) or mss.MATDATE is NULL) AND (mss.max_dayoftrade != f.max_dayoftrade) ) --Добавил dayoftrade ~ max(DT)
    ;

    --Шаг 2: добавляем строки, которых нет, а также обновляем удовлетворяющие условию
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
            f.max_dayoftrade, -- ДОБАВИЛ 25.04.22
    -- Скопировал из initial_load_v6
    -- Не понял почему участвует только таблица EFIR.TP_CBONDS_MICEX_OFFICIAL и нет EFIR.TP_CBONDS_MICEX_OFFICIAL_EVN
            CASE
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
            END end_session_date,
            f.max_lasttradedate,
    --        f.dayoftrade, Убрал. наверное это теперь повторяет f.max_dayoftrade
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
    --Правка 25.04.22
                        MIN(dt) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) begin_session_date, --Добавил id_iss
                        MAX(dt) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) max_dayoftrade, --Добавил id_iss
                        MAX(lasttradedate) OVER(PARTITION BY boardid, securityid, tradingsession, id_iss) max_lasttradedate --Добавил id_iss
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
            max_dayoftrade, -- ДОБАВИЛ 25.04.22
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
            f.max_dayoftrade, -- ДОБАВИЛ 25.04.22
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
    --Правка 25.04.22
               WHEN f.matdate <= TRUNC(SYSDATE) AND end_session_date is null THEN f.max_dayoftrade
               WHEN f.matdate > TRUNC(SYSDATE) AND end_session_date is null AND (mss.max_dayoftrade != f.max_dayoftrade) THEN
                     case
                       when f.max_dayoftrade > trunc (SYSDATE)-3 then NULL
                       when f.max_dayoftrade < trunc (SYSDATE)-3 then f.max_dayoftrade
                     end
               WHEN f.matdate is NULL AND end_session_date is null AND (mss.max_dayoftrade != f.max_dayoftrade) THEN --Пока не объединяем с кейсом "> trunc (SYSDATE)", т.к. Анатолий решит по итогу, что делать с "Вечными" облигациями. Мы пока только покажем, что многие из них "заморожены" с крайней датой торговли
                     case
                       when f.max_dayoftrade > trunc (SYSDATE)-3 then NULL
                       when f.max_dayoftrade < trunc (SYSDATE)-3 then f.max_dayoftrade
                     end

               WHEN f.matdate > TRUNC(SYSDATE) AND end_session_date is NOT NULL AND (mss.max_dayoftrade != f.max_dayoftrade) THEN
                    case
                       when f.max_dayoftrade > trunc (SYSDATE)-3 then NULL
                       when f.max_dayoftrade < trunc (SYSDATE)-3 then f.max_dayoftrade
                     end
               WHEN f.matdate is NULL AND end_session_date is NOT NULL AND (mss.max_dayoftrade != f.max_dayoftrade) THEN --Пока не объединяем с кейсом "> trunc (SYSDATE)", т.к. Анатолий решит по итогу, что делать с "Вечными" облигациями. Мы пока только покажем, что многие из них "заморожены" с крайней датой торговли
                    case
                       when f.max_dayoftrade > trunc (SYSDATE)-3 then NULL
                       when f.max_dayoftrade < trunc (SYSDATE)-3 then f.max_dayoftrade
                     end
                ELSE end_session_date -- т.е. остается старое значение
            END,

    --Что ещё важно отслеживать и менять при update-ах

        max_lasttradedate =
           CASE
                WHEN f.max_lasttradedate > mss.max_lasttradedate THEN f.max_lasttradedate
                WHEN mss.max_lasttradedate is null AND f.max_lasttradedate is not null THEN f.max_lasttradedate
                WHEN mss.max_lasttradedate is not null AND f.max_lasttradedate is null THEN mss.max_lasttradedate --Если прилетит зануление (т.е. ценную бумагу уберут), то дата останется старой максимальной от SESSIONS
             ELSE max_lasttradedate -- т.е. остается старой
           END,

        listed_from =
            CASE
                WHEN f.listed_from > mss.listed_from THEN f.listed_from
                WHEN mss.listed_from is null AND f.listed_from is not null THEN f.listed_from
                WHEN mss.listed_from is not null AND f.listed_from is null THEN mss.listed_from --Если прилетит зануление (т.е. ценную бумагу уберут), то дата останется старой максимальной от SESSIONS
             ELSE listed_from -- т.е. остается старой
           END,

        listed_till =
            CASE
                WHEN f.listed_till > mss.listed_till THEN f.listed_till
                WHEN mss.listed_till is null AND f.listed_till is not null THEN f.listed_till
                WHEN mss.listed_till is not null AND f.listed_till is null THEN mss.listed_till --Если прилетит зануление (т.е. ценную бумагу уберут), то дата останется старой максимальной от SESSIONS
             ELSE listed_till -- т.е. остается старой
           END,

    --Правка 25.04.22

         matdate = --На случай, если решат "вечную" облигацию ограничить какой-то датой., или наоборот, из обычной облигации, сделать вечную
            CASE
                WHEN mss.MATDATE != f.MATDATE then f.MATDATE
                ELSE matdate -- т.е. остается старой
            END,

         max_dayoftrade =
             CASE
                WHEN f.max_dayoftrade > mss.max_dayoftrade then f.max_dayoftrade
                ELSE mss.max_dayoftrade -- т.е. остается старой
             END,

        update_date =
          CASE
    --Правка 25.04.22
               WHEN f.max_lasttradedate > mss.max_lasttradedate THEN SYSDATE --Добавил от max_lasttradedate
               WHEN mss.max_lasttradedate is null AND f.max_lasttradedate is not null THEN SYSDATE
               WHEN mss.max_lasttradedate is not null AND f.max_lasttradedate is null THEN SYSDATE --Если прилетит зануление (т.е. ценную бумагу уберут),то дата update отразит попытку зануления.
               WHEN f.listed_from > mss.listed_from THEN SYSDATE --Добавил от listed_from
               WHEN mss.listed_from is null AND f.listed_from is not null THEN SYSDATE
               WHEN mss.listed_from is not null AND f.listed_from is null THEN SYSDATE --Если прилетит зануление (т.е. ценную бумагу уберут),то дата update отразит попытку зануления.
               WHEN f.listed_till > mss.listed_till THEN SYSDATE --Добавил от listed_till
               WHEN mss.listed_till is null AND f.listed_till is not null THEN SYSDATE
               WHEN mss.listed_till is not null AND f.listed_till is null THEN SYSDATE --Если прилетит зануление (т.е. ценную бумагу уберут),то дата update отразит попытку зануления.
               WHEN mss.MATDATE != f.MATDATE THEN SYSDATE --Добавил от matdate
               WHEN f.max_dayoftrade > mss.max_dayoftrade THEN SYSDATE --Добавил от max_dayoftrade

               WHEN f.matdate <= TRUNC(SYSDATE) AND end_session_date is null THEN SYSDATE
               WHEN f.matdate > TRUNC(SYSDATE) AND end_session_date is null AND (mss.max_dayoftrade != f.max_dayoftrade) THEN
                     case
                       when f.max_dayoftrade > trunc (SYSDATE)-3 then SYSDATE
                       when f.max_dayoftrade < trunc (SYSDATE)-3 then SYSDATE
                     end
               WHEN f.matdate is NULL AND end_session_date is null AND (mss.max_dayoftrade != f.max_dayoftrade) THEN --Пока не объединяем с кейсом "> trunc (SYSDATE)", т.к. Анатолий решит по итогу, что делать с "Вечными" облигациями. Мы пока только покажем, что многие из них "заморожены" с крайней датой торговли
                     case
                       when max_dayoftrade > trunc (SYSDATE)-3 then SYSDATE
                       when max_dayoftrade < trunc (SYSDATE)-3 then SYSDATE
                     end

               WHEN f.matdate > TRUNC(SYSDATE) AND end_session_date is NOT NULL AND (mss.max_dayoftrade != f.max_dayoftrade) THEN
                    case
                       when max_dayoftrade > trunc (SYSDATE)-3 then SYSDATE
                       when max_dayoftrade < trunc (SYSDATE)-3 then SYSDATE
                     end
               WHEN f.matdate is NULL AND end_session_date is NOT NULL AND (mss.max_dayoftrade != f.max_dayoftrade) THEN --Пока не объединяем с кейсом "> trunc (SYSDATE)", т.к. Анатолий решит по итогу, что делать с "Вечными" облигациями. Мы пока только покажем, что многие из них "заморожены" с крайней датой торговли
                    case
                       when max_dayoftrade > trunc (SYSDATE)-3 then SYSDATE
                       when max_dayoftrade < trunc (SYSDATE)-3 then SYSDATE
                     end
                ELSE update_date -- т.е. остается старое значение
            END
    ;

        EXCEPTION
        WHEN OTHERS THEN
            l_error := SQLERRM;
            RAISE_APPLICATION_ERROR(-20001,'Failed EFIR.MOEX_SEC_SESSION actualization' || l_error);

END;
/