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
            f.begin_session_date,
            case
            when matdate <= trunc (SYSDATE) then dayoftrade --Я упростил условие, схлопнув два предыдущих
            when matdate > trunc (SYSDATE)
            then
                case
                  when max_lasttradedate < trunc (SYSDATE)-3 or listed_till < trunc (SYSDATE)-3 then dayoftrade --Где есть lasttradedate или listed_till ранее чем 3 дня
                  when max_lasttradedate > trunc (SYSDATE)-3 or listed_till > trunc (SYSDATE)-3 then NULL --Где одна из дат торговли максимально близка к текущей дате
                  when max_lasttradedate is NULL and listed_till is NULL then NULL --Для разбора ситуации
                end
            when matdate is NULL
            then
                case
                when max_lasttradedate < trunc (SYSDATE)-3 or listed_till < trunc (SYSDATE)-3 then dayoftrade --Где есть lasttradedate или listed_till ранее чем 3 дня
                when max_lasttradedate > trunc (SYSDATE)-3 or listed_till > trunc (SYSDATE)-3 then NULL --Где одна из дат торговли максимально близка к текущей дате
                when max_lasttradedate is NULL and listed_till is NULL then NULL ----Для разбора ситуации
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
                      MIN(dt) OVER(PARTITION BY boardid, securityid, tradingsession) begin_session_date,
                      MAX(dt) OVER(PARTITION BY boardid, securityid, tradingsession) dayoftrade,
					  MAX(lasttradedate) OVER(PARTITION BY boardid, securityid, tradingsession) max_lasttradedate
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
            case
            when matdate <= trunc (SYSDATE)-3 then dayoftrade --Я упростил условие, схлопнув два предыдущих
            when matdate > trunc (SYSDATE)
            then
                case
                  when max_lasttradedate < trunc (SYSDATE)-3 or listed_till < trunc (SYSDATE)-3 then dayoftrade --Где есть lasttradedate или listed_till ранее чем 3 дня
                  when max_lasttradedate > trunc (SYSDATE)-3 or listed_till > trunc (SYSDATE)-3 then NULL --Где одна из дат торговли максимально близка к текущей дате
                  when max_lasttradedate is NULL and listed_till is NULL then NULL --Для разбора ситуации
                end
            when matdate is NULL
            then
                case
                when max_lasttradedate < trunc (SYSDATE)-3 or listed_till < trunc (SYSDATE)-3 then dayoftrade --Где есть lasttradedate или listed_till ранее чем 3 дня
                when max_lasttradedate > trunc (SYSDATE)-3 or listed_till > trunc (SYSDATE)-3 then NULL --Где одна из дат торговли максимально близка к текущей дате
                when max_lasttradedate is NULL and listed_till is NULL then NULL --Для разбора ситуации
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
                      MIN(dt) OVER(PARTITION BY boardid, securityid, tradingsession) begin_session_date,
                      MAX(dt) OVER(PARTITION BY boardid, securityid, tradingsession) dayoftrade,
					  MAX(lasttradedate) OVER(PARTITION BY boardid, securityid, tradingsession) max_lasttradedate
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
            case
                when listed_till > trunc (SYSDATE)-3 then NULL --Для акции, где дата listed_till максимально близка к текущей дате
                when listed_till < trunc (SYSDATE)-3 then dayoftrade --Для акции, где дата listed_till ранее чем 3 дня
				when listed_till is NULL then NULL --Для разбора ситуации
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
                  MIN(dt) OVER(PARTITION BY boardid, securityid, tradingsession) begin_session_date,
                  MAX(dt) OVER(PARTITION BY boardid, securityid, tradingsession) dayoftrade
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
            case
                when listed_till > trunc (SYSDATE)-3 then NULL --Для акции, где дата listed_till максимально близка к текущей дате
                when listed_till < trunc (SYSDATE)-3 then dayoftrade --Для акции, где дата listed_till ранее чем 3 дня
				when listed_till is NULL then NULL --Для разбора ситуации
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
                  MIN(dt) OVER(PARTITION BY boardid, securityid, tradingsession) begin_session_date,
                  MAX(dt) OVER(PARTITION BY boardid, securityid, tradingsession) dayoftrade
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
            case
                when listed_till > trunc (SYSDATE)-3 then NULL --Для акции, где дата listed_till максимально близка к текущей дате
                when listed_till < trunc (SYSDATE)-3 then dayoftrade --Для акции, где дата listed_till ранее чем 3 дня
				when listed_till is NULL then NULL --Для разбора ситуации
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
                  MIN(dt) OVER(PARTITION BY boardid, securityid, tradingsession) begin_session_date,
                  MAX(dt) OVER(PARTITION BY boardid, securityid, tradingsession) dayoftrade
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