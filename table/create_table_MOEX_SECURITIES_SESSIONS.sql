-- ALTER TABLE efir.SEQ_MOEX_SECURITIES_SESSIONS DROP CONSTRAINT UK_01;
-- DROP INDEX EFIR.PK_MOEX_SECURITIES_SESSIONS;
--/
alter session set ddl_lock_timeout=300;
/
DROP TABLE "EFIR"."MOEX_SECURITIES_SESSIONS";
/
DROP SEQUENCE EFIR.SEQ_MOEX_SECURITIES_SESSIONS;
/
  CREATE TABLE EFIR.MOEX_SECURITIES_SESSIONS (
    "ID" NUMBER(10,0) NOT NULL,
    "SECURITYID" VARCHAR2(15 BYTE) NOT NULL,
    "BOARDID" VARCHAR2(10 BYTE) NOT NULL,
    "ID_ISS" NUMBER(10,0) NOT NULL,
    "SHORTNAME" VARCHAR2(200 BYTE),
    "TRADINGSESSION" NUMBER(1,0),
    "TYPECODE" VARCHAR2(25 BYTE),
    "MATDATE" DATE,
    "BEGIN_SESSION_DATE" DATE, --NOT NULL,
    "END_SESSION_DATE" DATE,
    "MAX_LASTTRADEDATE" DATE,
    "MAX_DAYOFTRADE" DATE,
    "IS_TRADED" NUMBER(1,0),
    "LISTED_FROM" DATE,
    "LISTED_TILL" DATE,
    "ADD_DATE" DATE DEFAULT SYSDATE NOT NULL,
    "UPDATE_DATE" DATE NOT NULL
   )
  TABLESPACE "EFIR_TBS" ;
/
CREATE UNIQUE INDEX EFIR.PK_MOEX_SECURITIES_SESSIONS ON EFIR.MOEX_SECURITIES_SESSIONS (
     ID_ISS
    ,SECURITYID
    ,BOARDID
    ,TRADINGSESSION
);
/
CREATE SEQUENCE EFIR.SEQ_MOEX_SECURITIES_SESSIONS  MINVALUE 1 MAXVALUE 9999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE;
/
CREATE TRIGGER EFIR.TRG_INS_MOEX_SEC_SESSIONS
 BEFORE
 INSERT
 ON EFIR.MOEX_SECURITIES_SESSIONS
 FOR EACH ROW
begin
  SELECT EFIR.SEQ_MOEX_SECURITIES_SESSIONS.NEXTVAL INTO :new.id FROM dual;
end;
/
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."ID" IS 'Служебный идентификатор строки';
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."SECURITYID" IS 'Уникальный идентификатор (тикер) ценной бумаги на МБ';
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."BOARDID" IS 'Режим торгов ценной бумаги';
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."ID_ISS" IS 'Внутренний идентификатор ценной бумаги в конкретном режиме торгов';
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."SHORTNAME" IS 'Наименование ценной бумаги';
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."TRADINGSESSION" IS 'Идентификатор торговой сессии у ценной бумаги (0-утренняя, 1-основная, 2-вечерняя, 3-итоги)';
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."BEGIN_SESSION_DATE" IS 'Дата допуска ценной бумаги к торговой сессии';
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."END_SESSION_DATE" IS 'Дата приостановки/исключения бумаги из торговой сессии';
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."TYPECODE" IS 'Наименование типа ценной бумаги';
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."LISTED_FROM" IS 'Дата публикации ценной бумаги на режиме торгов МосБиржи';
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."LISTED_TILL" IS 'Крайняя дата по ценной бумаге на режиме торгов МосБиржи';
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."MATDATE" IS 'Дата погашения (прекращения торговли) ценной бумаги';
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."ADD_DATE" IS 'Служебная дата внесения строки в таблицу';
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."UPDATE_DATE" IS 'Служебная дата внесения строки в таблицу / Внесение изменений в запись';
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."IS_TRADED" IS 'Флаг торгуемости ценной бумаги';
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."MAX_LASTTRADEDATE" IS 'Дата последних фактических сделок по ценной бумаге';
COMMENT ON COLUMN "EFIR"."MOEX_SECURITIES_SESSIONS"."MAX_DAYOFTRADE" IS 'Дата последней публикации ценной бумаги на торговой сессии';
/
-- INSERT INTO "EFIR"."MOEX_SECURITIES_SESSIONS" (
--     securityid,
--     boardid,
--     id_iss,
--     shortname,
--     tradingsession,
--     typecode,
--     matdate,
--     begin_session_date,
--     end_session_date,
--     listed_from,
--     listed_till,
--     update_date
-- ) VALUES (
--     '5',
--     '1',
--     '1',
--     '1',
--     '1',
--     '1',
--     TO_DATE('2022-04-21 10:26:14', 'YYYY-MM-DD HH24:MI:SS'),
--     TO_DATE('2022-04-13 10:26:22', 'YYYY-MM-DD HH24:MI:SS'),
--     TO_DATE('2022-04-13 10:26:26', 'YYYY-MM-DD HH24:MI:SS'),
--     TO_DATE('2022-04-13 10:26:31', 'YYYY-MM-DD HH24:MI:SS'),
--     TO_DATE('2022-04-13 10:26:35', 'YYYY-MM-DD HH24:MI:SS'),
--     TO_DATE('2022-04-13 10:26:40', 'YYYY-MM-DD HH24:MI:SS')
-- )
-- /
-- select *
-- from "EFIR"."MOEX_SECURITIES_SESSIONS"