--- Установить уникальность для каждой записи в таблицу = SECURYTYID + BOARDID + TRADINGSESSION (учитывая и то, что TRADINGSESSION может быть is null );
--- Установить авто-присвоение порядкового номера служебного идентификатора ID каждой строке  в ходе наполнения таблицы;
--- Установить авто-присвоение служебной (системной) даты для поля ADD_DATE в момент записи строки в таблицу.
--/
--ALTER TABLE EFIR.SEQ_MOEX_SEC_SES_HIST DROP CONSTRAINT UK_01;
--DROP INDEX EFIR.PK_MOEX_SEC_SES_HIST;
/
DROP TABLE "EFIR"."MOEX_SEC_SES_HIST";
/
DROP SEQUENCE EFIR.MOEX_SEC_SES_HIST;
/
  CREATE TABLE EFIR.MOEX_SEC_SES_HIST (
    "ID" NUMBER(10,0) NOT NULL,
    "MOEX_SEC_SES_ID" NUMBER(10,0) NOT NULL,
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
    "IS_TRADED" NUMBER(1,0),
    "LISTED_FROM" DATE,
    "LISTED_TILL" DATE,
    "ADD_DATE" DATE DEFAULT SYSDATE NOT NULL,
    "UPDATE_DATE" DATE NOT NULL,
    "DT" DEFAULT SYSDATE DATE NOT NULL
   )
  TABLESPACE "EFIR_TBS" ;
/
--CREATE UNIQUE INDEX EFIR.PK_MOEX_SEC_SES_HIST ON EFIR.MOEX_SEC_SES_HIST (
--     ID_ISS
--    ,SECURITYID
--    ,BOARDID
--    ,TRADINGSESSION
--);
/
CREATE SEQUENCE EFIR.SEQ_MOEX_SEC_SES_HIST  MINVALUE 1 MAXVALUE 9999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE;
/
CREATE TRIGGER EFIR.TRG_INS_MOEX_SEC_SES_HIST
 BEFORE
 INSERT
 ON EFIR.MOEX_SEC_SES_HIST
 FOR EACH ROW
begin
  SELECT EFIR.SEQ_MOEX_SEC_SES_HIST.NEXTVAL INTO :new.id FROM dual;
end;
/
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."ID" IS 'Служебный идентификатор архивной строки';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."MOEX_SEC_SES_ID" IS 'Служебный идентификатор строки таблицы MOEX_SECURITIES_SESSIONS.ID';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."SECURITYID" IS 'Уникальный идентификатор (тикер) ценной бумаги на МБ';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."BOARDID" IS 'Режим торгов ценной бумаги';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."ID_ISS" IS 'Внутренний идентификатор ценной бумаги в конкретном режиме торгов (secid+boardid)';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."SHORTNAME" IS 'Наименование ценной бумаги';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."TRADINGSESSION" IS 'Идентификатор торговой сессии у ценной бумаги (0-утренняя, 1-основная, 2-вечерняя, 3-итоги)';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."BEGIN_SESSION_DATE" IS 'Дата допуска ценной бумаги к торговой сессии';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."END_SESSION_DATE" IS 'Дата исключения бумаги из торговой сессии';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."TYPECODE" IS 'Наименование типа ценной бумаги';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."LISTED_FROM" IS 'Дата публикации ценной бумаги на режиме торгов МосБиржи';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."LISTED_TILL" IS 'Крайняя дата по ценной бумаге на режиме торгов МосБиржи';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."MATDATE" IS 'Дата прекращения торговли ценной бумагой';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."ADD_DATE" IS 'Служебная дата внесения строки в таблицу';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."UPDATE_DATE" IS 'Служебная дата внесения строки в таблицу / Внесение изменений в запись';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."IS_TRADED" IS 'Для связи с таблицей MOEX_SECURITIES_BOARDS';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."MAX_LASTTRADEDATE" IS 'Которая будет соответствовать MAX(dt) OVER(PARTITION BY boardid, securityid, tradingsession)';
COMMENT ON COLUMN "EFIR"."MOEX_SEC_SES_HIST"."DT" IS 'Дата внесения записи';
/
--INSERT INTO "EFIR"."MOEX_SEC_SES_HIST" (
--    securityid,
--    boardid,
--    id_iss,
--    shortname,
--    tradingsession,
--    typecode,
--    matdate,
--    begin_session_date,
--    end_session_date,
--    listed_from,
--    listed_till,
--    update_date
--) VALUES (
--    '5',
--    '1',
--    '1',
--    '1',
--    '1',
--    '1',
--    TO_DATE('2022-04-21 10:26:14', 'YYYY-MM-DD HH24:MI:SS'),
--    TO_DATE('2022-04-13 10:26:22', 'YYYY-MM-DD HH24:MI:SS'),
--    TO_DATE('2022-04-13 10:26:26', 'YYYY-MM-DD HH24:MI:SS'),
--    TO_DATE('2022-04-13 10:26:31', 'YYYY-MM-DD HH24:MI:SS'),
--    TO_DATE('2022-04-13 10:26:35', 'YYYY-MM-DD HH24:MI:SS'),
--    TO_DATE('2022-04-13 10:26:40', 'YYYY-MM-DD HH24:MI:SS')
--)
--/
