Таблица     'EFIR.MOEX_SECURITIES_SESSIONS'     [table\create_table_MOEX_SECURITIES_SESSIONS.sql]
    (Основная таблица с данными)
Таблица     'EFIR.MOEX_SEC_SES_HIST'            [table\create_table_MOEX_SECURITIES_SESSIONS_HYSTORY.sql]
    (Таблица с историей. Записи измененнные в основной таблице)
Процедура   'EFIR.MOEX_SEC_SES_HIST'            [daily_load\procedure.sql]
Задание     'EFIR.MOEX_SEC_SES_ACTUALIZATION_J' [daily_load\job.sql]
Первичная загрузка данных                       [initial_load\initial_load.sql]
/
--Выполнить процедуру
BEGIN
    EFIR.MOEX_SEC_SESSION;
END;
/
select * from EFIR.MOEX_SEC_SES_HIST
/
select *
from EFIR.MOEX_SECURITIES_SESSIONS
where TRUNC(update_date) = TRUNC(sysdate)
order by update_date desc
/
--Проверка наличия задания в планировщике
select job_name,
  to_char(start_date, 'dd-mm-yyyy hh24:mi:ss') start_date,
  repeat_interval,
  to_char(last_start_date, 'dd-mm-yyyy hh24:mi:ss') last_start_date,
  to_char(next_run_date, 'dd-mm-yyyy hh24:mi:ss') next_run_date,
  enabled,
  run_count,
  failure_count
 from dba_scheduler_jobs
 where owner = 'EFIR'
 and job_name = 'MOEX_SEC_SES_ACTUALIZATION_J'
/
--лог выполнения задания
select  to_char(d.actual_start_date, 'dd-mm-yyyy hh24:mi:ss') "Start_DATE",
        to_char(l.log_date, 'dd-mm-yyyy hh24:mi:ss') "END_DATE",
        l.job_name,
        l.status,
        l.additional_info,
        d.error#,
        d.session_id,
        to_char(d.run_duration, 'hh24:mi:ss') "RUN_DURATION",
        to_char(d.cpu_used, 'hh24:mi:ss') "CPU_USED",
        (cast(d.actual_start_date as date) - cast(d.req_start_date as date)) * 24*60*60 "DELAY_sec",
        to_char(d.actual_start_date - req_start_date, 'hh24:mi:ss') "DELAY_RUN"
       from dba_scheduler_job_log l, dba_scheduler_job_run_details d
 where l.log_id = d.log_id
 and l.owner = 'EFIR'
 and l.job_name = 'MOEX_SEC_SES_ACTUALIZATION_J'
 order by l.log_date desc
/
/*[12:43] Александр Чернышёв
ну что я хочу тебе сказать.Вроде всё круть! Действительно записей похожих на history не было.
Прошерстил историю, update-ы просто проставились там где надо.
Проверял таким макаром, вдруг пригодиться когда-нибудь.
/*/
select * from EFIR.MOEX_SECURITIES_SESSIONS
where update_date > trunc (sysdate)-2 and MATDATE > trunc (SYSDATE)-5
and END_SESSION_DATE is NULL
order by MATDATE ASC
/
select * from EFIR.MOEX_SECURITIES_SESSIONS
where update_date > trunc (sysdate)-2 and MATDATE > trunc (SYSDATE)-5
and END_SESSION_DATE is NULL and MATDATE is NULL
/

/*28.04.2022*/
--Анализ причин появления записи в History
select * from MOEX_SEC_SES_HIST
where securityid = 'MTSS'

--Устанавливаем причину попадания в histoty по базе SESSIONS
select * from MOEX_SECURITIES_SESSIONS
where securityid = 'MTSS' and boardid = 'PTEQ'
--Если потребуется в дальнейшем выводить в API DH историю дат DT по ценной бумаге в соответствующем tradingsession,
--то вероятно нужно будет воспринимать (подменять) tradingsession = null, как tradingsession = 3 и забирать на вывод begin_session_date минимальную
--дату DT от записи с tradingsession = null, если она меньше чем минимальная запсиь с tradingsession = 3


--Смотрим ситуацию в родительской таблице TP_
select * from TP_SHARES_MICEX_OFFICIAL
where securityid = 'MTSS' and boardid = 'PTEQ'
order by DT desc
--Если потребуется в дальнейшем выводить в API DH весь листинг дат DT по ценной бумаге в соответствующем tradingsession,
--то вероятно нужно будет зацеплять (довыводить) даты DT от записей у которых  tradingsession = null, к записям у которых tradingsession = 3 с сортировкой ASC
/
--29/04/22
--Актуализация данных вручную

--Очистка таблиц
TRUNCATE TABLE efir.moex_securities_sessions
/
TRUNCATE TABLE efir.moex_sec_ses_hist
/
--Выполняем первоначальную "заливку данных"
--В Oracle SQL Developer открываем файл "initial_load.sql"
--Выполняем код

--Вручную выполненяем задание. Может выполняться до 10 минут. Позже оптимизируем.
EXEC DBMS_SCHEDULER.RUN_JOB(job_name => 'EFIR.MOEX_SEC_SES_ACTUALIZATION_J')
/
--Проверяем результат
SELECT * 
FROM efir.moex_sec_ses_hist
WHERE TRUNC(update_date) = TRUNC(sysdate)
ORDER BY update_date DESC
/
SELECT * 
FROM efir.moex_securities_sessions
WHERE TRUNC(update_date) = TRUNC(sysdate)
ORDER BY update_date DESC
/