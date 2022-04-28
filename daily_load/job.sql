--connect as EFIR
BEGIN
DBMS_SCHEDULER.DROP_JOB(job_name => 'MOEX_SEC_SES_ACTUALIZATION_J');

DBMS_SCHEDULER.CREATE_JOB('"MOEX_SEC_SES_ACTUALIZATION_J"',
                            job_type=>'STORED_PROCEDURE',
                            job_action=>'EFIR.MOEX_SEC_SESSION',
                            number_of_arguments=>0,
                            start_date => TIMESTAMP '2022-04-23 10:00:00',
                            repeat_interval=>'FREQ=DAILY; BYHOUR=10',
                            end_date=>NULL,
                            job_class=>'"DEFAULT_JOB_CLASS"',
                            enabled=>TRUE,
                            auto_drop=>FALSE,
                            comments=>'Actualization data (insert/updated) in table EFIR.MOEX_SECURITIES_SESSIONS and save history data in EFIR.MOEX_SEC_SES_HIST before update it'
                            );
COMMIT;
END;
/