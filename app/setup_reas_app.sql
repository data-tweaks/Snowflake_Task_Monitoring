
create application role if not exists reas_app_role;

-- bu schema versiyon lu olmali mi olmamali mi ona bi bak  
--create or alter versioned schema core;
create  schema if not exists core;
grant usage on schema core to application role reas_app_role ; 


create  schema if not exists core;

create or replace TABLE  CORE.TASK_RUN_HISTORY (
	ROOT_TASK_NAME VARCHAR(180),
	ROOT_TASK_ATTEMPT_NUMBER NUMBER(38,0),
	ROOT_TASK_STATE VARCHAR(12),
	TASK_NAME VARCHAR(180),
	CONDITION_TEXT VARCHAR(180),
	ROOT_TASK_SCHEDULED_FROM VARCHAR(15),
	TASK_SCHEDULED_FROM VARCHAR(15),
	TASK_SCHEDULED_TIME VARCHAR(60),
	TASK_START_TIME VARCHAR(60),
	TASK_COMPLETED_TIME VARCHAR(60),
	TASK_QUERY_START_TIME VARCHAR(60),
	TASK_QUERY_END_TIME VARCHAR(60),
	TASK_HISTORY_STATE VARCHAR(25),
	TASK_ERROR_CODE VARCHAR(60),
	TASK_ERROR_MESSAGE VARCHAR(180),
	DATABASE_NAME VARCHAR(80),
	QUERY_TYPE VARCHAR(120),
	TASK_QUERY_EXECUTION_STATUS VARCHAR(80),
	USER_NAME VARCHAR(160),
	ROLE_NAME VARCHAR(160),
	WAREHOUSE_NAME VARCHAR(150),
	WAREHOUSE_SIZE VARCHAR(50),
	TOTAL_ELAPSED_TIME NUMBER(38,3),
	COMPILATION_TIME NUMBER(38,3),
	EXECUTION_TIME NUMBER(38,3),
	EXECUTION_WAREHOUSE_TIME NUMBER(38,3),
	QUEUED_PROVISIONING_TIME NUMBER(38,0),
	QUEUED_OVERLOAD_TIME NUMBER(38,3),
	TRANSACTION_BLOCKED_TIME NUMBER(38,3),
	QUERY_LOAD_PERCENT NUMBER(38,2),
	BYTES_SCANNED NUMBER(38,0),
	PERCENTAGE_SCANNED_FROM_CACHE FLOAT,
	BYTES_WRITTEN NUMBER(38,0),
	ROWS_PRODUCED NUMBER(38,0),
	PARTITIONS_SCANNED NUMBER(38,0),
	PARTITIONS_TOTAL NUMBER(38,0),
	BYTES_SPILLED_TO_LOCAL_STORAGE NUMBER(38,0),
	BYTES_SPILLED_TO_REMOTE_STORAGE NUMBER(38,0),
	BYTES_SENT_OVER_THE_NETWORK NUMBER(38,0),
	QUERY_ACCELERATION_BYTES_SCANNED NUMBER(38,0),
	QUERY_ACCELERATION_PARTITIONS_SCANNED NUMBER(38,0),
	QUERY_RETRY_TIME NUMBER(38,0),
	QUERY_RETRY_CAUSE VARCHAR(150),
	QUERY_ID VARCHAR(100)
);
 
create or replace TABLE CORE.TASK_STATISTICS_TOP5 (
	CATEGORY VARCHAR(60),
	ROOT_TASK_NAME VARCHAR(250),
	TASK_NAME VARCHAR(250),
	ROOT_TASK_SCHEDULED_FROM VARCHAR(60),
	TASK_STATE VARCHAR(60),
	TASK_RUN_DT DATE,
	QUERY_TYPE VARCHAR(60),
	COMPILATION_TIME_IN_SEC NUMBER(10,3),
	EXECUTION_TIME_IN_SEC NUMBER(10,3),
	TOTAL_TIME_IN_SEC NUMBER(10,3),
	QUERY_ID VARCHAR(60)
);

create or replace TABLE CORE.TASK_STORAGE_SCAN_PERF_TOP5 (
	CATEGORY VARCHAR(30),
	QUERY_TYPE VARCHAR(80),
	ROOT_TASK VARCHAR(180),
	TASK VARCHAR(180),
	TRIGGERED_AS VARCHAR(50),
	EXECUTION_DT DATE,
	STATUS VARCHAR(30),
	PERFORMANCE_INDICATOR NUMBER(20,2),
	IS_BYTES_SPILLED NUMBER(38,0)
);

create or replace TABLE CORE.TASK_WH_USAGE_PARAMETRIZED (
	PARAMETER VARCHAR(60),
	PERIOD_DT VARCHAR(50),
	WAREHOUSE_NAME VARCHAR(80),
	DATABASE_NAME VARCHAR(50),
	TASK_NAME VARCHAR(180),
	CREDITS_USED NUMBER(38,6)
);

create or replace TABLE CORE.TASKS_FOR_HEALTH_CHECK (
	TASK_NAME VARCHAR(180),
	TASK_SCHEDULED_FROM VARCHAR(40),
	WAREHOUSE_NAME VARCHAR(180),
	DATABASE_NAME VARCHAR(80),
	TOTAL_ELAPSED_TIME NUMBER(38,0),
	QUERY_ID VARCHAR(80)
);

create or replace TABLE CORE.WAREHOUSE_UTILIZATION_MONTHLY (
	WAREHOUSE_NAME VARCHAR(180),
	TOTAL_HOURS NUMBER(38,0),
	TOTAL_IDLE_HOURS NUMBER(38,0)
);


create or replace TABLE CORE.QUERY_STATS (
	OPERATOR_TYPE VARCHAR(120),
	TOTAL_TIME NUMBER(38,5),
	INIT_TIME NUMBER(38,5),
	NETWORK_TIME NUMBER(38,5),
	REMOTE_DISK_IO_TIME NUMBER(38,5),
	PROCESSING_TIME NUMBER(38,5),
	LOCAL_DISK_IO_TIME NUMBER(38,5),
	TABLENAME VARCHAR(16777216),
	FUNCTIONS VARCHAR(16777216),
	GROUPINGKEYSET VARCHAR(16777216),
	GROUPKEYS VARCHAR(16777216),
	FILTERCONDITION VARCHAR(16777216),
	INPUT VARCHAR(16777216),
	JOIN_TYPE VARCHAR(16777216),
	ADDITIONAL_JOINCONDITION VARCHAR(16777216),
	JOINCONDITION VARCHAR(16777216),
	SORTKEYS VARCHAR(16777216),
	STAGENAME VARCHAR(16777216),
	STAGETYPE VARCHAR(16777216),
	TABLESCANCOLUMNS VARCHAR(16777216),
	QUERY_ID VARCHAR(16777216)
);

create or replace TABLE CORE.TASK_STATE (
	CREATED_ON VARCHAR(100),
	DATABASE_NAME VARCHAR(100),
	SCHEMA_NAME VARCHAR(100),
	NAME VARCHAR(150),
	ID VARCHAR(80),
	STATE VARCHAR(50),
	ERROR_INTEGRATION VARCHAR(150),
	SCHEDULE VARCHAR(80),
	OWNER VARCHAR(80),
	LAST_SUSPENDED_ON VARCHAR(100),
	LAST_SUSPENDED_REASON VARCHAR(160)
);


create or replace TABLE CORE.TASKS_MISSED_EXECUTIONS (
	TASK_NAME VARCHAR(260),
	SCHEDULED_TIME TIMESTAMP_LTZ(3),
	STATE VARCHAR(12),
	NEXT_SCHEDULED_TIME TIMESTAMP_LTZ(3)
);


create or replace procedure core.init_analyse()
  returns varchar
  LANGUAGE SQL
 comment = 'Calculates statistics for the tasks '  
execute as  OWNER  
 as 
  $$
    begin 

        truncate table core.TASK_RUN_HISTORY; 
        
        insert into core.TASK_RUN_HISTORY
        ( root_task_name, root_task_attempt_number, root_task_state, task_name, condition_text, 
            root_task_scheduled_from, task_scheduled_from, task_scheduled_time, task_start_time, task_completed_time, 
            task_query_start_time, task_query_end_time, task_history_state,
        	task_error_code, task_error_message,  database_name, query_type,  task_query_execution_status, 
        	user_name, role_name, warehouse_name, warehouse_size, total_elapsed_time, compilation_time, execution_time, 
            execution_warehouse_time, queued_provisioning_time,
        	queued_overload_time, transaction_blocked_time , query_load_percent, bytes_scanned, percentage_scanned_from_cache,
        	bytes_written, rows_produced, partitions_scanned, partitions_total, bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage,
        	bytes_sent_over_the_network, query_acceleration_bytes_scanned , query_acceleration_partitions_scanned , query_retry_time,
        	query_retry_cause, query_id
        )
        select   ctg.database_name  || '.' || ctg.schema_name || '.' || ctg.root_task_name,  
            ctg.attempt_number, ctg.state,
            th.database_name || '.' || th.schema_name || '.' || th.name, th.condition_text, ctg.scheduled_from, 
            th.scheduled_from,  
            to_varchar(th.scheduled_time, 'YYYY-MM-DD HH:MI:SS') , 
            to_varchar(th.query_start_time, 'YYYY-MM-DD HH:MI:SS'),   
            to_varchar(th.completed_time, 'YYYY-MM-DD HH:MI:SS'), 
            to_varchar(qh.start_time, 'YYYY-MM-DD HH:MI:SS'),  
            to_varchar(qh.end_time, 'YYYY-MM-DD HH:MI:SS') ,  
            th.state, th.error_code,  th.error_message,   
            th.database_name , 
            qh.query_type, qh.execution_status, qh.user_name, qh.role_name, 
            qh.warehouse_name, warehouse_size, qh.total_elapsed_time, qh.compilation_time, qh.execution_time, 
            datediff('millisecond', 
                     timeadd(
                        'millisecond',
                        qh.queued_overload_time + qh.compilation_time +
                        qh.queued_provisioning_time + qh.queued_repair_time +
                        qh.list_external_files_time,
                        qh.start_time
                         ) , 
                     qh.end_time ),  
            qh.queued_provisioning_time, qh.queued_overload_time, qh.transaction_blocked_time, 
            qh.query_load_percent, qh.bytes_scanned, qh.percentage_scanned_from_cache, 
            qh.bytes_written, qh.rows_produced, qh.partitions_scanned, qh.partitions_total, 
            qh.bytes_spilled_to_local_storage, qh.bytes_spilled_to_remote_storage, qh.bytes_sent_over_the_network, 
            qh.query_acceleration_bytes_scanned, qh.query_acceleration_partitions_scanned, 
            qh.query_retry_time, qh.query_retry_cause, qh.query_id
        from snowflake.account_usage.task_history th  
         left outer join snowflake.account_usage.query_history qh 
           on th.query_id = qh.query_id 
         left outer join snowflake.account_usage.complete_task_graphs ctg 
           on ctg.graph_run_group_id = th.graph_run_group_id
        where th.query_start_time > add_months(current_date() , -3 ); 

        truncate table core.TASK_STATISTICS_TOP5 ;  
        
         insert into core.task_statistics_top5 (category, root_task_name, task_name, root_task_scheduled_from , task_state,  task_run_dt , query_type,  compilation_time_in_sec,  execution_time_in_sec, total_time_in_sec, query_id)
        with tasks_with_max_compilation as (
        select 'Compilation time' category,  root_task_name , task_name , max(compilation_time ) process_time  
          from  core.task_run_history
          group by  root_task_name , task_name 
        order by max(compilation_time ) desc 
          limit 5 
         ),  
         tasks_with_max_execution as (
        select 'Execution time' category,  root_task_name , task_name , max(execution_time ) process_time  
          from   core.task_run_history
          group by  root_task_name , task_name 
        order by max(execution_time ) desc 
          limit 5 
         ), 
         topTasks as (
            select category,  root_task_name , task_name ,  process_time  from tasks_with_max_compilation 
            union all 
            select category,  root_task_name , task_name ,  process_time  from tasks_with_max_execution 
         ) -- select * from topTasks  
         select  mct.category,  rh.root_task_name , rh.task_name,  rh.root_task_scheduled_from, 
           rh.task_history_state as task_state , to_date(rh.task_query_start_time) as task_run_dt , 
           lower(query_type) as query_type, 
           round(rh.compilation_time /1000 , 2) as compilation_time_in_sec, 
           round(rh.execution_time / 1000 , 2) as  execution_time_in_sec, 
           round(rh.total_elapsed_time / 1000 , 2 ) as total_time_in_sec , 
           rh.query_id
         from  core.task_run_history  rh  
         inner join  topTasks  mct
           on  rh.root_task_name  = mct.root_task_name 
            and rh.task_name = mct.task_name  
            and  rh.compilation_time = mct.process_time   and category ='Compilation time'  
         union 
            select  mct.category,  rh.root_task_name , rh.task_name,  rh.root_task_scheduled_from, 
           rh.task_history_state as task_state , to_date(rh.task_query_start_time) as task_run_dt , 
           lower(query_type) as query_type, 
           round(rh.compilation_time /1000 , 2) as compilation_time_in_sec, 
           round(rh.execution_time / 1000 , 2) as  execution_time_in_sec, 
           round(rh.total_elapsed_time / 1000 , 2 ) as total_time_in_sec , 
           rh.query_id
         from core.task_run_history  rh  
         inner join  topTasks  mct
           on  rh.root_task_name  = mct.root_task_name 
            and rh.task_name = mct.task_name              
            and rh.execution_time = mct.process_time   and category =  'Execution time'  ; 

    truncate table core.TASK_STORAGE_SCAN_PERF_TOP5 ; 
     
    insert into core.TASK_STORAGE_SCAN_PERF_TOP5 
     (
       category, query_type, root_task, task, triggered_as, execution_dt, status, performance_indicator, is_bytes_spilled  
     )
         with perf_indicator_tmp as
         (
               select 'Performance' as category, 
                 lower(query_type) as query_type, 
                 root_task_name as root_task  , 
                 task_name as task,     
                 lower(root_task_scheduled_from) triggered_as, task_start_time, 
                 to_date(task_start_time ) as execution_dt, 
                 lower(task_history_state) as status , 
                 case when bytes_sent_over_the_network +  bytes_spilled_to_remote_storage + bytes_spilled_to_local_storage <= 0 
                        then  case when partitions_scanned <> 0 and partitions_total <> 0  and partitions_total >= 100 
                                  then round((partitions_scanned /partitions_total  ) * 100 , 2) 
                              else 0 end                     
                      when bytes_sent_over_the_network + bytes_spilled_to_remote_storage    <= 0 
                        then bytes_spilled_to_local_storage 
                      when  bytes_sent_over_the_network + bytes_spilled_to_local_storage <= 0 
                        then bytes_spilled_to_remote_storage
                      when bytes_spilled_to_remote_storage + bytes_spilled_to_local_storage <= 0 
                       then bytes_sent_over_the_network   
                  end performance_indicator ,
                  case when bytes_sent_over_the_network +  bytes_spilled_to_remote_storage + bytes_spilled_to_local_storage > 0 
                        then 1 else 0 
                  end as is_bytes_spilled  
              from  core.task_run_history    
         ), 
         performance_indicator_max as 
         (
            select category, 
               query_type, 
               root_task  , 
               task,     
               triggered_as,  
               execution_dt, 
               status , 
               max(performance_indicator) over(partition by task, is_bytes_spilled , triggered_as ) performance_indicator , 
               is_bytes_spilled
             from perf_indicator_tmp 
        ), 
        performance_indicator_spilled as 
        (
             select tmp.category, 
               tmp.query_type, 
               tmp.root_task  , 
               tmp.task,     
               tmp.triggered_as,  
               tmp.execution_dt, 
               tmp.status , 
               tmp.performance_indicator, 
               tmp.is_bytes_spilled
             from perf_indicator_tmp tmp 
               inner join performance_indicator_max mx 
                on tmp.performance_indicator= mx.performance_indicator 
                  and tmp.task = mx.task 
                  and tmp.is_bytes_spilled = mx.is_bytes_spilled
                  and tmp.triggered_as = mx.triggered_as
              where tmp.is_bytes_spilled = 1
               order by tmp.performance_indicator desc limit 5       
        ), 
        performance_indicator_partition as 
        (
             select tmp.category, 
               tmp.query_type, 
               tmp.root_task  , 
               tmp.task,     
               tmp.triggered_as,  
               tmp.execution_dt, 
               tmp.status , 
               tmp.performance_indicator, 
               tmp.is_bytes_spilled
             from perf_indicator_tmp tmp 
               inner join performance_indicator_max mx 
                on tmp.performance_indicator= mx.performance_indicator 
                  and tmp.task = mx.task 
                  and tmp.is_bytes_spilled = mx.is_bytes_spilled
                  and tmp.triggered_as = mx.triggered_as
              where tmp.is_bytes_spilled = 0 
               order by tmp.performance_indicator desc limit 5       
         ),
         performance_indicator as 
         (
            select category, 
               query_type, 
               root_task  , 
               task,     
               triggered_as,  
               execution_dt, 
               status , 
               performance_indicator, 
               is_bytes_spilled
             from performance_indicator_partition
             union 
            select category, 
               query_type, 
               root_task  , 
               task,     
               triggered_as,  
               execution_dt, 
               status , 
               performance_indicator, 
               is_bytes_spilled
             from performance_indicator_spilled
         ) 
         select category, 
           query_type, 
           root_task  , 
           task,     
           triggered_as,  
           execution_dt, 
           status , 
           performance_indicator, 
           is_bytes_spilled
         from performance_indicator ; 

         
     create or replace table  core.warehouse_sizes 
      as 
      select distinct warehouse_size,  credits_perhour 
      from 
        (
      select 'X-Small' AS warehouse_size, 
          1 as credits_perhour 
        union all 
      select 'Small' as warehouse_size, 
          2 as credits_per_hour 
        union all 
      select 'Medium'  as warehouse_size, 
          4 as credits_per_hour 
        union all 
      select 'Large' as warehouse_size, 
          8 as credits_per_hour 
        union all 
      select 'X-Large' as warehouse_size, 
        16  as credits_per_hour 
        union all 
      select '2X-Large' as warehouse_size, 
        32 as credits_per_hour 
        union all 
      select '3X-Large' as warehouse_size, 
        64 as credits_per_hour 
        union all 
      select '4X-Large' as warehouse_size, 
        128 as credits_per_hour
        ); 

 truncate table core.TASK_WH_USAGE_PARAMETRIZED  ; 

 insert into core.TASK_WH_USAGE_PARAMETRIZED  (parameter, period_dt,  database_name, warehouse_name,  task_name, credits_used)
with serverless as 
(
   select 'Serverless' as parameter,  lpad(month(to_date(start_time)) , 2,0 )  || '-' || year(to_date(start_time)) period, 
       sum(credits_used) serverless_credits 
    from snowflake.account_usage.serverless_task_history
  group by lpad(month(to_date(start_time)) , 2,0 )  || '-' || year(to_date(start_time)) 
), 
wh_daily_tasks as 
(
      select  'wh_daily' as parameter,  tr.task_name ,  
           tr.database_name , 
           to_date(task_start_time) as start_time , 
           sum(execution_warehouse_time * credits_perhour / (60*60*1000) ) credits_used 
       from core.task_run_history tr 
       left outer join core.warehouse_sizes ws 
           on tr.warehouse_size = ws.warehouse_size  
      group by tr.task_name , tr.database_name,  to_date(task_start_time) 
), 
wh_monthly_sum as 
(
     select 'wh_monthly' as parameter,  lpad(month(to_date(task_start_time)) , 2,0 )  || '-' || year(to_date(task_start_time)) as period ,  
       warehouse_name ,  
       sum(execution_warehouse_time * credits_perhour / (60*60*1000)) credits_used 
     from core.task_run_history tr 
    left outer join core.warehouse_sizes ws 
       on tr.warehouse_size = ws.warehouse_size   
    group by  lpad(month(to_date(task_start_time)) , 2,0 )  || '-' || year(to_date(task_start_time)) , warehouse_name  
)
  select coalesce(serverless.parameter , wh_monthly_sum.parameter, wh_daily_tasks.parameter ) as parameter ,
      coalesce(to_varchar(coalesce(serverless.period , wh_monthly_sum.period  ) ) , to_varchar(  wh_daily_tasks.start_time))  as period_dt , 
      nvl(database_name, '-') as database_name , 
      nvl(warehouse_name, '-') as warehouse_name , 
      nvl(task_name, '-') as task_name , 
      coalesce(serverless.serverless_credits ,wh_monthly_sum.credits_used, wh_daily_tasks.credits_used ) as  credits_used   
  from serverless 
    full outer join wh_monthly_sum 
     on serverless.parameter = wh_monthly_sum.parameter
    full outer join wh_daily_tasks 
     on  serverless.parameter = wh_daily_tasks.parameter; 

 truncate table core.tasks_for_health_check; 

insert into core.tasks_for_health_check (task_name, task_scheduled_from, warehouse_name, database_name, total_elapsed_time, query_id)
select task_name , 
    task_scheduled_from, 
    warehouse_name, 
    database_name, 
    total_elapsed_time , 
    query_id
from 
(
select task_name , 
    task_scheduled_from, 
    warehouse_name, 
    database_name, 
    total_elapsed_time , 
    query_id, 
    max(total_elapsed_time) over(partition by task_name) max_elapsed_time 
from core.task_run_history
where to_date(task_start_time) > current_date() - 14  
) where  total_elapsed_time = max_elapsed_time ; 

truncate table   core.warehouse_utilization_monthly; 

insert into core.warehouse_utilization_monthly(warehouse_name,  total_hours,total_idle_hours  )
WITH wh_events AS (
 SELECT
   warehouse_name,
   event_name, 
   TIMESTAMP, 
   LAG(timestamp) over (partition by warehouse_name ORDER BY timestamp) AS  prev_time,
   LAG(event_name) over (partition by warehouse_name ORDER BY timestamp) AS  prev_event
 FROM   snowflake.account_usage.warehouse_events_history
 WHERE   event_name IN ( 'RESUME_WAREHOUSE', 'SUSPEND_WAREHOUSE')
   and timestamp >= current_date() - 30 
)
 SELECT 
warehouse_name,
 count(distinct day(timestamp)  || monthname(timestamp)) *24 total_hours,   
 sum(DATEDIFF(hour, prev_time, timestamp)) AS total_idle_hours 
FROM wh_events
  where event_name = 'RESUME_WAREHOUSE'
    and prev_event = 'SUSPEND_WAREHOUSE'
group by warehouse_name ;


truncate table core.tasks_missed_executions; 

insert into core.tasks_missed_executions (task_name, scheduled_time, state, next_scheduled_time )
select  tg.database_name || '.' || tg.schema_name || '.' || tg.root_task_name , tg.scheduled_time,  tg.state, next_scheduled_time 
from 
    (
        select root_task_name  , database_name , schema_name  , max(scheduled_time )  max_scheduled_time
        from snowflake.account_usage.COMPLETE_TASK_GRAPHS  where SCHEDULED_FROM = 'SCHEDULE'  
        group by root_task_name  , database_name , schema_name 
    ) tmp  
inner join snowflake.account_usage.COMPLETE_TASK_GRAPHS tg 
  on  tmp.root_task_name = tg.root_task_name  
    and tmp.database_name = tg.database_name 
    and tmp.schema_name = tg.schema_name 
    and tg.scheduled_time =  max_scheduled_time
 where next_scheduled_time < current_timestamp()
   and next_scheduled_time > current_date() - 5 ; 

   end; 
$$;


CREATE OR REPLACE PROCEDURE  CORE.INSERT_QUERY_STATS( wh_name varchar)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Insert query stats to QUERY_STATS table to analyse the task queries for healt_check'
EXECUTE AS OWNER
AS
$$
DECLARE
  v_query_id_column varchar; 
  v_query_id varchar(80); 
  v_insert_stm varchar default '' ;    
  v_wh_name varchar(80) ; 
  v_select_query_stat_stm varchar; 
  c_health_check cursor for select distinct query_id , WAREHOUSE_NAME  from  tasks_for_health_check  ;   
  v_insertValues_stm varchar; 
  my_exception exception (-20001, 'Simple message') ; 
BEGIN
    v_insertValues_stm := 'insert into  CORE.QUERY_STATS (  operator_type, total_time, init_time, network_time, remote_disk_io_time, processing_time , local_disk_io_time  , TableName, functions,  groupingKeySet, 
                        GroupKeys ,  filterCondition, input,  join_type,additional_JoinCondition, joinCondition, sortKeys, StageName, stageType, tableScanColumns, query_id   ) ' ; 
                        
    v_select_query_stat_stm :=   'select  operator_type ,
                                     execution_time_breakdown:"overall_percentage"::number(38,5) total_time,  
                                     execution_time_breakdown:"initialization"::number(38,5) as init_time, 
                                     execution_time_breakdown:"network_communication"::number(38,5) as network_time, 
                                     execution_time_breakdown:"remote_disk_io"::number(38,5) remote_disk_io_time, 
                                     execution_time_breakdown:"processing"::number(38,5) as processing_time   , 
                                     execution_time_breakdown:"local_disk_io"::number(38,5) as local_disk_io_time,
                                     operator_attributes:"table_name"::varchar as TableName , 
                                     operator_attributes:"functions"::varchar as functions , 
                                     operator_attributes:"key_sets"::varchar as groupingKeySet , 
                                     operator_attributes:"grouping_keys"::varchar as GroupKeys , 
                                     operator_attributes:"filter_condition"::varchar as filterCondition ,  
                                     operator_attributes:"input_expression"::varchar as input, 
                                     operator_attributes:"join_type"::varchar as join_type  ,
                                     operator_attributes:"additional_join_condition"::varchar as additional_JoinCondition, 
                                     operator_attributes:"equality_join_condition"::varchar as joinCondition , 
                                     operator_attributes:"sort_keys"::varchar as sortKeys , 
                                     operator_attributes:"stage_name"::varchar as StageName, 
                                     operator_attributes:"stage_type"::varchar as stageType ,  
                                     operator_attributes:"columns"::varchar as tableScanColumns , ' ; 
                                                                 

   OPEN c_health_check ;
   LOOP 
     FETCH c_health_check  INTO  v_query_id , v_wh_name; 
     IF (  nvl(v_query_id ,'' )   <> ''  ) THEN 
           if (lower(trim(wh_name)) =  lower(trim(v_wh_name))) then 
                v_query_id_column := ''''   || v_query_id  ||  '''' || ' as query_id  ' ; 
                v_insert_stm := v_insertValues_stm ||  v_select_query_stat_stm || v_query_id_column  || ' from table(get_query_operator_stats( ' || '''' ||     v_query_id ||  ''''   || ')) ;'   ; 
              execute immediate  v_insert_stm  ; 
           else 
              null ; 
           end if; 
     ELSE 
       break; 
     END IF; 
   END LOOP; 
   close c_health_check; 
   RETURN v_insert_stm;
exception   
   when STATEMENT_ERROR then
    RETURN OBJECT_CONSTRUCT('Error type', 'STATEMENT_ERROR',
                            'SQLCODE', SQLCODE,
                            'SQLERRM', SQLERRM,
                            'SQLSTATE', SQLSTATE);
   when OTHER then
    RETURN OBJECT_CONSTRUCT('Error type', 'Other error',
                            'SQLCODE', SQLCODE,
                            'SQLERRM', SQLERRM,
                            'SQLSTATE', SQLSTATE);
END;
$$
;


-- Grant usage and permissions on objects
grant usage on schema core to application role reas_app_role;
grant usage on procedure core.init_analyse() to application role reas_app_role;
grant usage on procedure core.INSERT_QUERY_STATS( varchar) to application role reas_app_role;
grant select on all tables in schema core to application role reas_app_role;

-- creating schema for streamlit and reference update proc 
create or alter versioned schema reas ; 

grant usage on schema reas to application role reas_app_role;

create or replace streamlit reas.task_monitoring 
   from  '/streamlit'
   MAIN_FILE = '/reas_task_monitor.py' ; 

-- streamlit grants 
grant usage on streamlit reas.task_monitoring  to application role reas_app_role; 
--grant select on all tables in  schema reas to application role reas_app_role;

CREATE or replace PROCEDURE reas.REGISTER_SINGLE_REFERENCE(ref_name STRING, operation STRING, ref_or_alias STRING)
  RETURNS STRING
  LANGUAGE SQL
  AS $$
    BEGIN
      CASE (operation)
        WHEN 'ADD' THEN
          SELECT SYSTEM$ADD_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'REMOVE' THEN
          SELECT SYSTEM$REMOVE_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'CLEAR' THEN
          SELECT SYSTEM$REMOVE_ALL_REFERENCES(:ref_name);
      ELSE
        RETURN 'unknown operation: ' || operation;
      END CASE;

      RETURN NULL;
    END;
  $$;

GRANT USAGE ON PROCEDURE reas.REGISTER_SINGLE_REFERENCE(STRING, STRING, STRING) TO APPLICATION ROLE reas_app_role;

