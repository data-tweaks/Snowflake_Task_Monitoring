
create or replace procedure init_analyse()
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


