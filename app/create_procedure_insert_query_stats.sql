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