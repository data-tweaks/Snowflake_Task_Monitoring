import streamlit as st
from snowflake.snowpark.session import Session 

import pandas as pd  
import toml
import os

def writeTabs():
    st.write("\n\n")
    st.write("\n\n")
    st.write("\n\n")



def snowpark_session_create(): 
 
    config = toml.load("app/config/connections.toml")
    connConfig = config["taskMonitoring"]

    connection_params = {
        "user" : st.secrets["user"],   
        "password": st.secrets["password"],
        "account" : st.secrets["account"],
        "warehouse" : connConfig.get("warehouse"),
        "role" : connConfig.get("role"),
    }

    session = Session.builder.configs(connection_params).create()
    return session 


session = snowpark_session_create() 


def getTaskInfo(session): 
        
        v_getsuspendedState = f''' select task_name, scheduled_time, state, next_scheduled_time from   taskmonitoring.core.tasks_missed_executions  ; '''
        suspended_df =  pd.DataFrame(session.sql(v_getsuspendedState).collect())

        #Header and explanation
        st.header(" :trackball: **:orange[TASKS TO BE EXAMINED]**" )    
        st.write(f" **List of tasks that are not triggered past their last scheduled times.**") 
    
        container_taskStateSummary = st.container(border=True)
        container_taskStateSummary.write("\n\n")
        task_name, last_executed_time, last_execution_state, next_schedule  = container_taskStateSummary.columns(4)
        task_name.markdown('<p style="font-family: Helvetica, sans-serif;  font-weight: bold;"> Task</p>' ,  unsafe_allow_html=True)   
        last_executed_time.markdown('<p style="font-family: Helvetica, sans-serif; font-weight: bold;"> Last Execution </p>',  unsafe_allow_html=True  ) 
        last_execution_state.markdown( '<p style="font-family: Helvetica, sans-serif;  font-weight: bold;"> Last Execution State </p>',  unsafe_allow_html=True  ) 
        next_schedule.markdown(  '<p style="font-family: Helvetica, sans-serif; font-weight: bold;"> Scheduled To </p>',  unsafe_allow_html=True  )

        rownum_iter = 0 

        for row in suspended_df.iterrows():
            task_name.markdown( suspended_df["TASK_NAME"][rownum_iter]) 
            last_executed_time.markdown( suspended_df["SCHEDULED_TIME"][rownum_iter]) 
            last_execution_state.markdown( suspended_df["STATE"][rownum_iter]) 
            next_schedule.markdown( suspended_df["NEXT_SCHEDULED_TIME"][rownum_iter])
            rownum_iter  = rownum_iter +1 
        
        container_taskStateSummary.write("\n\n") 




def getLongestCompilation(session): 

        v_getLongestCompl = f''' select query_type as query_type,  
                                    task_name as task,     
                                    root_task_name as root_task  ,                                 
                                    to_varchar(compilation_time_in_sec)  as compilation_time ,                               
                                    to_varchar(total_time_in_sec)  as total_elapsed_time ,
                                    lower(root_task_scheduled_from) triggered_as, 
                                    to_varchar(task_run_dt, 'YYYY-MM-DD') as execution_dt, 
                                    task_state as status
                                from  taskmonitoring.core.task_statistics_top5 
                                where category = 'Compilation time' order by compilation_time_in_sec desc ;  '''
        compl_df =  pd.DataFrame(session.sql(v_getLongestCompl).collect())

        #header and explanations 
        #st.image("compilation_time.jpg", width = 620)       
        st.header("  :clock9: **:orange[COMPILATION TIME]**")
        st.write("\n\n") 
        st.write(f" **Compilation time increases with the query complexity.This is the time metadata manager spends on creating an execution plan.**") 
        st.write(f" **To reduce the compilation time seeking to reduce the query complexity is usually a good strategy.** " )
    
        container_ComplSummary = st.container(border=True)
        container_ComplSummary.write("\n\n")

        if compl_df.empty: 
            row_num = 0 
            container_ComplSummary.markdown( f"**:blue[MAKE SURE THAT ANALYSIS IS RUN PROPERLY!]**" ) 
            container_ComplSummary.markdown( f"""**:blue[Go to "Setup & Configure" page]**""" ) 
        else: 
            row_num= compl_df["TASK"].count() 

        rownum_iter = 0 

        for row in compl_df.iterrows():
            task_name = compl_df["TASK"][rownum_iter]
            compilation_time = compl_df["COMPILATION_TIME"][rownum_iter]
            total_time = compl_df["TOTAL_ELAPSED_TIME"][rownum_iter]
            container_ComplSummary.markdown( '**Statement type:** ' + compl_df["QUERY_TYPE"][rownum_iter]) 
            container_ComplSummary.markdown( f" :blue[{task_name}] ") 
            container_ComplSummary.markdown( '**Root Task:** ' + compl_df["ROOT_TASK"][rownum_iter]) 
            container_ComplSummary.write( f" **Compliation time in sec:** **:orange[{compilation_time}]**  " )
            container_ComplSummary.markdown( f" **Total elapsed time in sec:**  **:orange[{total_time}]** "    ) 
            container_ComplSummary.markdown( '**Trigger:** ' + compl_df["TRIGGERED_AS"][rownum_iter])
            container_ComplSummary.markdown( '**Date:** ' + compl_df["EXECUTION_DT"][rownum_iter]) 
            container_ComplSummary.markdown( compl_df["STATUS"][rownum_iter]) 

            rownum_iter  = rownum_iter +1 

            # divident 
            if rownum_iter < row_num: 
               container_ComplSummary.write("__" * 34) 
 
def getLongestExec(session): 
    
        v_getLongestExec = f''' select query_type as query_type,  
                                    task_name as task,     
                                    root_task_name as root_task  ,                                 
                                    to_varchar(execution_time_in_sec)  as execution_time ,                               
                                    to_varchar(total_time_in_sec)  as total_elapsed_time ,
                                    lower(root_task_scheduled_from) triggered_as, 
                                    to_varchar(task_run_dt, 'YYYY-MM-DD') as execution_dt, 
                                    task_state as status
                                from  taskmonitoring.core.task_statistics_top5 
                                where category = 'Execution time' order by execution_time_in_sec desc ;  '''
        exec_df =  pd.DataFrame(session.sql(v_getLongestExec).collect())
        
        st.header(" :clock9: **:orange[EXECUTION TIME]**") 
        st.write("\n\n") 
        st.write(f" **CPU requiring jobs have longer execution times. Focus on eliminating SORT, GROUPING operations, reducing number of columns and rows. And don't forget to use cache if possible.**") 
        
        container_ExecSummary = st.container(border=True)
        container_ExecSummary.write("\n\n")

        if exec_df.empty:  
            row_num = 0 
            container_ExecSummary.markdown( f"**:blue[MAKE SURE THAT ANALYSIS IS RUN PROPERLY!]**" ) 
            container_ExecSummary.markdown( f"""**:blue[Go to "Setup & Configure" page]**""" ) 
        else: 
            row_num= exec_df["TASK"].count() 
        
        rownum_iter = 0 
        
        for row in exec_df.iterrows():
            task_name =  exec_df["TASK"][rownum_iter]
            exec_time = exec_df["EXECUTION_TIME"][rownum_iter] 
            total_time = exec_df["TOTAL_ELAPSED_TIME"][rownum_iter] 
            container_ExecSummary.markdown( '**Statement type:** ' + exec_df["QUERY_TYPE"][rownum_iter] ) 
            container_ExecSummary.markdown( f" :blue[{task_name}] ") 
            container_ExecSummary.markdown( '**Root Task:**  ' + exec_df["ROOT_TASK"][rownum_iter]) 
            container_ExecSummary.write( f"**Execution time in sec:** **:orange[{exec_time}]** "  )
            container_ExecSummary.markdown( f"**Total elapsed time in sec:** **:orange[{total_time}]**"  ) 
            container_ExecSummary.markdown( '**Trigger:** ' + exec_df["TRIGGERED_AS"][rownum_iter])
            container_ExecSummary.markdown( '**Date:** ' + exec_df["EXECUTION_DT"][rownum_iter]) 
            container_ExecSummary.markdown( exec_df["STATUS"][rownum_iter]) 
            rownum_iter  = rownum_iter +1 

            if rownum_iter < row_num: 
                container_ExecSummary.write("__" * 34) 


def getScanStats(session):
        v_getScanStat = f''' select task, 
                            query_type , 
                            root_task , 
                            triggered_as , 
                            to_varchar(execution_dt , 'YYYY-MM-DD') as execution_dt , 
                            to_varchar(performance_indicator) as performance_indicator , 
                            is_bytes_spilled , 
                            status 
                        from taskmonitoring.core.task_storage_scan_perf_top5 ;    '''
        scan_df =  pd.DataFrame(session.sql(v_getScanStat).collect())        
        
        #Header and explanations 
        st.header("  :mag: **:orange[STORAGE SCAN PERFORMANCE]**")         
        st.write("\n\n") 
        st.write(f" **Bytes spilled to local disk or remote disk is an indication of insufficient memory or poorly designed query that consumes lots of memory.** " ) 
        st.markdown(f" **High partition scan percentage, GROUP BY, SORT operations can cause byte spilling. This effects performance and resource consumption. Performance problems caused by this issue can be fixed by tuning the query with partition pruning, reducing the number of columns and rows, avoiding memory consuming operations.** ")  
        st.write(f"**The 5 tasks below are the tasks that have higher PARTITION SCAN RATES from the tables which have already high number of partitions as well as tasks that have high SPILLED BYTES AMOUNT.** ")
        container_scanStat = st.container(border=True)
        container_scanStat.write("\n\n")

        if scan_df.empty:  
            row_num = 0       
            container_scanStat.markdown( f"**:blue[MAKE SURE THAT ANALYSIS IS RUN PROPERLY!]**" ) 
            container_scanStat.markdown( f"""**:blue[Go to "Setup & Configure" page]**""" ) 
        else: 
            row_num= scan_df["TASK"].count() 
        
        rownum_iter = 0 
        
        for row in scan_df.iterrows():
            taskName = scan_df["TASK"][rownum_iter] 
            container_scanStat.markdown( '**Statement type:** ' + scan_df["QUERY_TYPE"][rownum_iter] ) 
            container_scanStat.markdown( f" :blue[{taskName}] ") 
            container_scanStat.markdown( '**Root Task:** ' + scan_df["ROOT_TASK"][rownum_iter]) 

            ind_performance = scan_df["PERFORMANCE_INDICATOR"][rownum_iter] 

            #listing top 5 tasks either have highests value for byte spilled or scan percentage
            if   pd.to_numeric(scan_df["IS_BYTES_SPILLED"][rownum_iter])   == 1 and rownum_iter < 3 :  
                container_scanStat.write(  f" **Total bytes spilled:**  **:orange[{ind_performance}]** "    )
            if pd.to_numeric(scan_df["IS_BYTES_SPILLED"][rownum_iter])  == 0 : 
                container_scanStat.write(  f" **Partition scan percentage:**  **:orange[{ind_performance}]**  "    ) 
            
            container_scanStat.markdown( '**Trigger:** ' + scan_df["TRIGGERED_AS"][rownum_iter])
            container_scanStat.markdown( '**Date:** ' + scan_df["EXECUTION_DT"][rownum_iter]) 
            container_scanStat.markdown( scan_df["STATUS"][rownum_iter]) 
            rownum_iter  = rownum_iter +1 

            if rownum_iter < row_num: 
                container_scanStat.write("__" * 34) 


def runSummary(session):

    st.header(f"  :pencil: **:orange[SUMMARY OF TASK STATE ]**") 
    st.write("**Lists suspended tasks at the time of analysis, top 5 tasks that have longest compilation and execution times along with highest spillage or partition scan percentages in the last 3 months.**")
    #img.image("summary.jpg", width = 600)
    
    writeTabs()
    getTaskInfo(session) 

    writeTabs()
    getLongestCompilation(session) 

    writeTabs()
    getLongestExec(session)
    
    writeTabs()
    getScanStats(session) 


writeTabs() 
runSummary(session)