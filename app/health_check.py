# Import python packages
import streamlit as st
from snowflake.snowpark.session import Session 
from streamlit_extras.stylable_container import stylable_container
import plotly.express as px

import pandas as pd 
import toml
import os


def tabs():
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


tabs()
st.header(f"  :stethoscope: **:orange[TASK HEALTH]**") 
st.write("**Health Check** analyzes task queries that have max execution time for the last **14 days.** ") 
st.write("In the analysis, **processing events** that **consume resources the most**; are displayed. **Possible actions** that can be taken for improving task performance are listed to reduce the cost.")
st.write("To continue with the analyses, you can choose the task below you want to go through in a deeper level.")

tabs()
st.write("**:blue[Insert database and task names below :]** ")

left, right = st.columns(2)
v_getDatabase_stm = f'''  select distinct th.database_name  
                            from taskmonitoring.core.QUERY_STATS  st 
                              inner join taskmonitoring.core.tasks_for_health_check th 
                               on st.query_id = th.query_id ;  '''
databaseName_dt = pd.DataFrame(session.sql(v_getDatabase_stm).collect() )
databaseName = left.selectbox("**Database name:** " , databaseName_dt)


v_getTasks_stm = f'''  select distinct th.task_name  
                       from taskmonitoring.core.QUERY_STATS  st 
                         inner join taskmonitoring.core.tasks_for_health_check th 
                           on st.query_id = th.query_id 
                        where th.database_name = '{databaseName}'
                            order by 1 ;  '''
getTasks_dt = pd.DataFrame( session.sql(v_getTasks_stm).collect() ) 
taskName = left.selectbox("**Task name:** ", getTasks_dt)
left.write("\n\n")

tabs()
WH_analyse = st.container(border = True)
WH_analyse.header(" :gear: ")   

v_wh_or_serverless_stm = f"""  with avg_exec as 
                                    (
                                    select task_name ,  
                                        warehouse_name , 
                                        count(*) exec_count, 
                                        lpad(month(to_date(TASK_QUERY_START_TIME)),2, '0')  || '-' ||  year(to_date(TASK_QUERY_START_TIME))  exec_month , 
                                        avg(TOTAL_ELAPSED_TIME) avg_exec_time  
                                        from taskmonitoring.core.TASK_RUN_HISTORY 
                                        where task_name = '{taskName}' 
                                        group by lpad(month(to_date(TASK_QUERY_START_TIME)),2, '0')  || '-' ||  year(to_date(TASK_QUERY_START_TIME)) , 
                                        task_name , 
                                        warehouse_name 
                                        order by 4 limit 1 
                                    ) 
                                    select task_name ,  
                                        ae.warehouse_name , 
                                        exec_count, 
                                        exec_month , 
                                        avg_exec_time/1000  as avg_exec_time ,  
                                        nvl(round(100 - (total_idle_hours/ total_hours ) * 100, 2 ), 0) as wh_utilization 
                                    from avg_exec ae  
                                        left outer join core.WAREHOUSE_UTILIZATION_MONTHLY um 
                                        on um.warehouse_name = ae.warehouse_name 
                            """

wh_or_serverless_dt = pd.DataFrame(session.sql(v_wh_or_serverless_stm).collect()) 

if not wh_or_serverless_dt.empty: 
    wh_task = wh_or_serverless_dt["WAREHOUSE_NAME"][0]
    WH_analyse.write(f"**:orange[Warehouse]** : {wh_task}" )

    if wh_or_serverless_dt["WH_UTILIZATION"][0] == 0: 
        WH_analyse.write( f""" **:blue[The task does not run on a STANDART warehouse. Compute pools, applications computes can not be included in "swap to serverless" analysis!]** """   )
    else: 
        if wh_or_serverless_dt["EXEC_COUNT"][0] >= 20 and wh_or_serverless_dt["WH_UTILIZATION"][0] <= 50 and wh_or_serverless_dt["AVG_EXEC_TIME"][0] <= 100 : 
            WH_analyse.write("**:blue[You may want to consider changing task to a serverless task. The reasons for this are:]**")
            WH_analyse.write("- Warehouse utilization is too low. ")
            WH_analyse.write("- The task is executed frequently which means more resumes and suspends are applied to the warehouse. ")
            WH_analyse.write("- The execution time of the task is not too high.  ")
else: 
    WH_analyse.write(f"**:blue[Check if the analysis are run correctly.]** ")

tabs()
st.write("**:orange[!PS]**")
st.write("**The analysis below is done only for the tasks using warehouses. Serverless tasks are not included in the analysis.**")
st.write("**The analysis is based on the last queries ran by the tasks within the last 14 days.**")

tabs()
st.header(f"  :gear: **:orange[PROCESSING EVENTS]**" )


v_getDiagramStats_stm =  f'''  select st.query_id , 
                                    th.task_name, 
                                    sum(nvl(st.total_time, 0)) as total_time ,  
                                    sum(nvl(st.init_time, 0)) as init_time,
                                    sum(nvl(st.network_time, 0)) as network_time,
                                    sum(nvl(st.remote_disk_io_time, 0)) as remote_disk_io_time, 
                                    sum(nvl(st.processing_time, 0)) processing_time , 
                                    sum(nvl(st.local_disk_io_time, 0)) local_disk_io_time                        
                                from   taskmonitoring.core.QUERY_STATS  st 
                                inner join taskmonitoring.core.tasks_for_health_check th 
                                on st.query_id = th.query_id 
                            where th.task_name = '{taskName}'
                                group by  st.query_id , th.task_name ;  '''


getDiagramStats_dt = pd.DataFrame(session.sql(v_getDiagramStats_stm).collect())

analyseContainer = st.container(border = True)
analyseContainer.write("\n\n")
analyseLeft, analyseRight = analyseContainer.columns(2)

if not getDiagramStats_dt.empty: 
    processing_Times =   'Init', 'Network', 'Remote Disk IO', 'Processing' , 'Local Disk IO' 
    labels =   {  "names":  ['Init', 'Network', 'Remote Disk IO', 'Processing' , 'Local Disk IO' ] }
    labels = pd.DataFrame(labels)

    sizes = [ pd.to_numeric(getDiagramStats_dt['INIT_TIME'][0]), 
              pd.to_numeric(getDiagramStats_dt['NETWORK_TIME'][0]), 
              pd.to_numeric(getDiagramStats_dt['REMOTE_DISK_IO_TIME'][0]), 
              pd.to_numeric(getDiagramStats_dt['PROCESSING_TIME'][0]) , 
              pd.to_numeric(getDiagramStats_dt['LOCAL_DISK_IO_TIME'][0]) ]
    explode = (0, 0, 0,0.1, 0 )

    sumSize = pd.to_numeric(getDiagramStats_dt['INIT_TIME'][0]) +   pd.to_numeric(getDiagramStats_dt['NETWORK_TIME'][0]) +   pd.to_numeric(getDiagramStats_dt['REMOTE_DISK_IO_TIME'][0]) +   pd.to_numeric(getDiagramStats_dt['PROCESSING_TIME'][0]) + pd.to_numeric(getDiagramStats_dt['LOCAL_DISK_IO_TIME'][0]) 
 
    if sumSize != 0: 
        fig = px.pie(labels, values= sizes , names= processing_Times, title="Processing time ratios",)
        analyseRight.plotly_chart(fig, theme=None)

    else: 
        warning = ":red[TOTAL EXECUTION TIME FOR THE TASK IS NOT AVAILABLE!] "
        analyseRight.subheader(warning)
        analyseRight.subheader("To see the diagram choose a task with total execution time greater than zero.")
        analyseContainer.write("\n\n")
else:     
    analyseContainer.write( f"**:blue[Run the analysis with correct privileges to see the diagram.]** "   )
    analyseContainer.write( f"**:blue[Check if the analysis are run correctly.]** "   )


v_analyseStat_stm = f'''  select st.query_id , th.task_name, 
                                st.total_time as total_time ,  
                                st.init_time  as init_time,
                                st.network_time  as network_time,
                                st.remote_disk_io_time  as remote_disk_io_time, 
                                st.processing_time  processing_time , 
                                st.local_disk_io_time  local_disk_io_time , 
                                st.operator_type , 
                                join_type, 
                                joincondition,
                                groupkeys, 
                                functions, 
                                filtercondition, 
                                tablescancolumns , 
                                tablename, 
                                sortkeys , 
                                additional_joincondition
                        from   taskmonitoring.core.QUERY_STATS  st 
                        inner join taskmonitoring.core.tasks_for_health_check th 
                        on st.query_id = th.query_id 
                    where th.task_name = '{taskName}'
                            and st.total_time > 0 
                        order by st.total_time desc limit 4  '''    


analyseSytat_df = pd.DataFrame(session.sql(v_analyseStat_stm).collect() )

if not analyseSytat_df.empty: 

    analyseLeft.write( f"**:blue[{taskName}]** " )
    analyseLeft.write( "See the most expensive **EVENTS** during the task execution below. Focus on performance improvements on these events if necessary. "   )
    i = 0 
    for row in analyseSytat_df.iterrows() :
        analyseLeft.divider()
        operator = analyseSytat_df["OPERATOR_TYPE"][i]       
        total_time =  analyseSytat_df["TOTAL_TIME"][i]   
        analyseLeft.write(f"**:blue[{operator}]** - Total time: **:orange[{total_time}]** ")
        if analyseSytat_df["OPERATOR_TYPE"][i] == 'Join': 
            analyseLeft.write( "**Join Condition:** " + analyseSytat_df["JOINCONDITION"][i] )  
            analyseLeft.write( "**Join Type:** " + analyseSytat_df["JOIN_TYPE"][i] )  
        if analyseSytat_df["OPERATOR_TYPE"][i] == 'Filter': 
            analyseLeft.write("**Filter Condition:** " +  analyseSytat_df["FILTERCONDITION"][i]) 
        if analyseSytat_df["OPERATOR_TYPE"][i] == 'WindowFunction': 
            analyseLeft.write( "**Functions:** " + analyseSytat_df["FUNCTIONS"][i] ) 
        if analyseSytat_df["OPERATOR_TYPE"][i] == 'TableScan': 
            analyseLeft.write( "**Table name:** " + analyseSytat_df["TABLENAME"][i] ) 
            analyseLeft.write( "**Columns:** " + analyseSytat_df["TABLESCANCOLUMNS"][i] ) 
        if analyseSytat_df["OPERATOR_TYPE"][i] == 'Aggregate': 
            analyseLeft.write(  "**Functions:** " + analyseSytat_df["FUNCTIONS"][i] ) 
            analyseLeft.write( "**Keys:** " +  analyseSytat_df["GROUPKEYS"][i] ) 
        if analyseSytat_df["OPERATOR_TYPE"][i] == 'CreateTableAsSelect': 
            analyseLeft.write("**Table name:** " +  analyseSytat_df["TABLENAME"][i] )
        if analyseSytat_df["OPERATOR_TYPE"][i] == 'Sort': 
            analyseLeft.write(  "**Keys:** " +  analyseSytat_df["SORTKEYS"][i] )
        if analyseSytat_df["OPERATOR_TYPE"][i] == 'CartesianJoin': 
            analyseLeft.write("**Join Condition:** " + analyseSytat_df["ADDITIONAL_JOINCONDITION"][i] )
            analyseLeft.write(  "**Join Type:** " + analyseSytat_df["JOIN_TYPE"][i] )
        i = i + 1 
    analyseLeft.write( "\n\n" )
    
    
tabs()
st.header(f" :memo: **:orange[ANALYZING DIAGRAM]** "  )
st.write("\n\n")
st.write("**Total Time:**  ")   
st.write("Total execution time of the task statement task. ")   
st.divider()
st.write("**Init Time:**  ") 
st.write("Compilation time for the task statement. If this is high, simplifying the query for making it easier to compile the metadata for Snowflake may increase the performance. Warehouse size does not have an impact on compilation time, therefore increasing the size of the warehouse will not decrease the compilation time and can not be used as a solution here. ")
st.divider()
st.write("**Processing Time:** ") 
st.write("Time for running the query statement. If this is high, it means your query is complex and you may be using a lot of warehouse resources to run the query. You can try to reduce the amount of data here by **filtering out the rows** and **reducing the number of columns.** Also you can avoid using **expensive functions** such as **SORT**, **GROUP BY** and **window functions.** Other things you can do are;   ")         
st.write("- Increase WH size, if you think it is necessary. Keep in mind that increasing the WH size does not always resolve the problem with query execution time. It should be done when there are lots of queries queueing up, number of CPU and memory is not enough for the type of queries running in the WH.")
st.write("- Use clusters in the join predications. Cluster maintanence also comes with a cost, so this also should be avoided if it does not bring significant benefits.") 
st.write("- Use cache if you can") 
st.write("- Increase the number of max clusters")
st.divider()
st.write("**Local Disk IO Time:**  ")  
st.write("When the data is not cached at the **Result Cache**, but cached at the WH cache, this parameter shows the time that is spent to bring the data from the WH cache. ")  
st.divider()
st.write("**Remote Disk IO Time:**   ")   
st.write("Time spent to read the data from storage. Here the data is not cached neither in the result cache nor in WH cache. If it is possible to use the cache use a warehouse that is suspended less frequently therefore can cache the data. ")   
st.divider()
st.write("**Network Time:**  ")    
st.write("That is the time spent while waiting the data to be transfered over the network.  ")    