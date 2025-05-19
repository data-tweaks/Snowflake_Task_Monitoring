import streamlit as st 
from streamlit_extras.stylable_container import stylable_container
from snowflake.snowpark.context import get_active_session
import pandas as pd 
import time


page_element="""
<style>
[data-testid="stAppViewContainer"]{
  background-image: url("http://data-tweaks.com/wp-content/uploads/2025/03/data_tweaks_reas.png") ;  
  background-size: cover;
}
</style>
"""

st.markdown(page_element, unsafe_allow_html=True)
 

def  get_taskState_old(session) : 
    databases_df = session.sql('show databases;' ).collect()
    databases_df = pd.DataFrame(databases_df)

    databases_df = databases_df[databases_df["kind"] != 'IMPORTED DATABASE']

    for database_name_v in databases_df["name"] : 
        session.sql(f''' use database  { database_name_v } ;  ''') 
        tasks_df = session.sql(f''' show tasks in database  { database_name_v } ;  ''' ).collect() 
        tasks_df = pd.DataFrame(tasks_df)
        if len(tasks_df.index) > 0 :       
            tasks_df = tasks_df[[ "created_on", 
                                  "database_name" ,
                                  "schema_name", 
                                  "name" , 
                                  "id", 
                                  "state", 
                                  "error_integration", 
                                  "schedule", 
                                  "owner", 
                                  "last_suspended_on",
                                  "last_suspended_reason"]]
            
            row_iter = 0 
             
            for row in tasks_df.iterrows():
                t_created_on_v = tasks_df["created_on"][row_iter].date()
                t_database_name_v = tasks_df["database_name"][row_iter]
                t_schema_name_v = tasks_df["schema_name"][row_iter]
                t_name_v = tasks_df["name"][row_iter]
                t_id_v = tasks_df["id"][row_iter]
                t_state_v = tasks_df["state"][row_iter]
                t_error_integration_v = tasks_df["error_integration"][row_iter]
                t_schedule_v = tasks_df["schedule"][row_iter]
                t_owner_v = tasks_df["owner"][row_iter]
                t_last_suspended_on_v = tasks_df["last_suspended_on"][row_iter].date()
                t_last_suspended_reason_v = tasks_df["last_suspended_reason"][row_iter]
                row_iter = row_iter + 1 

                merge_sql_v = f'''  merge into core.task_state taskSt
                                      using 
                                    (select  '{t_created_on_v}' as created_on, '{t_database_name_v}' as database_name, 
                                       '{t_schema_name_v}' as schema_name, 
                                       '{t_name_v}' as name, '{t_id_v}' as id,  '{t_state_v}' as state, 
                                       '{t_error_integration_v}' as error_integration , 
                                       '{t_schedule_v}' as schedule, '{t_owner_v}' as owner, 
                                       '{t_last_suspended_on_v}' as last_suspended_on, 
                                       '{t_last_suspended_reason_v}' as last_suspended_reason) as src
                                     on taskSt.created_on = src.created_on 
                                       and taskSt.name = src.name 
                                       and taskSt.schema_name = src.schema_name 
                                       and taskSt.database_name = src.database_name 
                                       and taskSt.state = src.state                       
                                   when not MATCHED then insert  
                                       (  created_on, database_name, schema_name , name , id , state , error_integration, 
                                          schedule, owner , last_suspended_on , last_suspended_reason  )
                                     values   
                                       ( src.created_on, src.database_name, src.schema_name, src.name, src.id,  src.state, src.error_integration, 
                                        src.schedule,  src.owner, src.last_suspended_on , src.last_suspended_reason )  ;     '''

                session.sql(merge_sql_v).collect()




session = get_active_session()

st.header(f" **:grey[CONFIGURATION]**")
st.write("Before proceeding to review the analysis on **Summary**, **Task Cost** and **Health Check**; click **Configure & Run Analysis** button and configure the REAS TASK analyses app.")
st.write("\n\n")

with stylable_container(
    "green",
    css_styles="""
    button {
        background-color: #cac9c9;
        color: black;
    }""",
):
    container_config = st.container(border=True)
 
    container_config.write("\n\n")
    container_config.markdown("Snowflake native applications, as well as REAS; are secure by nature. It means they can access only to metadata and are not able to reach the underlying data, even with powerful security roles granted to them. Your data is never vulnerable even with the granted roles and is secured by Snowflake. ") 
    container_config.markdown("Read more about native apps and their secure nature in the links below :") 
    container_config.write("https://www.snowflake.com/guides/what-are-native-apps/  \n\n  https://www.snowflake.com/en/blog/snowflake-native-apps-security/ ")

    container_config.markdown( "For task monitoring analysis, REAS needs to access task history metadata tables. As described in the links above your data never leaves your account and it is secure to grant the roles to the application.")
    container_config.markdown(f''' Grant access to the metadata tables in the **Settings** section with required security roles to the REAS application. ''') 

    container_config.divider()

    container_config.write(f''' **:blue[!PS]** ''')
    container_config.write(f''' **:blue[On "Task Health Check" you can only analyse the tasks that are running on the warehouses which are listed here. If you can not see the warehouse you want to do analysis please grant monitor priviliges to the warehouse.]** ''')

    container_config.divider()

    # listing the privileges 
    container_config.write("**GRANTED PRIVILIGES ON APPLICATION:**")
    container_config.write("See more information on the link : https://data-tweaks.com/reas-task-monitoring/") 
    priviliges_dt = pd.DataFrame( session.sql( 'SHOW PRIVILEGES IN APPLICATION REAS_TASK;' ).collect() ) 

    if not priviliges_dt.empty: 
      priv_rownum = 0 
      for privRow in priviliges_dt: 
        if (priv_rownum < 2):
          assigned_priv = priviliges_dt["privilege"][priv_rownum] 
          if priviliges_dt["is_granted"][priv_rownum] == "true":
              container_config.write( f" - {assigned_priv} ") 
          else: 
            container_config.write(f"Please grant app the privilige **:blue[{assigned_priv}]** .") 
          priv_rownum = priv_rownum + 1 
        else:
           pass 

    container_config.write("**WAREHOUSES THAT ARE GRANTED MONITOR PRIVILIGES**")
    # Listing the grants on warehouses 
    getAllWHJSON_dt = pd.DataFrame( session.sql(f" select SYSTEM$GET_ALL_REFERENCES('CONSUMER_WAREHOUSE' , True) as JSONTEXT " ).collect()) 
    if not getAllWHJSON_dt.empty: 
        jsonVal = getAllWHJSON_dt["JSONTEXT"][0]

        v_add_char_open = "{"
        v_add_char_close = "}"
        v_getWHstm_select  =  f'''SELECT  v.value:name::varchar   as v_warehouses  from (SELECT PARSE_JSON(column1) AS src FROM VALUES (' {v_add_char_open} "value":   ''' 
        v_getWHstm_last = f'''  {v_add_char_close}' ) v ),   LATERAL FLATTEN(INPUT => SRC:value) v '''
        v_getWHstm = f" {v_getWHstm_select}   {jsonVal}  {v_getWHstm_last}  " 
        v_getWH = session.sql(v_getWHstm ).collect()
        getWH  = pd.DataFrame(v_getWH)

        row_iter = 0 
        for rowWH in getWH.iterrows() :
            wh_name = getWH["V_WAREHOUSES"][row_iter]
            container_config.write( f" - {wh_name}  ")
            row_iter = row_iter + 1 
          

    container_config.divider()
     
    container_config.write(f"Initial analysis runs for the last 3 months. **(Configuring can take more than 30 min depending on the WH size you are using and the data amount you have in account usage schema!)** ")
    start_analysis_txt = f""" Start analysis by clicking the **"RUN ANALYSIS"** button below.  """
    container_config.write(start_analysis_txt)
    
    container_config.write("\n\n")
    container_config.write("\n\n")
    run_button = container_config.button(f" **:grey[CONFIGURE & RUN ANALYSIS]**")
    container_config.write("\n\n")

 
    # running analyses 
    if run_button: 
        #get_taskState_old(session) 
        with st.spinner(text="In progress"): 
          time.sleep(3)
          
          #getting ata from account_usage schema views 
          v_run_analyse_proc = 'call CORE.INIT_ANALYSE() ; '
          session.sql(v_run_analyse_proc).collect()
          container_config.write("**:blue[Analysis run for the last 3 months is completed. ]**")
          container_config.write("**:blue[Running query stats for the last 14 days now.]**")

          #truncating query_stat table before we continue with query stats 
          session.sql('Truncate table core.QUERY_STATS ').collect()  ; 

          #running query_stats for each warehouse 
          i = 0 
          for row in getWH.iterrows() :
              wh_name = getWH["V_WAREHOUSES"][i]
              v_run_query_stats_proc = f" call CORE.INSERT_QUERY_STATS(  '{wh_name}' ) ; "
              session.sql(v_run_query_stats_proc).collect()
              container_config.write( f" **:blue[- Stats run completed for WH: {wh_name}]** ")
              i = i + 1 

          #message to convey job status 
          container_config.write("**:blue[Query stats execution is completed.]**")
          container_config.success("Task analysis has been successfully completed.")
        




