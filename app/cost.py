import streamlit as st 
from snowflake.snowpark.session import Session 
import pandas as pd 
import toml
import os


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

def tabs(): 
    st.write("\n\n")
    st.write("\n\n")
    st.write("\n\n")

tabs()
st.header(" :chart_with_downwards_trend:  **:orange[COST ANALYSIS]**" )    
st.write("**Cost Analysis** navigates standart and serverless warehouses, **credit consumption** and presents charts for analysing the cost and credit consumption trend in monthly basis.") 

st.write("\n\n")
st.write("\n\n")
st.header(f" :bar_chart: **:orange[MONTHLY USED CREDITS]** ")
st.write("The chart below shows total credit consumption of tasks running on standart **warehouses** for the given month. You can navigate the values by moving the mouse on the chart.")


MonthlyChartContainer = st.container(border = True)
MonthlyChartContainer.write("\n\n")

dt, text = MonthlyChartContainer.columns(2) 

v_selectMonth = f'''  select distinct period_dt from  taskmonitoring.core.TASK_WH_USAGE_PARAMETRIZED where parameter = 'wh_monthly' ;  '''
month_df = pd.DataFrame(session.sql(v_selectMonth).collect())

if month_df.empty: 
    v_monthInput = "1900-01-01"
else: 
    v_monthInput = dt.selectbox( "**Select a month:** ",  month_df['PERIOD_DT'] )
text.write("\n\n")

v_serverless_task_usage_stm = f'''  select  period_dt, credits_used 
                                        from  taskmonitoring.core.TASK_WH_USAGE_PARAMETRIZED where parameter = 'Serverless'
                                        and  period_dt =  '{v_monthInput}' ;  ''' 

serverless_task_usage_dt = pd.DataFrame(session.sql(v_serverless_task_usage_stm).collect())

if not serverless_task_usage_dt.empty  : 
    serverless_task_usage_dt['CREDITS_USED']  = pd.to_numeric( serverless_task_usage_dt['CREDITS_USED']  )
    serverless_task_usage_sum = serverless_task_usage_dt['CREDITS_USED'].sum().round(3).astype(str)
else: 
    serverless_task_usage_sum = "0" 


v_wh_cost_stm = f'''  select  period_dt, warehouse_name,   sum(credits_used)  as credits_used 
                        from  taskmonitoring.core.TASK_WH_USAGE_PARAMETRIZED
                        where parameter = 'wh_monthly'
                            and period_dt = '{v_monthInput}'  
                        group by period_dt, warehouse_name ;  '''


wh_cost_df = pd.DataFrame(session.sql(v_wh_cost_stm).collect()) 

if not wh_cost_df.empty:
    wh_cost_df['CREDITS_USED'] =   pd.to_numeric(wh_cost_df['CREDITS_USED'])


    MonthlyChartContainer.write("\n\n")
    MonthlyChartContainer.bar_chart(wh_cost_df, x= "WAREHOUSE_NAME", y=  "CREDITS_USED" ,  x_label = "Warehouse" , y_label = "Total Used Credits" ,color=  "CREDITS_USED"    , stack="layered" , height = 520  )
    MonthlyChartContainer.write("\n\n")

    v_TotalusedCredits_df = wh_cost_df['CREDITS_USED'].sum().round(3).astype(str)

    text.write( f"Total **:blue[STANDART WH credits]** used for task execution: **:orange[{v_TotalusedCredits_df}]** "   )
    text.write( f"Total **:blue[SERVERLESS credits]** used for task execution:  **:orange[{serverless_task_usage_sum}]** "     )
else: 
    dt.write( f"**:blue[Run the analysis with correct privileges to see data here.]** "   )
    dt.write( f"**:blue[Check if the analysis are run correctly.]** "   )



tabs()
st.header(f" :bar_chart: **:orange[TOTAL USED CREDITS ANALYSIS]** ")
st.write("Navigate the charts to see total credit consumption on the account for standart and serverless warehouses for the last 3 months. You can use the charts to analyse the credit consumption patterns for the last 3 months timeline.")

chartContainer = st.container(border = True)
chartContainer.write("\n\n")

chartContainer.write("**:grey[WAREHOUSE TOTAL CREDIT CONSUMPTION]**")

whChart, whText  = chartContainer.columns(2)

v_wh_cost_inMonths_stm = f''' select  period_dt,   sum(credits_used) as credits_used 
                                from  taskmonitoring.core.TASK_WH_USAGE_PARAMETRIZED
                                where parameter = 'wh_monthly'
                                group by period_dt  ;  '''

wh_costInMonths_dt = pd.DataFrame(session.sql(v_wh_cost_inMonths_stm).collect())

if not wh_costInMonths_dt.empty:
    wh_costInMonths_dt['CREDITS_USED'] = pd.to_numeric(wh_costInMonths_dt['CREDITS_USED'])
    whChart.line_chart(wh_costInMonths_dt, x= "PERIOD_DT", y=  "CREDITS_USED" ,  x_label = "Period" , y_label = "Total Used Credits" , height = 350 ) 
else: 
    whChart.write( f"**:blue[Check if the analysis are run correctly.]** "   )


whText.write("Running tasks on warehouses may not be the best idea for all tasks. If the warehouse is mostly idle, it will be resumed for task executions and will be suspended after executions.")
whText.write("Resumes and suspends are very expensive especially if they happen repeatedly on idle warehouses. This keeps the warehouse up constantly. Therefore to repeat this process only for running specific tasks in short time intervals increases the cost .") 
whText.write("For this kind of task executions consider using serverless tasks especially for the tasks that do not have long execution times.")
whText.write("Otherwise, using warehouses for tasks can be beneficial for cache usage.")

chartContainer.write("\n\n")
chartContainer.divider() 
chartContainer.write("\n\n")
chartContainer.write("\n\n")


chartContainer.write("**:grey[SERVERLESS TOTAL CREDIT CONSUMPTION]**")

serverlessChart, serverlessText  = chartContainer.columns(2)
serverlessText.write("Serverless compute resources are 1,5 times more expensive than standart warehouses. So serverless warehouses should be used for the tasks that have shorter execution times and for the tasks that have tendency to resume and suspend the warehouses constantly.")

v_sless_cost_inMonths_stm = f''' select  period_dt, sum(credits_used) as  credits_used
                                    from  taskmonitoring.core.TASK_WH_USAGE_PARAMETRIZED 
                                where parameter = 'Serverless'
                                group by period_dt     
                                    order by 1 ; '''

sless_costInMonths_dt = pd.DataFrame(session.sql(v_sless_cost_inMonths_stm).collect())

if not sless_costInMonths_dt.empty:
    sless_costInMonths_dt['CREDITS_USED'] = pd.to_numeric(wh_costInMonths_dt['CREDITS_USED'])
    serverlessChart.line_chart(sless_costInMonths_dt, x= "PERIOD_DT", y=  "CREDITS_USED" ,  x_label = "Period" , y_label = "Total Used Credits" , height = 350 ) 
    serverlessChart.write("\n\n") 
else: 
    serverlessChart.write( f"**:blue[Check if the analysis are run correctly or if there are any tasks running on serverless warehouses.]** ")  
    serverlessChart.write("\n\n") 

tabs() 
st.header(f" :bar_chart: **:orange[TASK EXECUTION ANALYSIS]** ")
st.write("Analyze task executions and see their warehouse credit consumption independently. ")
st.write("Credit consumption can be analyzed for the last 3 months. Only warehouse tasks can be analyzed here.")

chartContainerTask = st.container(border = True) 
chartContainerTask.write("\n\n")

leftTaskChart , rightTaskChart = chartContainerTask.columns(2)     
leftTaskChart.write("**Insert required date interval for WH consumption analysis.**")
fromDateCol, toDateCol = leftTaskChart.columns(2) 

defaultFromDate = pd.DataFrame(session.sql("select case when  date_trunc(month, current_date())  = current_date() then  add_months(date_trunc(month, current_date()), -1)  else date_trunc(month, current_date()) end  as from_Date ").collect())

fromDate = fromDateCol.date_input("**From date:** " , pd.to_datetime(defaultFromDate["FROM_DATE"][0]))
toDate = toDateCol.date_input("**To date:** ")

v_getDatabaseName_stm = f'''select distinct database_name  from  taskmonitoring.core.TASK_WH_USAGE_PARAMETRIZED where parameter = 'wh_daily' and period_dt  between '{fromDate}' and '{toDate}' ;  '''

getDBName_dt = session.sql(v_getDatabaseName_stm).collect()

leftTaskChart.write("\n\n") 
leftTaskChart.write("**Select database and task:** ") 

dbName = leftTaskChart.selectbox("**Select database:** " , getDBName_dt)

v_getTasks_stm = f''' select distinct task_name  
                         from  taskmonitoring.core.TASK_WH_USAGE_PARAMETRIZED 
                       where parameter = 'wh_daily' and period_dt  between '{fromDate}' and '{toDate}' 
                            and database_name = '{dbName}' ;  '''

getTaskName_dt = session.sql(v_getTasks_stm).collect() 

taskName=  leftTaskChart.selectbox("**Select task:** " , getTaskName_dt)

v_taskCostChart_stm = f'''   select period_dt,  task_name , sum(credits_used) as credits_used 
                              from  taskmonitoring.core.TASK_WH_USAGE_PARAMETRIZED 
                            where parameter = 'wh_daily' and period_dt  between '{fromDate}' and '{toDate}' 
                              and database_name ='{dbName}'  and  task_name = '{taskName}' 
                              group by  task_name , period_dt ;  '''
taskChart_dt = pd.DataFrame(session.sql(v_taskCostChart_stm).collect() )


if  taskChart_dt.empty: 
    taskChart_dt["CREDITS_USED"] = 0 
    taskChart_dt["PERIOD_DT"] = '1900-01-01'


taskChart_dt["CREDITS_USED"] = pd.to_numeric(taskChart_dt["CREDITS_USED"])
rightTaskChart.line_chart(taskChart_dt, x= 'PERIOD_DT', y=  'CREDITS_USED' ,  x_label = "Date" , y_label = "Total Used Credits" , height = 350 ) 
rightTaskChart.write("\n\n")
