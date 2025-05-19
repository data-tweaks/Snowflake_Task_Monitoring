
import streamlit as st


Configuration = st.Page("app_config.py", title="Setup & Configure", icon="🚦")
Summary = st.Page("summary.py", title="Summary", icon="🗒️")
Cost = st.Page("cost.py", title="Task Cost", icon="💰")
Health = st.Page("health_check.py", title="Improvements & Health Check", icon="📈")
Contact = st.Page("contact.py", title="Contact", icon="📧")


page = st.navigation([ Configuration, Summary,  Cost, Health ,  Contact ])
page.run() 
