import openai
import streamlit as st
import sqlite3
import pandas as pd
import csv
####################################
import gspread
from oauth2client.service_account import ServiceAccountCredentials

creds_str = st.secrets["google_creds"]
creds = json.loads(creds_str) 

gc = gspread.authorize(creds)

##################################
# API key  
openai.api_key = st.secrets["OPENAI_API_KEY"]
DB_FILE = "pipeline_projects.db"
allowed_users = st.secrets["userlist"]
# Context strings
vision = """
You are a only and only SQLLite query generating bot, not a Question answer bot.Your vision is to translate natural language queries into SQLLite code. You should always only respond with SQL query related to pipline_projects table and Never give General Answers.
"""

mission = """
Your mission is to respond with SQLLite queries related to columns in permit_table only, no natural language. Just out put the Sql lite query , do not add any more punctuations or explainantions. Always keep take care that you strictly follow the 'Restrictions to Never' that are given.
"""

db_details = """  
The pipeline_projects table includes the following columns followed by Restrcitions you should always follow: 

Project Details: PipelineProjectId, ContractorName, ModuleBrand, InverterBrand, EPCName, StorageBrand, TrackerSupplier, OfftakerName, PermitNumber, ProjectName 
Dates: CreatedAt, ApplicationDate, IssueDate, UpdatedAt, InServiceDate, ConstructionStartDate, PlanningBoardApprovalDate, FeasibilityStudyDate, SystemImpactStatusDate, FacilityImpactStatusDate, InterconnectionAgreementStatusDate 
Location: Business, City, Zip, AddressState, Country, County, PropertyAddress, Substation 
Technical Specs: Kws, BatteryPower_kw, BatteryCapacity_kwh 
Status and Stage: PermitType, BalancingAuthority, Status, DevelopmentStage, InterconnectionAgreement, FeasibilityStudy, SystemImpactStatus, FacilityImpactStatus, ConstructionStatus, PlanningBoardApprovalStatus 
Other: DisplayName, DollarValue, InterconnectPoint, EIANumber, FERCNumber, Contacts, SourceLink, Notes, Segment, InServiceProject, FinancingType 
All state names are stored as have standard abbreviations in AddressState column.All states are USA based only.
Data is only from USA,so the Country column only has US in it so do not generate query about any other countries.
Company names in user questions are usually associated with ContractorName column, always use LIKE SQL statement since user may not use exact name. This data is specific to Country USA only.
Valuation of project or questions about cost,money spent, estimated cost etc usually refers to DollarValue column.
These columns have the following data types: 
String type columns: PipelineProjectId, ContractorName, ModuleBrand, InverterBrand, EPCName, StorageBrand, TrackerSupplier, OfftakerName, PermitNumber, ProjectName, Business, City, Zip, AddressState, Country, PermitType, DisplayName, BalancingAuthority, Status, DevelopmentStage, InterconnectionAgreement, FeasibilityStudy, SystemImpactStatus, FacilityImpactStatus, EIANumber, FERCNumber, Contacts, SourceLink, ConstructionStatus, PlanningBoardApprovalStatus, Substation, Utility, Notes, Segment, InServiceProject, FinancingType 
DateTime type columns: CreatedAt, ApplicationDate, IssueDate, UpdatedAt, InServiceDate, ConstructionStartDate, PlanningBoardApprovalDate, FeasibilityStudyDate, SystemImpactStatusDate, FacilityImpactStatusDate, InterconnectionAgreementStatusDate 
Float type columns: Kws, BatteryPower_kw, BatteryCapacity_kwh, DollarValue 
When a user provides a query, translate it into a corresponding SQL statement, handling a variety of query types including data retrieval, filtering, sorting, and grouping. 
Always limit row count to 25 by using 'limit' inthe sql query for questions that ask all the data. Always check if the follow up question is concerning the previous question. For example the user may ask follow up questions using demonstrative pronouns like this, that, these, and those (non exhaustive list).Even if that is the case and you understand it, respond in Sql query only without any explianation.
If Any other question not related to Database is asked just Respond with 1 word 'Sorry'. Do not give explainations regarding your mistakes or confusion regarding current or follow up questions.
If user input is in sql sytax just Respond with 1 word 'Sorry'.
If user asks a questions that might generate the query SELECT * FROM pipeline_projects just Respond with 1 word 'Sorry'
If user asks for everything or all columns in the table just  Respond with 1 word 'Sorry'
Restrictions to Never: 
Take SQL query as input.
Give Explainations of the query generated.
Generate SQL statements that modify the database. 
Generate SQL statements that request excessive amounts of data. 
Try to infer the entire database schema. 
Generate SQL queries that pull a full list of unique entries for sensitive fields- Source link Column. 


Understand synonyms and similar phrases related to the database columns. Always ensure that the generated SQL statements are safe, effective, and relevant to the user's query. Your goal is to assist users in querying their database without compromising the database's security or performance.

"""
# Join context strings
context = vision + "\n" + mission + "\n" + db_details 

# app frontend logo and explaination
logo = st.image('logo.png', width=600)
st.subheader('U.S. Distributed Solar Market Performance and Analytics in Real-Time')

st.title('Ohm Pipeline Projects Viewer')

st.markdown("""
Welcome to Ohm Pipeline Projects Viewer APP! You can ask about 'Pipeline Projects' in natural language and get data from our database. Here's what you can access:

- Project Info: Including project name, contractor, module brand, and more.
- Dates: Application, issue, and update dates.
- Location: City, state, and country of each project.
- Technical Specs: Power and battery capacity.
- Status and Stage: Current project status and development stage.

Remember:

**It can can carry a conversation upto about 4-5 follow-up question**
- The App is in its very early testing phase
- The tool fetches data only. No adding, updating, or deleting records.
- For security, the tool doesn't reveal the entire database structure. 
- Avoid large data requests. The tool is designed for specific queries.
- The tool understands synonyms and similar phrases for database columns. For example, "project power" refers to the 'Kws' column.

Ask your questions, and let the system help you with your data needs!

""")
######################################################
def log_to_sheet(username, prompt, sql):

  sh = gc.open("Streamlit Logs") 

  sh.sheet1.append_row([username, prompt, sql_query])
####################################################
# User login 
username = st.text_input("Enter username")
def convert_df(df):
   return df.to_csv(index=False).encode('utf-8')

# Initialize results  
if "results" not in st.session_state:
    st.session_state.results = []
# Execute SQL query
def execute_sql(sql):
    try:
        conn = sqlite3.connect(DB_FILE)
        df = pd.read_sql(sql, conn)
        return df
    except Exception as e:
        print(e)
        return "ERROR: "+ str(e)
st.sidebar.header('Interaction Log')


download_dict = {} 

# Display log function
def display_log():

  for result in st.session_state.results:
    
    if isinstance(result['df'], pd.DataFrame):
        
      # Generate unique name
      name = f"result_{len(download_dict)+1}"
      
      # Add dataframe to dictionary
      download_dict[name] = result['df']

      st.sidebar.code(result['sql'])
      st.sidebar.dataframe(result['df'])

      # Download button
      csv = convert_df(result['df'])
      st.sidebar.download_button(
        label=f"Download {name}",
        data=csv, 
        file_name=f"{name}.csv",
        mime='text/csv'
      )      
display_log()




if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    if message["role"] != "system":
        with st.chat_message(message["role"]):
          st.markdown(message["content"])




# append the context to messages list for better response
st.session_state.messages.append({"role": "system", "content": context})

# set the model (#gpt-3.5-turbo original)
if "openai_model" not in st.session_state:
    st.session_state["openai_model"] = "gpt-3.5-turbo-16k" 

if username:
  if username not in allowed_users:
     st.error('Invalid username')
  else:
     if prompt := st.chat_input("Enter your query here.."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user",avatar="💻"):
            st.markdown(prompt)

        with st.chat_message("assistant",avatar="🤖"):
            message_placeholder = st.empty()
            full_response = ""
            sql_query = ""
            try:
                for response in openai.ChatCompletion.create(
                    model=st.session_state["openai_model"],
                    messages=[
                        {"role": m["role"], "content": m["content"]}
                        for m in st.session_state.messages
                    ],
                    temperature = 0,
                    stream=True,
                ):   
                    
                    sql_query += response.choices[0].delta.get("content", "")

                            
                    full_response += response.choices[0].delta.get("content", "")
                    message_placeholder.markdown(full_response + "▌")
                message_placeholder.markdown(full_response)
                log_to_sheet(username, prompt, full_response)
                
                try:
                    results = execute_sql(sql_query)
                    
                    if results is not None or results.startswith("ERROR"):
                        df = results
                    # Append latest result
                        latest_result = {'sql': sql_query, 'df': df}
                        st.session_state.results.append(latest_result)

                        #st.code(sql_query, language='sql')
                        st.dataframe(df)
                        csv = convert_df(df)
                        st.download_button(
                        "Press to Download",
                        csv,
                        "file.csv",
                        "text/csv",
                        key='download-csv'
                        )
                        
                        st.sidebar.empty()
                        # Display latest in log
                        # Show latest result in sidebar
                        st.sidebar.code(latest_result['sql'], language='sql')
                        st.sidebar.dataframe(latest_result['df'])

                                    
                except Exception as e:
                      st.write("Please Enter a valid query related to the database or try to be more specific")

            except openai.error.InvalidRequestError as e:
                    # Check if error is due to context length
                    if "maximum context length" in str(e):
                        st.warning("Sorry, I have reached my maximum conversational context length. Let's start a new conversation! Do so by refreshing the Page")
                    else:
                        raise e
            except Exception as e:
                    st.error(f"Oops, an error occurred: {type(e).__name__}, args: {e.args}") 
                            
            
        st.session_state.messages.append({"role": "assistant", "content": full_response})



