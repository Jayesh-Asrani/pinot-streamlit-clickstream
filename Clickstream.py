import pandas as pd
import streamlit as st
from pinotdb import connect
from datetime import datetime
import plotly.express as px
import time
from datetime import date, timedelta

st.set_page_config(layout="wide")

conn = connect(host='broker.pinot.flrg1s.s7e.startree.cloud', port=443, path='/query/sql', scheme='https',
               username=st.secrets["username"], password=st.secrets["password"])



def product_funnel():
    st.header("Product Funnel")

    Metric_1 = 'pix_settings_list_item__clicked'

    query = """
       SET timeoutMs=1000000;
       SET useMultistageEngine=false;

select FUNNEL_COUNT(
    STEPS(
      metric_name = 'app__opened',
      metric_name = 'bopp__screen_viewed',
      metric_name = 'transfer_out_checkout_screen__viewed',
      metric_name = 'location__requested'
    ),
    CORRELATE_BY(customer_id)
  ) AS counts
from clickstream
           """

    curs = conn.cursor()
    curs.execute(query)
    df_funnel = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    data = dict(
        number=[int(df_funnel['counts'][0][0]), int(df_funnel['counts'][0][1]), int(df_funnel['counts'][0][2]),
                int(df_funnel['counts'][0][3])],
        stage=["Step 1", "Step 2", "Step 3", "Step 4"])
    fig = px.funnel(data, x='number', y='stage')
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Raw Data")

    st.dataframe(df_funnel, use_container_width=True, height=50)


def customer_lookup():
    st.title("Customer Lookup")

    # Input customer ID
    customer_id = st.text_input("Enter Customer ID", value='cust_5de0864c-b8ac-42b5-002116')

    query = """
        SELECT
            customer_id , event_type , platform, event_timestamp , metric_name , package , country , current_screen ,device_model ,device_manufacturer ,event_properties
        FROM
            "clickstream"
        WHERE
            "customer_id" = %(customer_id)s
        ORDER BY event_timestamp DESC
        LIMIT 100
            """

    curs = conn.cursor()
    curs.execute(query, {"customer_id": customer_id})
    df_clickstream = pd.DataFrame(curs, columns=[item[0] for item in curs.description])
    # Display the filtered data as a table
    st.dataframe(df_clickstream, use_container_width=True, height=600)


def event_ranking():
    st.title("Event Ranking")

    def select_platform_callback():
        st.session_state['selected_platform'] = st.session_state.select_platform

    def select_metric_callback():
        st.session_state['selected_metric'] = st.session_state.select_metric

    def select_date_callback():
        st.session_state['selected_date'] = st.session_state.select_date

    selected_platform = st.selectbox("Select Platform", ('Android', 'iOS'), on_change=select_platform_callback,
                                     key='select_platform')

    query = """
    SELECT
     distinct(metric_name) as metric_name
     from clickstream
	 order by metric_name
    LIMIT 1000
                    """

    curs = conn.cursor()
    curs.execute(query)
    df_metric_name = pd.DataFrame(curs, columns=[item[0] for item in curs.description])
    selected_metric = st.selectbox("Select Metric", df_metric_name['metric_name'], index=2,
                                   on_change=select_metric_callback, key='select_metric')

    selected_date = st.date_input("Select Event Start Date", value=date(2024, 4, 1), on_change=select_date_callback,
                                  key='select_date')

    to_date = str(selected_date + timedelta(days=30))
    selected_date = str(selected_date)

    query = """
SELECT
  package
  ,COUNT(1) as total
FROM
  "clickstream"
WHERE
  "platform" = %(selected_platform)s
  and "metric_name" = %(selected_metric)s
  and event_timestamp BETWEEN
    FROMDATETIME(%(selected_date)s, 'yyyy-MM-dd')
    AND FROMDATETIME(%(to_date)s, 'yyyy-MM-dd')
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10
            """

    curs = conn.cursor()
    curs.execute(query, {"selected_platform": selected_platform, "selected_metric": selected_metric,
                         "selected_date": selected_date, "to_date": to_date})
    df_event_ranking = pd.DataFrame(curs, columns=[item[0] for item in curs.description])
    # Display the filtered data as a table

    st.subheader("Total Per Package")

    st.bar_chart(df_event_ranking, use_container_width=True, x='package', y='total', height=700)

    st.subheader("Raw Data")

    st.dataframe(df_event_ranking, use_container_width=True, height=400)


def session_analysis():
    st.title("Session Analysis")

    def select_date_callback():
        st.session_state['selected_date'] = st.session_state.select_date

    def select_metric_callback():
        st.session_state['selected_metric'] = st.session_state.select_metric

    query = """
        SELECT
         distinct(metric_name) as metric_name
         from clickstream
    	 order by metric_name
        LIMIT 1000
                        """

    curs = conn.cursor()
    curs.execute(query)
    df_metric_name = pd.DataFrame(curs, columns=[item[0] for item in curs.description])
    selected_metric = st.selectbox("Select Metric", df_metric_name['metric_name'], index=0,
                                   on_change=select_metric_callback, key='select_metric')

    selected_current_screen = st.text_input("Enter Current Screen", value='onboarding')

    selected_date = st.date_input("Select Event Start Date", value=date(2024, 4, 1), on_change=select_date_callback,
                                  key='select_date')

    to_date = str(selected_date + timedelta(days=300))
    to_date = int((datetime.strptime(str(to_date), "%Y-%m-%d")).timestamp()) * 1000
    selected_date = int((datetime.strptime(str(selected_date), "%Y-%m-%d")).timestamp()) * 1000

    query = """
    SET useMultistageEngine=true;
     SET timeoutMs=10000000;
    SELECT
      event_timestamp_day
      ,COUNT(DISTINCT session_id) as total_sessions
      ,AVG(session_length) as total_time_spent
    FROM (
      SELECT
          event_timestamp_day
          ,session_id
          ,SUB(
            MAX(event_timestamp)
            ,MIN(event_timestamp)
          ) as session_length
      FROM
        "clickstream"
      WHERE
        metric_name = %(selected_metric)s
        AND current_screen = %(selected_current_screen)s
   and event_timestamp BETWEEN %(selected_date)s AND %(to_date)s
  GROUP BY 1,2
) as sessions
GROUP BY 1 order by 1 """

    curs = conn.cursor()
    curs.execute(query, {"selected_date": selected_date, "to_date": to_date,
                         "selected_current_screen": selected_current_screen, "selected_metric": selected_metric},
                 queryOptions="timeoutMs=20000")
    df_session_analysis = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    df_session_analysis['Event_Day'] = pd.to_datetime(df_session_analysis['event_timestamp_day'], unit='ms')

    st.subheader("Total Sessions Per Day")

    fig1 = px.line(df_session_analysis, x='Event_Day', y='total_sessions', markers=True)
    fig1.update_xaxes(title='Date')
    fig1.update_yaxes(title='Total Sessions')

    # Display the filtered data as a table
    st.plotly_chart(fig1, use_container_width=True)

    st.subheader("Total Time Spent Per Day")

    fig2 = px.line(df_session_analysis, x='Event_Day', y='total_time_spent', markers=True)
    fig2.update_xaxes(title='Date')
    fig2.update_yaxes(title='Total Time')

    # Display the filtered data as a table
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Raw Data")

    st.dataframe(df_session_analysis, use_container_width=True, height=250)


def event_segmentation_analysis():
    st.title("Event Segmentation Analysis")

    query = """
    SET timeoutMs=1000000;
SET useMultistageEngine=true;

WITH parsed_events_per_day AS (
  SELECT event_timestamp_day as event_date,
    customer_id,
    metric_name,
    flow AS evp_flow,
    current_screen AS evp_current_screen,
    button AS evp_button
  FROM "clickstream"
  WHERE metric_name IN ('app__opened', 'bopp__screen_viewed')
    and event_timestamp >= 1680307200000
    and event_timestamp <= 1712275200000
),
user_groups AS (
  SELECT event_date,
    customer_id,
(metric_name = 'app__opened') as group_a,
(metric_name = 'bopp__screen_viewed') as group_b,
    SUM(
      CASE
        WHEN metric_name = 'app__opened' THEN 1
        ELSE 0
      END
    ) as user_performed_bopp_click
  FROM parsed_events_per_day
  GROUP BY 1,
    2,
    3,
    4
),
users_that_performed_enroll as (
  SELECT customer_id,
    SUM(user_performed_bopp_click) total_enrolls
  FROM user_groups
  GROUP BY 1
  HAVING total_enrolls > 0
)
SELECT usg.event_date,
  COUNT(
    DISTINCT CASE
      WHEN group_a THEN usg.customer_id
      ELSE null
    END
  ) as total_unique_users_group_a,
  COUNT(
    DISTINCT CASE
      WHEN group_b THEN usg.customer_id
      ELSE null
    END
  ) as total_unique_users_group_b
FROM user_groups usg
  JOIN users_that_performed_enroll us_enroll ON (usg.customer_id = us_enroll.customer_id)
GROUP BY 1
ORDER BY 1"""

    curs = conn.cursor()
    curs.execute(query)
    df_event_segmentation_analysis = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    df_event_segmentation_analysis['Event_Day'] = pd.to_datetime(df_event_segmentation_analysis['event_date'],
                                                                 unit='ms')

    df_event_segmentation_analysis_melt = pd.melt(df_event_segmentation_analysis, id_vars=['Event_Day'],
                                                  value_vars=['total_unique_users_group_a',
                                                              'total_unique_users_group_b'])

    fig1 = px.area(df_event_segmentation_analysis_melt, x='Event_Day', y='value', color='variable',
                   color_discrete_sequence=['red', 'blue'], markers=True)

    fig1.update_xaxes(title='Date')
    fig1.update_yaxes(title='Unique Users')

    # Display the filtered data as a table
    st.plotly_chart(fig1, use_container_width=True)

    # Display the filtered data as a table

    st.subheader("Raw Data")

    st.dataframe(df_event_segmentation_analysis, use_container_width=True, height=200)


PAGES = {
    "Customer Lookup": customer_lookup,
    "Event Ranking": event_ranking,
    "Product Funnel": product_funnel,
    "Session Analysis": session_analysis,
    "Event Segmentation Analysis": event_segmentation_analysis

}

st.sidebar.title('Clickstream Analytics')

now = datetime.now()
dt_string = now.strftime("%d %B %Y %H:%M:%S")
st.sidebar.write(f"Last update: {dt_string}")

if not "sleep_time" in st.session_state:
    st.session_state.sleep_time = 5

if not "auto_refresh" in st.session_state:
    st.session_state.auto_refresh = False

auto_refresh = st.sidebar.checkbox('Auto Refresh?', st.session_state.auto_refresh)

if auto_refresh:
    number = st.sidebar.number_input('Refresh rate in seconds', value=st.session_state.sleep_time)
    st.session_state.sleep_time = number

selection = st.sidebar.radio("Go to", list(PAGES.keys()))
page = PAGES[selection]
page()
st.markdown("""
<style>
section.main[tabindex='0'] div[data-testid='stVerticalBlock'] div.element-container:nth-child(1):has(> iframe) {
    display: none;
}

</style>
""", unsafe_allow_html=True)

if auto_refresh:
    time.sleep(number)
    st.rerun(scope="app")
