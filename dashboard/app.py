import streamlit as st
import pandas as pd
import json
import os
import time
import plotly.express as px
from kafka import KafkaConsumer

st.set_page_config(page_title='Data Quality Dashboard', layout='wide')
st.title('Adaptive Data Quality Dashboard')

KAFKA_BROKER = os.getenv('KAFKA_BROKER_URL', 'localhost:9092')
UPDATE_INTERVAL_SEC = 1.0
MAX_BUFFER_SIZE = 500

@st.cache_resource
def create_consumer():
    return KafkaConsumer(
        'clean_events', 'quarantined_events',
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='dashboard_viewer'
    )

consumer = create_consumer()

if 'events_buffer' not in st.session_state:
    st.session_state['events_buffer'] = []

msg_pack = consumer.poll(timeout_ms=200)
new_messages = []
for tp, messages in msg_pack.items():
    for message in messages:
        value = message.value
        value['topic'] = message.topic
        new_messages.append(value)
    
if new_messages:
    st.session_state['events_buffer'].extend(new_messages)
    if len(st.session_state['events_buffer']) > MAX_BUFFER_SIZE:
        st.session_state['events_buffer'] = st.session_state['events_buffer'][-MAX_BUFFER_SIZE:]
    
df = pd.DataFrame(st.session_state['events_buffer'])
        
if not df.empty:
    required_columns = ['temperature', 'timestamp', 'topic']
    for col in required_columns:
        if col not in df.columns:
            df[col] = None 
    
    k1, k2, k3 = st.columns(3)   
    total_count = len(df)
    anomaly_count = len(df[df['topic'] == 'quarantined_events'])
    k1.metric(label='Размер буфера в реальном времени', value=total_count)     
    k2.metric(label='Обнаружено аномалий', value=anomaly_count, delta_color='inverse')
    k3.metric(label='Последнее обновление', value=time.strftime('%H:%M:%S'))
            
    df['timestamp'] = pd.to_datetime(df['timestamp'])
            
    chart_df = df.dropna(subset=['temperature', 'timestamp'])
    if not chart_df.empty:
        fig = px.scatter(
            chart_df, 
            x='timestamp',
            y='temperature',
            color='topic',
            color_discrete_map={
                'clean_events': 'green',
                'quarantined_events': 'red'
            },
            title='Поток данных с датчиков в режиме реального времени',
            labels={'timestamp': 'Время', 'temperature': 'Температура (°C)'}
        )
            
        fig.update_layout(transition_duration=0)
            
        st.plotly_chart(fig, width='stretch')
                
    else:
        st.info('Ожидание валидных данных для построения графика...')

time.sleep(1)
st.rerun()