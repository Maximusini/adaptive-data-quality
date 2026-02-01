import streamlit as st
import pandas as pd
import json
import os
import plotly.express as px
from kafka import KafkaConsumer

st.set_page_config(page_title='Data Quality Dashboard', layout='wide')
st.title('Adaptive Data Quality Dashboard')

KAFKA_BROKER = os.getenv('KAFKA_BROKER_URL', 'localhost:9092')

placeholder = st.empty()

consumer = KafkaConsumer(
    'clean_events', 'quarantined_events',
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest'
)

last_events = []
for message in consumer:
    value = message.value
    value['topic'] = message.topic
    
    last_events.append(value)
    
    if len(last_events) > 500:
        last_events.pop(0)
    
    df = pd.DataFrame(last_events)
    with placeholder.container():
        k1, k2, k3 = st.columns(3)
        total_count = len(df)
        anomaly_count = len(df[df['topic'] == 'quarantined_events'])
        
        k1.metric(label='Размер буфера в реальном времени', value=total_count)     
        k2.metric(label='Обнаружено аномалий', value=anomaly_count, delta_color='inverse')
        
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
            fig = px.scatter(
                df, 
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
            st.plotly_chart(fig, use_container_width=True)
        