FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir streamlit kafka-python pandas plotly

COPY app.py .

CMD ["streamlit", "run", "app.py"]