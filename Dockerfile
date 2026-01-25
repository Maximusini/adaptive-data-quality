FROM apache/spark:3.5.0

USER root

RUN pip install --no-cache-dir pandas numpy scikit-learn joblib pyarrow