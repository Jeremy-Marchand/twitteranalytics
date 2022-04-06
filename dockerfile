FROM python:3.8.12-bullseye
COPY api /api
COPY requirements.txt /requirements.txt
COPY gcp_key /gcp_key
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN export GOOGLE_APPLICATION_CREDENTIALS="gcp_key/wagon-bootcamp-802-bd537eeb2bd3.json"
CMD uvicorn api.fast:app --host 0.0.0.0 --port $PORT
