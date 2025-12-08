FROM python:3.10-slim

WORKDIR /app

COPY notification_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY notification_service/alert_notifier.py .
COPY data/alert_routes.db .

CMD ["python", "alert_notifier.py"]
