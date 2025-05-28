FROM python:3.9-slim

WORKDIR /app

# Install required libs
RUN pip install flask flask-cors kafka-python Werkzeug

COPY backend.py .

CMD ["python", "backend.py"]
