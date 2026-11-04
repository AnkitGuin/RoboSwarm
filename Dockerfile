FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY pyproject.toml ./
COPY src/ ./src/
RUN pip install --no-cache-dir .
COPY . .
RUN mkdir -p /app/data
CMD ["uvicorn", "robostream.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
