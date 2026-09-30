# Optional: containerised deployment for any host that runs Docker (Render, Fly.io, Railway,
# Hugging Face Spaces "Docker" SDK, your own server). Streamlit Community Cloud does NOT need
# this file -- it builds directly from requirements.txt.
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
