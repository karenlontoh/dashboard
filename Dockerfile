# Gunakan base image Python resmi
FROM python:3.12-slim

# Buat direktori kerja
WORKDIR /app

# Copy requirements.txt dan install dependensi
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy semua source code
COPY . .

# Jalankan aplikasi FastAPI
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
