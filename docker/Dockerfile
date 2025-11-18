FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        default-libmysqlclient-dev \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create pb_files directories that will be mounted from host
RUN mkdir -p /app/pb_files /app/pb_files_depreciated && \
    chmod 755 /app/pb_files /app/pb_files_depreciated

EXPOSE 5050

RUN chmod +x scripts/entrypoint.sh
CMD ["scripts/entrypoint.sh"]
