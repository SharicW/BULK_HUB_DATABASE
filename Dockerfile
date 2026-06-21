FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium chromium-driver \
    ca-certificates fonts-liberation \
    libnss3 libnspr4 \
    libatk-bridge2.0-0 libatk1.0-0 \
    libgtk-3-0 \
    libgbm1 libdrm2 \
    libasound2 \
    libcups2 \
    libx11-6 libx11-xcb1 \
    libxcomposite1 libxdamage1 libxext6 libxfixes3 \
    libxrandr2 libxss1 libxi6 libxtst6 \
    libpango-1.0-0 libpangocairo-1.0-0 libcairo2 \
    libu2f-udev \
    && rm -rf /var/lib/apt/lists/*

RUN chromium --version && chromedriver --version

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
