FROM python:3.12

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

COPY requirements.txt /app

RUN pip install --no-cache-dir -r requirements.txt


CMD ["python", "main.py"]
