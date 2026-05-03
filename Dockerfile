FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY core/ core/
COPY targets/ targets/
COPY project.env.example .

RUN mkdir -p cache temp "Google Merchant - country feed updates" "Meta catalog - country feed updates"

ENV TZ=UTC

ENTRYPOINT ["python", "main.py"]
CMD ["smart", "--target", "google"]
