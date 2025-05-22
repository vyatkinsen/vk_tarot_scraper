FROM python:3.12-slim

WORKDIR /app

COPY vk_requirements.txt .
RUN pip install --no-cache-dir -r vk_requirements.txt

COPY . .

CMD ["python", "vk_scraper.py"]