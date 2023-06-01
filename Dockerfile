FROM python:3.11

COPY requirements.txt .

RUN pip3 install -r requirements.txt

RUN pip3 install wheel python-dotenv

CMD ["python", "/2-util/main.py"]