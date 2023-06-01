FROM python:3.11

COPY . .

RUN pip3 install -r requirements.txt

RUN pip3 install wheel python-dotenv