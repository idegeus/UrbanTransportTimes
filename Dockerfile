FROM python:3.11

COPY requirements.txt .

RUN apt-get update && apt-get install -y libgdal-dev libproj-dev gdal-bin

RUN pip3 install -r requirements.txt

RUN pip3 install wheel python-dotenv

CMD ["python", "/2-util/main.py"]