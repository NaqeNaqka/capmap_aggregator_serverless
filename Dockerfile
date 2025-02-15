FROM python:3.11.4 

RUN mkdir /app

COPY ./src /app

COPY requirements.txt /app

WORKDIR /app

RUN pip install -r requirements.txt

CMD ["python", "main.py"]