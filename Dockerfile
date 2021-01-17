FROM python:3.8-buster

WORKDIR .

COPY . .

RUN pip install -r requirements.txt

CMD ["python3", "frege_extractor/main.py"]
