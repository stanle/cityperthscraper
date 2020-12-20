FROM joyzoursky/python-chromedriver:3.6-selenium

COPY requirements.txt .

RUN apt update -y \
    && apt install -y default-jre

RUN pip install -r requirements.txt

RUN useradd morph

USER morph