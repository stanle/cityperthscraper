FROM openaustralia/buildstep:early_release

COPY requirements.txt .

RUN apt update -y \
    && apt-get install -y python3-pip \
    && apt-get install -y default-jre

RUN pip3 install -r requirements.txt

RUN useradd morph

USER morph