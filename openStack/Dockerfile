FROM python:3.8-slim-buster
RUN mkdir -p  ~/openStack-server
COPY . ~/openStack-server
WORKDIR ~/openStack-server
RUN pip3 install -r requirements.txt
CMD python Main.py
