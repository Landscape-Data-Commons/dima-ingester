# python ubuntu image
FROM python:3.8.12-slim-buster

# install linux dependencies
RUN apt-get update && \
    apt-get install postgresql \
            postgresql-contrib \
            unixodbc-dev \
            libpq-dev \
            g++ \
            default-jre \
            bash -y && \
            pip3 install --upgrade pip

# prepare environment
WORKDIR /usr/src
COPY src/requirements.txt .

# install python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

#copy application
COPY / .

# run application
CMD ["python3", "index.py"]
