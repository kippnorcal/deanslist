# deanslist_connector
ETL job to bring Deanslist data into the data warehouse

## Dependencies:

* Python3.7
* [Pipenv](https://pipenv.readthedocs.io/en/latest/)
* [Docker](https://www.docker.com/)

## Getting Started

### Setup Environment

1. Clone this repo

```
$ git clone https://github.com/kipp-bayarea/deanslist_connector.git
```

2. Install Pipenv

```
$ pip install pipenv
$ pipenv install
```

3. Install Docker

* **Mac**: [https://docs.docker.com/docker-for-mac/install/](https://docs.docker.com/docker-for-mac/install/)
* **Linux**: [https://docs.docker.com/install/linux/docker-ce/debian/](https://docs.docker.com/install/linux/docker-ce/debian/)
* **Windows**: [https://docs.docker.com/docker-for-windows/install/](https://docs.docker.com/docker-for-windows/install/)

4. Create .env file with project secrets

```
DB_SERVER=
DB=
DB_USER=
DB_PWD=
DB_SCHEMA=

GMAIL_USER=
GMAIL_PWD=
SLACK_EMAIL=

DOMAIN=deanslist domain
```

5. Create datamap.py file with API keys

```
api_key_map = {
    "BayviewMS":"XXXXXXXXXXXXXXXXXXX",
    "SFBay":"XXXXXXXXXXXXXXXXXXX",
```


5. Build Docker Image

```
$ docker build -t deanslist .
```

### Running the Job

```
$ docker run --rm -it deanslist
```
