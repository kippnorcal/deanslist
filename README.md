# deanslist
ETL job to bring DeansList data into the data warehouse

## Dependencies:

* Python3.8
* [Pipenv](https://pipenv.readthedocs.io/en/latest/)
* [Docker](https://www.docker.com/)

## Getting Started

### Setup Environment

1. Clone this repo

```
$ git clone https://github.com/kippnorcal/deanslist.git
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

# Mailgun & email notification variables
MG_API_KEY=
MG_API_URL=
MG_DOMAIN=
SENDER_EMAIL=
RECIPIENT_EMAIL=

DOMAIN=deanslist domain
```

5. Create DeansList_APIConnection database table.
Refer to sql/DeansList_APIConnection.sql.


6. Build Docker Image

```
$ docker build -t deanslist .
```

### Running the Job

```
$ docker run --rm -it deanslist
```

### Runtime parameters
Run the job for only certain schools (one or many). School names must match APIKeys table.

```
$ docker run --rm -it deanslist --schools "KIPP Bayview Academy" "KIPP Bridge Academy (Upper)"
```

By default, we get behaviors data for the current month. 
To backfill ONLY behavior data for a specified date range (ie. no other endpoints), 
use the following command. Note: for best performance, limit the date range to 1 month.

```
$ docker run --rm -it deanslist --behavior-backfill "2019-12-01" "2019-12-31"
```

## Maintenance

* If a new school starts using DeansList, then the table custom.DeansList_APIConnection needs to be updated. Set Active=True to pull records for the newly added school.
* The connector can be turned off when school is out of session for the summer.
* No other annual maintenance is required.
