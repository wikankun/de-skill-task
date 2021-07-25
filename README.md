
## Prerequisite

- Python 3
- Docker & docker-compose

### Install requirements

```
virtualenv env
```

```
. env/bin/activate
```

```
pip install -r requirements.txt
```

## Task 1 Solution

How to run:
```
cd soal1
```

```
mkdir ./dags ./data ./logs ./plugins
```

```
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

```
docker-compose up airflow-init
```

```
docker-compose up
```

## Task 2 Solution

Asumsi:
- If the total content of the weight is less than the total content of the commodity, then the first weight is considered the weight of the entire item
- If the total contents of the weight are the same as the total contents of the commodity, then the weight will be mapped according to the commodity

How to run:
```
python soal2.py
```

## Task 3 Solution

How to run:
```
python soal3.py
```
