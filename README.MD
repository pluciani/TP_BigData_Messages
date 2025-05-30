# TP Big Data Messages


## Lancement

### Démarrer le docker
```bash
docker compose up
```

### Correction nécessaire au fonctionnement de Hive
```bash
docker compose restart hive-metastore

docker exec -it hive-metastore bash

schematool -dbType postgres -initSchema
```

## Copier les datasets dans hdfs

### Accéder au conteneur namenode
```bash
docker exec -it namenode bash
```

### Créer le dossier /data et copier les fichiers csv
```bash
hdfs dfs -mkdir /data
hdfs dfs -put ./myhadoop/train.csv /data/
hdfs dfs -put ./myhadoop/labeled_data.csv /data/
```

## Paramètrage de Hive
### Accéder au conteneur hive-server
```bash
docker exec -it hive-server bash
```

### Création de la db
```bash
hive
create database bigdata_db
```

## Traitement des datas

### Accéder au conteneur spark-master
```bash
docker exec -it spark-master bash
```
### Corrections nécessaires pour le spark-master
```bash
apk update
apk upgrade
apk add --no-cache python3 py3-pip openjdk11-jdk bash build-base libffi-dev openssl-dev tzdata py3-numpy
```
#### Packages nécessaires au lancement de certains scripts
```bash
pip3 install pyspark kafka kafka-python pyhive thrift thrift_sasl numpy 
```

### Script de traitement des données
```bash
python3 ./app/data_process.py
```

### Script d'entraînement du modèle
```bash
python3 ./app/model.py
```

## Lancement des producer et consumer Kafka

### Producer Kafka
```bash
python3 ./app/tweet-producer.py
```

### Consumer Kafka
```bash
python3 ./app/tweet-consumer.py
```

## Accès à superset

- http://localhost:8089/superset/

## Lien vers notre présentation 

- https://www.canva.com/design/DAGo6nQZYbY/6D01JSJvFnWo-lrPC3cn0A/edit?utm_content=DAGo6nQZYbY&utm_campaign=designshare&utm_medium=link2&utm_source=sharebutton


## Auteurs

- Simon GUILLET
- Joseph FREMONT
- Paul LUCIANI
