import uuid
import datetime
from airflow import DAG, settings
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator
)

dataproc_cluster_info = Variable.get("dataproc_cluster_info", deserialize_json=True)

# Данные инфраструктуры
YC_DP_FOLDER_ID = dataproc_cluster_info["YC_DP_FOLDER_ID"]
YC_DP_SSH_PUBLIC_KEY = dataproc_cluster_info["YC_DP_SSH_PUBLIC_KEY"]
YC_DP_SUBNET_ID = dataproc_cluster_info["YC_DP_SUBNET_ID"]
YC_DP_GROUP_ID = dataproc_cluster_info["YC_DP_GROUP_ID"]
YC_DP_SA_ID = dataproc_cluster_info["YC_DP_SA_ID"]
YC_DP_METASTORE_URI = dataproc_cluster_info["YC_DP_METASTORE_URI"]
YC_DP_AZ = 'ru-central1-a'
YC_SOURCE_BUCKET = dataproc_cluster_info["YC_SOURCE_BUCKET"]
YC_DATA_BUCKET = dataproc_cluster_info["YC_DATA_BUCKET"]
YC_DP_LOGS_BUCKET = dataproc_cluster_info["YC_LOG_BUCKET"]


# Создание подключения для Object Storage
session = settings.Session()
ycS3_connection = Connection(conn_id='yc-s3')
if not session.query(Connection).filter(Connection.conn_id == ycS3_connection.conn_id).first():
    session.add(ycS3_connection)
    session.commit()

# Создание подключения для сервисного аккаунта
ycSA_connection = Connection(conn_id='yc-airflow-auth-key')
if not session.query(Connection).filter(Connection.conn_id == ycSA_connection.conn_id).first():
    session.add(ycSA_connection)
    session.commit()

# Определение параметров DAG
dag = DAG(
        dag_id='hh_demographic_dag',
        schedule_interval='@daily',
        tags=['data-proc-and-airflow'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False)


# Настройки DAG
with dag:
    # 1 этап: создание кластера Yandex Data Processing
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        folder_id=YC_DP_FOLDER_ID,
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Временный кластер для выполнения PySpark-задания под оркестрацией Managed Service for Apache Airflow™',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_DP_LOGS_BUCKET,
        service_account_id=YC_DP_SA_ID,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        enable_ui_proxy=False,
        masternode_resource_preset='s3-c2-m8',
        masternode_disk_type='network-ssd',
        masternode_disk_size=50,
        computenode_resource_preset='s3-c2-m8',
        computenode_disk_type='network-ssd',
        computenode_disk_size=50,
        computenode_count=1,
        computenode_max_hosts_count=3,  # Количество подкластеров для обработки данных будет автоматически масштабироваться в случае большой нагрузки.
        services=['YARN', 'SPARK'],     # Создаётся легковесный кластер.
        datanode_count=0,               # Без подкластеров для хранения данных.
        properties={                    # С указанием на удалённый кластер Metastore.
            'spark:spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',
            'spark:spark.sql.hive.metastore.sharedPrefixes': 'com.amazonaws,ru.yandex.cloud',
            'spark:spark.sql.warehouse.dir': f's3a://{YC_DATA_BUCKET}/warehouse',
            'spark:spark.sql.catalogImplementation': 'hive',
            'spark:spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
            'spark:spark.jars': f's3a://{YC_SOURCE_BUCKET}/jars/delta-core_2.12-2.3.0.jar,s3a://{YC_SOURCE_BUCKET}/jars/delta-storage-2.3.0.jar',
            'spark:spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
            'spark:spark.sql.repl.eagerEval.enabled': 'true'
        },
        security_group_ids=[YC_DP_GROUP_ID],
        connection_id=ycSA_connection.conn_id,
        dag=dag
    )

    # 2 этап: запуск задания PySpark
    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task-transactional_data',
        main_python_file_uri=f's3a://{YC_SOURCE_BUCKET}/data/4.5-hh_demographic.py',
        connection_id=ycSA_connection.conn_id,
        dag=dag
    )

    # 3 этап: удаление кластера Yandex Data Processing
    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )

    # Формирование DAG из указанных выше этапов
    create_spark_cluster >> poke_spark_processing >> delete_spark_cluster
