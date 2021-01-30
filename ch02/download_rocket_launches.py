import json
import pathlib
import requests

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
  dag_id='download_rocket_launches',
  start_date=airflow.utils.dates.days_ago(14),
  schedule_interval='@daily'
)

download_launches = BashOperator(
  task_id='download_launches',
  bash_command="curl -o /tmp/launches.json https://ll.thespacedevs.com/2.0.0/launch/upcoming/\?format\=json\&limit\=5",
  dag=dag
)

def _get_pictures():
  pathlib.Path('/tmp/images').mkdir(parents=True, exist_ok=True)

  with open('/tmp/launches.json') as f:
    launches = json.load(f)
    image_urls = [f['image'] for f in launches['results']]
    for image_url in image_urls:
      response = requests.get(image_url)
      image_filename = image_url.split('/')[-1]
      target_file = f'/tmp/images/{image_filename}'
      with open(target_file, 'wb') as f:
        f.write(response.content)
      print(f'Downloaded {image_url} to {target_file}')

get_pictures = PythonOperator(
  task_id='get_pictures',
  python_callable=_get_pictures,
  dag=dag
)

notify = BashOperator(
  task_id='notify',
  bash_command='echo "There are now $(ls /tmp/images | wc -l) images"',
  dag=dag
)

download_launches >> get_pictures >> notify