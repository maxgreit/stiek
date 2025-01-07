from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Venv activerings functie
def venv_command(script_path):
    return f"source /home/greit/klanten/stiek/stiek_venv/bin/activate && python3 {script_path}"


# Definieer de standaardinstellingen voor de DAG
default_args = {
    'owner': 'Max - Greit',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['max@greit.nl'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definieer de DAG
dag = DAG(
    'stiek_multi_daily_dag_v01',
    default_args=default_args,
    description='Data update',
    schedule_interval="0 8-20/4 * * *",
    catchup=False,
)

# Definieer de taken van de DAG
uren_rapportage_taak = BashOperator(
        task_id='urenrapportage',
        bash_command=venv_command("/home/greit/klanten/stiek/e-uur/urenrapportage/uren_main.py"),
        dag=dag,
    )

actieve_plaatsing_taak = BashOperator(
        task_id='actieve_plaatsing',
        bash_command=venv_command("/home/greit/klanten/stiek/e-uur/plaatsing/actief_plaatsing_main.py"),
        dag=dag,
    )

inactieve_plaatsing_taak = BashOperator(
        task_id='inactieve_plaatsing',
        bash_command=venv_command("/home/greit/klanten/stiek/e-uur/plaatsing/inactief_plaatsing_main.py"),
        dag=dag,
    )

ontbrekende_uren_taak = BashOperator(
        task_id='ontbrekende_uren',
        bash_command=venv_command("/home/greit/klanten/stiek/e-uur/ontbrekende_uren/ontbrekend_main.py"),
        dag=dag,
    )

looncomponenten_taak = BashOperator(
        task_id='looncomponenten',
        bash_command=venv_command("/home/greit/klanten/stiek/e-uur/looncomponenten/actief_main.py"),
        dag=dag,
    )

start_parallel_tasks = EmptyOperator(
        task_id='start_parallel_tasks',
        dag=dag,
    )

end_parallel_tasks = EmptyOperator(
        task_id='end_parallel_tasks',
        dag=dag,
    )

# Taak structuur
start_parallel_tasks >> [
    uren_rapportage_taak,
    actieve_plaatsing_taak,
    inactieve_plaatsing_taak,
    ontbrekende_uren_taak,
    looncomponenten_taak,
] >> end_parallel_tasks
                          