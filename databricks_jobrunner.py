from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
import json
from time import sleep
import requests

db_conn = BaseHook.get_connection('DatabricksJobRunner')

class DatabricksJobRunner(BaseOperator):

    def __init__(
            self,
            jobid,
            notbook_params,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.databricks_url = db_conn.host
        self.jobid = jobid
        self.notbook_params = notbook_params
        self.databricks_token = db_conn.extra_dejson['token']


    def execute(self, context):
        jobs_url = self.databricks_url + 'api/2.0/jobs/run-now'
        payload = json.dumps({"job_id":self.job_id, "notbook_params":self.notbook_params})
        header = {'Authorization':'Bearer '+self.databricks_token}
        response = requests.request('POST', jobs_url, headers=header, data=payload)
        print(response.text)
        run_id = response.json()['run_id']
        status = None
        while (status != 'TERMINATED') and (status != 'SKIPPED') and (status != 'INTERNAL_ERROR'):
            url = self.databricks_url + 'api/2.0/jobs/runs/get-output?run_id='+run_id
            response = requests.request('GET', url, headers=header)
            status = response.json()['metadata']['state']['life_cycle_state']
            print(status)
            sleep(10)
        print(response.json())
        try:
            if response.json()['metadata']['state']['result_state']!='SUCCESS':
                raise AirflowException('Databricks Job Failed')
        except:
            raise AirflowException('Databricks Job Failed')

        
