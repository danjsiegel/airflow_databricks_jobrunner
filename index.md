# airflow_databricks_jobrunner
Currently, there is a similar official [airflow operator](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators.html). While the operator does work, there is virtually no feedback, it essentially just runs and then returns back a failure if there is a problem. 

To deal with this, I put together this small bit of code to mimick a subset of the functionalty, basically to submit a job, tell you what is happening while the code is executing and to tell you if the job completed succesffully or raise an error. In order to configure the job, you will need to create a connection called `DatabricksJobRunner.` For the connection, you should put your databricks URL to your workspace. In the extra space, you will need to supply a token `{"token":"your_token"}.` 

The operator essentially takes a job id, submits a run to the cluster, and the polls every 10 seconds for the status and prints the status. Once the job is done, based on the status of the run, it raises an error or completes gracefully. 

```python
from databricks_jobrunner import DatabricksJobRunner`

with dag:
    databricks_job = DatabricksJobRunner(task_id='Databricks Job', dag=dag, jobid='id of your job', notebook_params:'params_you_want_to_pass')
```
