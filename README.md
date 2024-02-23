LSDA
================================
LSDA is a transpiler that aims to optimize data science pipelines in python. We accomplish this by converting preprocessing operations to SQL and executing them in database systems during runtime. Currently, we are supporting only pandas operations.
We build on the work of the [mlinspect framework](https://github.com/stefan-grafberger/mlinspect) and its extension [mlinspect-SQL](https://gitlab.db.in.tum.de/ge69xap/mlinspect) which can be used to inspect ML Pipelines for debugging purposes. 
We use [XDB](https://dl.acm.org/doi/10.14778/3611540.3611625) to process cross-database queries. Since it is not publicly available, you would have to set up a PostgreSQL database system and change the query compilation step accordingly.


## Run mlinspect locally

Prerequisite: Python 3.8

1. Clone this repository
2. Set up the environment

	`cd mlinspect` <br>
	`python -m venv venv` <br>
	`source venv/bin/activate` <br>

3. If you want to use the visualisation functions we provide, install graphviz which can not be installed via pip

    `Linux: ` `apt-get install graphviz` <br>
    `MAC OS: ` `brew install graphviz` <br>
	
4. Install pip dependencies 

    `pip install -e .[dev]` <br>

5. To ensure everything works, you can run the tests (without graphviz, the visualisation test will fail)

    `python setup.py test` <br>
    

## How to use the SQL backend
We prepared one example, [TPC-H Query 3](example_to_sql/MeinBeispielMitDerNeuenDB-Query3.ipynb) to showcase LSDA's potential.

In order to run the example, you need a PostgreSQL database system running in the background and provide its connection details.

	create user luca;
	alter role luca with password 'password';
	grant pg_read_server_files to luca;
	create database healthcare_benchmark;
	grant all privileges on database healthcare_benchmark to luca;


## How to use LSDA

```python
from mlinspect.to_sql.dbms_connectors.postgresql_connector import PostgresqlConnector
from mlinspect import PipelineInspector

dbms_connector = PostgresqlConnector(...)

IPYNB_PATH = ...

inspector_result = PipelineInspector\
        .on_pipeline_from_ipynb_file(IPYNB_PATH)\
        .execute_in_sql(dbms_connector=dbms_connector, mode="VIEW", materialize=True)

extracted_dag = inspector_result.dag
dag_node_to_inspection_results = inspector_result.dag_node_to_inspection_results
check_to_check_results = inspector_result.check_to_check_results
```
