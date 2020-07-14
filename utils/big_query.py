import re

from google.cloud import bigquery, storage
from google.oauth2 import service_account
from google.auth import compute_engine


class Credentials():
    """Holds credentials and client objects for Big Query and Google CLoud
       storage. Credentials to project mapping is 1-1.

    Args:
        cred_path (string): Local credentials json path
        project_name (string): Google Cloud project name

    Attributes:
        creds (object): oauth2.service_account object
        bq_client (object): bigquery.Client object
        storage_client (object): storage.Client object
        cred_path
        project_name
    """

    def __init__(self, cred_path, project_name):
        self.cred_path = cred_path
        self.project_name = project_name
        self.credentials = self._get_big_query_credentials()
        self.bq_client = self._get_bigquery_client()
        self.storage_client = self._get_storage_client()

    def __str__(self):
        return (f"Cred Path: {self.cred_path}\n"
                f"Project: {self.project_name}")

    def _get_big_query_credentials(self):
        return service_account.Credentials \
                              .from_service_account_file(self.cred_path)

    def _get_bigquery_client(self):
        return bigquery.Client(credentials=self.credentials,
                               project=self.project_name)

    def _get_storage_client(self):
        return storage.Client(credentials=self.credentials,
                              project=self.project_name)


class Sql():
    """Represents a single sql query. Currently the only method allows
       for the execution of that query on a big query project specified
       by the Client() object passed to that method.

    Args:
        query (string): The sql scripy/ query to be run.
        name (string)

    Attributes:
        query
        name

    """
    def __init__(self, query, name):
        self.query = query
        self.name = name

    def __str__(self):
        return f"Sql Object: {self.name}"

    def __repr__(self):
        return f"Sql Object: {self.name}"

    def format_query(self, formatters):
        self.query = self.query.format(*formatters)

    def execute_on_big_query(self, client, config=None, return_result=False):
        """Executes the query attribute on Big Query via the
           google.cloud.bigquery API.

        Args:
            client (object): A bigquery.Client() object.
            config (dict): A dictionary comprised of config arguments if the
                           result of the query is to be stored:
                {'dataset': 'string'
                 'table_name': 'string'
                 'write_method': 'WRITE_APPEND' or 'WRITE_TRUNCATE'
                 'partitioned_table': bool}
            return_result (bool).

        Returns:
            DataFrame: Pandas DataFrame
        """
        if config:
            job_config = bigquery.QueryJobConfig()
            table_ref = client.dataset(config['dataset']) \
                              .table(config['table_name'])
            job_config.destination = table_ref
            job_config.write_disposition = config['write_method']

            if 'partitioned_table' in config:
                partition = bigquery.table \
                                    .TimePartitioning(field='report_timestamp')

                job_config.time_partitioning = partition

            query_job = client.query(self.query,
                                     location='EU',
                                     job_config=job_config)

        else:
            query_job = client.query(self.query,
                                     location='EU')

        result = query_job.result()

        if return_result:
            df = result.to_dataframe()
            return df

    def get_sources(self):
        return re.findall("(?<=`)(.*?)(?=`)", self.query)


if __name__ == '__main__':
    print("Don't do that...")
