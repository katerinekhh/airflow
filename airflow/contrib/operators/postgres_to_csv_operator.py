import csv
import sqlparse

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults


class PostgresToCsvOperator(BaseOperator):
    """
       Executes sql code in a specific Postgres database and creates a .csv file
       with selected data. CSV headers will match column names from sql select statement by default.
       Or can be passed as a parameter.

       :param sql: the sql code to be executed. (templated)
       :type sql: Can receive a str representing a sql statement,
           a list of str (sql statements), or reference to a template file.
           Template reference are recognized by str ending in '.sql'
       :param postgres_conn_id: reference to a specific postgres database
       :type postgres_conn_id: str
       :param csv_file_path: path to csv file, which will be created with selected data.
       :type csv_file_path: str
       :param parameters: (optional) the parameters to render the SQL query with.
          (default value: None)
       :type parameters: dict
       :param headers: list of column names for csv file, if they should not match default headers
           corresponding column names from sql select statement.
           (default value: None)
       :type headers: list[str]
       :param increment: if True, creates a value for %(last_updated_value)s parameter.
           WHERE clause in your sql should contain such parameter.
           (default value: False)
           'incremental sql' used for executing last_updated_value:
               'SELECT MAX({{ task.last_modified_fname }}) FROM {{ task.destination_table }}'
       :type increment: bool

       :param destination_table: table name, from where to select last updated value.
       :type destination_table: str
       :param last_modified_fname: column name to refer to in 'incremental sql'.
       :type last_modified_fname: str
       :param destination_conn_id: reference to a specific postgres database to execute 'incremental sql'.
       :type destination_conn_id: str
       :param default_last_updated_value: default last_updated_value, if None is selected.
           (default value: '1970-01-01 00:00:00+00:00')
       :type default_last_updated_value: str/int
   """

    template_fields = [
        'sql', 'last_updated_sql',
        'destination_table', 'last_modified_fname',
    ]
    template_ext = ['.sql']

    @apply_defaults
    def __init__(  # noqa: CFQ002
            self,
            csv_file_path: str,
            parameters={},
            sql='',
            postgres_conn_id='',
            destination_conn_id='',
            destination_table='',
            last_modified_fname='',
            headers=None,
            increment=False,
            default_last_updated_value='1970-01-01 00:00:00+00:00',
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.destination_conn_id = destination_conn_id
        self.csv_file_path = csv_file_path
        self.parameters = parameters
        self.increment = increment
        self.headers = headers
        self.destination_table = destination_table
        self.last_modified_fname = last_modified_fname
        self.last_updated_sql = 'SELECT MAX({{ task.last_modified_fname }}) FROM {{ task.destination_table }}'
        self.default_last_updated_value = default_last_updated_value
        if self.parameters and not isinstance(self.parameters, dict):
            raise SyntaxError(f"Argument 'parameters' must be type - dict")
        if self.increment:
            if not self.last_modified_fname:
                raise SyntaxError("Argument 'last_modified_fname' is required for incremental select")
            if not self.destination_table:
                raise SyntaxError("Argument 'destination_table' is required for incremental select")

    @staticmethod
    def _parse_sql_field_to_csv_header(sql_field) -> str:
        csv_header = sql_field.lower().strip().replace('\"', '')
        if ' as ' in csv_header:
            csv_header = csv_header.split(' as ')[-1]
        if '.' in csv_header:
            csv_header = csv_header.split('.')[1]
        if ' ' in csv_header:
            csv_header = csv_header.split(' ')[1]
        return csv_header

    def execute(self, context):  # noqa: C901
        if self.increment:
            last_updated_value = self._extract_last_updated_value()
            self.parameters.update({'last_updated_value': last_updated_value})

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        results = hook.get_records(sql=self.sql, parameters=self.parameters)
        if not results:
            self.log.info('No data extracted')

        if not self.headers:
            self.headers = self._get_csv_headers_from_sql()
        self._create_csv(results, self.headers)

    def _extract_last_updated_value(self) -> str:
        hook = PostgresHook(postgres_conn_id=self.destination_conn_id)
        last_updated_field = hook.get_first(sql=self.last_updated_sql)[0]
        if not last_updated_field:
            self.log.info(
                f'Last event value not found, ' + (
                    f'using default value - {self.default_last_updated_value}'),
            )
            return self.default_last_updated_value

        self.log.info(f'Last event value was {last_updated_field}')
        return last_updated_field

    def _create_csv(self, results: list, headers: list) -> None:
        with open(self.csv_file_path, 'w') as csv_file:
            writer_headers = csv.DictWriter(csv_file, fieldnames=headers)
            writer_headers.writeheader()
            writer = csv.writer(csv_file)
            for row in results:
                writer.writerow(row)
        self.log.info('Finished creating csv file')

    def _get_csv_headers_from_sql(self) -> list:
        parsed_sql = sqlparse.parse(self.sql)[0].tokens
        parsed_sql_fields = []
        for token in parsed_sql:
            if not isinstance(token, sqlparse.sql.IdentifierList):
                continue
            for field in token.get_identifiers():
                parsed_sql_fields.append(field.value)
        headers = []
        for sql_field in parsed_sql_fields:
            csv_header = self._parse_sql_field_to_csv_header(sql_field)
            headers.append(csv_header)
        return headers