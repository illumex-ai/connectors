import snowflake.connector
import pandas as pd
from common.connectors.db.db_connector import DBConnector


class Snowflake(DBConnector):
    def __init__(self, connection):
        super().__init__(connection)

    def _query(self, query, db=""):
        with snowflake.connector.connect(
            user=self.connection_properties["user"],
            password=self.connection_properties["password"],
            account=self.connection_properties["account"],
            database=db,
            login_timeout=10,
        ) as conn:
            with conn.cursor() as cursor:
                warehouse = self.connection_properties["warehouse"]
                cursor.execute(f"USE WAREHOUSE {warehouse};")
                cursor.execute(query)
                # Hack: Snowflake ignore AS lower and make it upper.
                columns_list = [desc[0].lower() for desc in cursor.description]
                return pd.DataFrame(cursor.fetchall(), columns=columns_list)

    # override
    def test(self):
        res = self._query(
            """
            SHOW DATABASES;
        """
        )
        res = res["name"].to_list()
        return {"databases": res}

    # Unify into one function because of Athena
    def get_schemas(self):
        def get_columns():
            def get(db):
                q = f"""
                SELECT TABLE_CATALOG AS "database", TABLE_SCHEMA AS "schema", TABLE_NAME AS "table_name", COLUMN_NAME AS "column_name", ORDINAL_POSITION AS "ordinal_position" ,DATA_TYPE AS "data_type", COLUMN_DEFAULT AS "default", IS_NULLABLE AS "is_nullable", CASE 
                    WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN CHARACTER_MAXIMUM_LENGTH
                    WHEN NUMERIC_PRECISION IS NOT NULL THEN NUMERIC_PRECISION
                    WHEN DATETIME_PRECISION IS NOT NULL THEN DATETIME_PRECISION
                    ELSE INTERVAL_PRECISION
                    END AS "Length", COMMENT as "comment"
                FROM {db}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA';
                """

                schema = self._query(q, db)

                schema["column_name"] = schema["column_name"].str.removesuffix(
                    "$SYS_FACADE$0"
                )
                schema["column_name"] = schema["column_name"].str.removesuffix(
                    "$SYS_FACADE$1"
                )
                schema = schema.drop_duplicates(
                    subset=["database", "schema", "table_name", "column_name"]
                )
                schema["length"] = schema["length"].astype("Int64")
                return schema

            schemas = map(get, self.databases)
            return pd.concat(schemas, ignore_index=True)

        def get_tables():
            def get(db):
                q = f"""
                SELECT TABLE_CATALOG AS "database", TABLE_SCHEMA AS "schema", TABLE_NAME AS "table_name", TABLE_TYPE AS "table_type",ROW_COUNT AS "row_count",BYTES AS "size",RETENTION_TIME AS "retention_time",CREATED AS "created",LAST_ALTERED AS "last_altered", COMMENT AS "comment"
                FROM {db}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA';
                """

                schema = self._query(q, db)
                schema["row_count"] = schema["row_count"].astype("Int64")
                schema["retention_time"] = schema["retention_time"].astype("Int64")
                schema["size"] = schema["size"].astype("Int64")
                return schema

            schemas = map(get, self.databases)
            return pd.concat(schemas, ignore_index=True)

        return get_tables(), get_columns()

    # db is needed by snowflake altought the query history is not per db
    def get_queries(self, data_interval_start, data_interval_end):
        data_interval_start = data_interval_start.to_iso8601_string()
        data_interval_end = data_interval_end.to_iso8601_string()
        q = f"""
        SELECT END_TIME as end_time, QUERY_TEXT as query_text
        FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(TO_TIMESTAMP_LTZ('{str(data_interval_start)}'),TO_TIMESTAMP_LTZ('{str(data_interval_end)}')))
        WHERE QUERY_TYPE != 'USE' AND QUERY_TYPE != 'SHOW' AND QUERY_TYPE != 'GRANT' AND QUERY_TYPE != 'CREATE_USER' AND QUERY_TYPE != 'CREATE_ROLE'
        AND QUERY_TYPE != 'DROP' AND QUERY_TYPE != 'COMMIT' AND QUERY_TYPE != 'ALTER_SESSION'
        AND EXECUTION_STATUS='SUCCESS' AND lower(QUERY_TEXT) NOT LIKE ('%information_schema%')
        ORDER BY END_TIME DESC;
        """
        return self._query(q, self.databases[0])

    def get_views(self):
        def get(db):
            q = f"""SHOW VIEWS IN DATABASE {db}"""
            return self._query(q, db)

        views = map(get, self.databases)
        views = pd.concat(views, ignore_index=True)
        views = views.loc[views["schema_name"] != "INFORMATION_SCHEMA"]
        views = views.rename(
            columns={
                "schema_name": "schema",
                "database_name": "database",
                "text": "view_definition",
            }
        )
        views = views[["name", "schema", "database", "view_definition"]]
        return views
