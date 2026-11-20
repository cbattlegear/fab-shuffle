"""SQL identifier and target clearing behavior shared by streamed transfers."""

import pyodbc

from fabshuffle.fabric.data_stores import TableRef
from fabshuffle.transfer import bulkcopy


def test_streaming_clear_falls_back_to_delete_and_quotes_identifiers():
    class Cursor:
        def __init__(self):
            self.statements = []

        def execute(self, sql):
            self.statements.append(sql)
            if sql.startswith("TRUNCATE"):
                raise pyodbc.Error("foreign key")

    cursor = Cursor()
    bulkcopy._clear_table(cursor, TableRef(schema="sales", name="Odd]Name"))
    assert cursor.statements == [
        "TRUNCATE TABLE [sales].[Odd]]Name]",
        "DELETE FROM [sales].[Odd]]Name]",
    ]
