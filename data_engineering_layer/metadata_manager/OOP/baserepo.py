"""
base_repository.py

Generic BaseRepository for the metadata-driven ingestion framework.

This class implements only the CRUD behavior that is common to every
metadata table (dataset, source, target, pipeline_config). Table-specific
query methods (e.g. find_by_name, get_active_pipelines) belong in the
child repository classes, not here.
"""

from typing import Any, Optional

import psycopg2
from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import RealDictCursor
from psycopg2 import sql


class RepositoryError(Exception):
    """Raised when a repository operation fails."""


class BaseRepository:
    """
    Base class for all metadata repositories.

    Provides generic, table-agnostic CRUD operations built on top of
    psycopg2. Child classes only need to supply the table name and,
    optionally, the primary key column name.
    """

    def __init__(
        self,
        connection: PGConnection,
        table_name: str,
        primary_key: str = "id",
        schema: str = "public",
    ) -> None:
        self._connection = connection
        self._schema = schema
        self._table_name = table_name
        self._primary_key = primary_key


    def _table_identifier(self) -> sql.Identifier:
        """
        Return a schema-qualified table identifier.
        """
        return sql.Identifier(self._schema, self._table_name)
    
    def create(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Insert a new row built from the given dictionary.

        Args:
            data: Column-value pairs to insert.

        Returns:
            The inserted row as a dictionary.

        Raises:
            RepositoryError: If the insert fails.
        """
        if not data:
            raise RepositoryError("Cannot create a record from empty data.")

        columns = list(data.keys())
        values = list(data.values())

        query = sql.SQL("INSERT INTO {table} ({columns}) VALUES ({placeholders}) RETURNING *").format(
            table=self._table_identifier(),
            columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
            placeholders=sql.SQL(", ").join(sql.Placeholder() * len(values)),
        )

        return self._execute_and_fetch_one(query, values, "create")

    def update(self, record_id: Any, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        """
        Update an existing row identified by record_id.

        Args:
            record_id: Value of the primary key identifying the row.
            data: Column-value pairs to update.

        Returns:
            The updated row as a dictionary, or None if no row matched.

        Raises:
            RepositoryError: If the update fails.
        """
        if not data:
            raise RepositoryError("Cannot update a record with empty data.")

        columns = list(data.keys())
        values = list(data.values())

        assignments = sql.SQL(", ").join(
            sql.SQL("{col} = {placeholder}").format(col=sql.Identifier(col), placeholder=sql.Placeholder())
            for col in columns
        )

        query = sql.SQL("UPDATE {table} SET {assignments} WHERE {pk} = {placeholder} RETURNING *").format(
            table=self._table_identifier(),
            assignments=assignments,
            pk=sql.Identifier(self._primary_key),
            placeholder=sql.Placeholder(),
        )

        return self._execute_and_fetch_one(query, values + [record_id], "update")

    def delete(self, record_id: Any) -> bool:
        """
        Delete a row identified by record_id.

        Args:
            record_id: Value of the primary key identifying the row.

        Returns:
            True if a row was deleted, False if no row matched.

        Raises:
            RepositoryError: If the delete fails.
        """
        query = sql.SQL("DELETE FROM {table} WHERE {pk} = %s").format(
            table=self._table_identifier(),
            pk=sql.Identifier(self._primary_key),
        )

        try:
            with self._connection.cursor() as cursor:
                cursor.execute(query, [record_id])
                deleted = cursor.rowcount > 0
                self._connection.commit()
                return deleted
        except psycopg2.Error as exc:
            self._connection.rollback()
            raise RepositoryError(f"Failed to delete from '{self._table_name}': {exc}") from exc

    def get_by_id(self, record_id: Any) -> Optional[dict[str, Any]]:
        """
        Fetch a single row by primary key.

        Args:
            record_id: Value of the primary key identifying the row.

        Returns:
            The matching row as a dictionary, or None if not found.

        Raises:
            RepositoryError: If the query fails.
        """
        query = sql.SQL("SELECT * FROM {table} WHERE {pk} = %s").format(
            table=self._table_identifier(),
            pk=sql.Identifier(self._primary_key),
        )

        try:
            with self._connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, [record_id])
                row = cursor.fetchone()
                return dict(row) if row else None
        except psycopg2.Error as exc:
            raise RepositoryError(f"Failed to fetch from '{self._table_name}': {exc}") from exc


 
    def exists(self, filters: dict[str, Any]) -> bool:
        """
        Check whether a row matching the given criteria already exists.
 
        Intended primarily as a pre-insert duplicate check, so it matches
        on arbitrary column-value pairs rather than the primary key (which
        a not-yet-inserted record won't have). Columns are combined with
        AND.
 
        Args:
            filters: Column-value pairs to match, e.g. {"name": "Roads in
                South Sudan 2026", "dataset_id": 13}.
 
        Returns:
            True if at least one matching row exists, False otherwise.
 
        Raises:
            RepositoryError: If filters is empty or the query fails.
        """
        if not filters:
            raise RepositoryError("Cannot check existence with empty filters.")
 
        columns = list(filters.keys())
        values = list(filters.values())
 
        conditions = sql.SQL(" AND ").join(
            sql.SQL("{col} = {placeholder}").format(col=sql.Identifier(col), placeholder=sql.Placeholder())
            for col in columns
        )
 
        query = sql.SQL("SELECT EXISTS (SELECT 1 FROM {table} WHERE {conditions})").format(
            table=self._table_identifier(),
            conditions=conditions,
        )
 
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(query, values)
                return bool(cursor.fetchone()[0])
        except psycopg2.Error as exc:
            raise RepositoryError(f"Failed to check existence in '{self._table_name}': {exc}") from exc
 

    def _execute_and_fetch_one(self, query: sql.Composed, params: list[Any], action: str) -> Optional[dict[str, Any]]:
        """
        Execute a write query, commit, and return the single row it produced.

        Shared by create() and update() to avoid duplicating transaction
        handling logic.

        Args:
            query: A composed SQL query ending in RETURNING *.
            params: Parameters to bind to the query.
            action: Human-readable action name, used in error messages.

        Returns:
            The resulting row as a dictionary, or None if no row was returned.

        Raises:
            RepositoryError: If the query fails.
        """
        try:
            with self._connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                row = cursor.fetchone()
                self._connection.commit()
                return dict(row) if row else None
        except psycopg2.Error as exc:
            self._connection.rollback()
            raise RepositoryError(f"Failed to {action} record in '{self._table_name}': {exc}") from exc