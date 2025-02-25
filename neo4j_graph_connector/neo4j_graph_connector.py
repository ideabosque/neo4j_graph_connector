#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import datetime
import logging
import traceback
from typing import Any, Dict, List, Optional, Tuple

from neo4j import GraphDatabase


class Neo4jConnector(object):
    def __init__(self, logger: logging.Logger, **setting: Dict[str, Any]) -> None:
        self.logger = logger
        self.driver = GraphDatabase.driver(
            setting["neo4j_uri"],
            auth=(setting["neo4j_username"], setting["neo4j_password"]),
        )
        self.database = setting.get("neo4j_database", "neo4j")

    def close(self):
        if self.driver:
            self.driver.close()

    @property
    def driver(self):
        return self._driver

    @driver.setter
    def driver(self, driver: object) -> object:
        self._driver = driver

    def get_graph_schema(self) -> Dict[str, Any]:
        """
        Generates the schema in the specified format, including:
        - Entities with attributes and relations
        - Relations with source and target
        :param database: The name of the database to query
        :return: A dictionary representing the schema
        """
        schema = {"entities": {}, "relations": {}}
        try:
            with self.driver.session(database=self.database) as session:
                # Get all labels and their properties
                labels_query = """
                    MATCH (n)
                    RETURN DISTINCT labels(n) AS labels, keys(n) AS properties
                """
                label_results = session.run(labels_query)
                for record in label_results:
                    labels = record["labels"]
                    properties = record["properties"]
                    for label in labels:
                        if label not in schema["entities"]:
                            schema["entities"][label] = {
                                "attributes": [],
                                "relations": [],
                            }
                        schema["entities"][label]["attributes"] = list(
                            set(schema["entities"][label]["attributes"])
                            | set(properties)
                        )

                # Get all relationship types and their source/target mappings
                relationships_query = """
                    MATCH (a)-[r]->(b)
                    RETURN DISTINCT type(r) AS relationship, labels(a) AS source, labels(b) AS target
                """
                relationship_results = session.run(relationships_query)
                for record in relationship_results:
                    relationship = record["relationship"]
                    source_labels = record["source"]
                    target_labels = record["target"]

                    # Add relationship details
                    schema["relations"][relationship] = {
                        "source": source_labels[0] if source_labels else None,
                        "target": target_labels[0] if target_labels else None,
                    }

                    # Update relations for source entity
                    if source_labels:
                        for source in source_labels:
                            if source in schema["entities"]:
                                schema["entities"][source]["relations"].append(
                                    relationship
                                )

            # Deduplicate relations for entities
            for entity in schema["entities"]:
                schema["entities"][entity]["relations"] = list(
                    set(schema["entities"][entity]["relations"])
                )

            return schema
        except Exception as e:
            log = traceback.format_exc()
            self.logger.error(log)
            raise e

    def execute_cypher_query_with_pagination(
        self,
        cypher_query: str,
        parameters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        skip: int = 0,
        get_total: bool = False,
    ) -> Tuple[int, List[Dict[str, Any]]]:
        """
        Executes a Cypher query with pagination on the specified database and optionally returns the total number of results.

        :param cypher_query: The Cypher query string
        :param parameters: A dictionary of query parameters (optional)
        :param database: The name of the database to query (default is "neo4j")
        :param limit: The maximum number of records to fetch per page (default is 100)
        :param skip: The number of records to skip (default is 0)
        :param get_total: Whether to retrieve the total number of results (default is False)
        :return: A dictionary containing the paginated results and total count (if requested)
        """
        try:
            results = []
            total = None

            if get_total:
                # Modify the query to get the total count
                count_query = (
                    "CALL (*) { " f"{cypher_query} " "} RETURN count(*) as total"
                )
                with self.driver.session(database=self.database) as session:
                    total_result = session.run(count_query, parameters or {})
                    total = total_result.single()["total"]

            _parameters = parameters or {}
            # Add pagination clauses to the query
            if "LIMIT" in cypher_query.upper() or "SKIP" in cypher_query.upper():
                _cypher_query = cypher_query
            else:
                _cypher_query = f"{cypher_query} SKIP $skip LIMIT $limit"
                _parameters.update({"skip": skip, "limit": limit})

            with self.driver.session(database=self.database) as session:
                result = session.run(_cypher_query, _parameters)
                results = [
                    {
                        key: (
                            (
                                value.to_native().isoformat()
                                if isinstance(
                                    value.to_native(),
                                    (datetime.date, datetime.datetime),
                                )
                                else value.to_native()
                            )
                            if hasattr(value, "to_native")
                            else value
                        )
                        for key, value in record.data().items()
                    }
                    for record in result
                ]

            return total, results
        except Exception as e:
            log = traceback.format_exc()
            self.logger.error(log)
            raise e
