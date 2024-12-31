#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import logging
import traceback
from typing import Any, Dict, List, Optional

from neo4j import GraphDatabase


class Neo4jConnector(object):
    def __init__(self, logger: logging.Logger, **setting: Dict[str, Any]) -> None:
        self.logger = logger
        self.driver = GraphDatabase.driver(
            setting["neo4j_uri"],
            auth=(setting["neo4j_username"], setting["neo4j_password"]),
        )

    def close(self):
        if self.driver:
            self.driver.close()

    @property
    def driver(self):
        return self._driver

    @driver.setter
    def driver(self, driver: object) -> object:
        self._driver = driver

    def get_graph_schema(self, database: str = "neo4j") -> Dict[str, Any]:
        """
        Generates the schema in the specified format, including:
        - Entities with attributes and relations
        - Relations with source and target
        :param database: The name of the database to query
        :return: A dictionary representing the schema
        """
        schema = {"entities": {}, "relations": {}}
        try:
            with self.driver.session(database=database) as session:
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

    def execute_cypher_query(
        self,
        cypher_query: str,
        parameters: Optional[Dict[str, Any]] = None,
        database: str = "neo4j",
    ) -> List[Dict[str, Any]]:
        """
        Executes a Cypher query on the specified database and returns the results.

        :param cypher_query: The Cypher query string
        :param parameters: A dictionary of query parameters (optional)
        :param database: The name of the database to query (default is "neo4j")
        :return: A list of dictionaries containing the query results
        """
        try:
            with self.driver.session(database=database) as session:
                result = session.run(cypher_query, parameters)
                return [record.data() for record in result]
        except Exception as e:
            log = traceback.format_exc()
            self.logger.error(log)
            raise e
