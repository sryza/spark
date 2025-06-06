#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

try:
    import graphviz

    has_graphviz = True
except ImportError:
    has_graphviz = False

from pyspark.pipelines.show_cli import (
    render_dataflow_graph,
    get_resolved_dataflow_graph,
    show_dataflow_graph,
    _display_dot_via_imgcat,
    _save_dot_to_file,
)


class ShowCliTests(unittest.TestCase):
    @unittest.skipIf(not has_graphviz, "graphviz not installed")
    def test_render_dataflow_graph(self):
        # Create a mock dataflow graph
        mock_dataflow_graph = {
            "flow_definitions": [
                {
                    "flow_id": "flow1",
                    "flow_name": "Flow 1",
                    "input_datasets": ["dataset1"],
                    "output_datasets": ["dataset2"],
                }
            ],
            "dataset_definitions": [
                {"dataset_id": "dataset1", "dataset_name": "Dataset 1"},
                {"dataset_id": "dataset2", "dataset_name": "Dataset 2"},
            ],
        }

        # Render the graph
        dot = render_dataflow_graph(mock_dataflow_graph)

        # Check that the dot object is created correctly
        self.assertIsInstance(dot, graphviz.Digraph)

        # Check that the source contains the expected nodes and edges
        source = dot.source
        self.assertIn("flow1", source)
        self.assertIn("Flow 1", source)
        self.assertIn("dataset1", source)
        self.assertIn("Dataset 1", source)
        self.assertIn("dataset2", source)
        self.assertIn("Dataset 2", source)

    @patch("pyspark.sql.SparkSession")
    def test_get_resolved_dataflow_graph(self, mock_spark_session):
        # Create mock objects
        mock_client = MagicMock()
        mock_spark_session.client = mock_client

        # Set up the mock to return a specific result
        mock_properties = {
            "pipeline_command_result": MagicMock(
                get_resolved_dataflow_graph_result={
                    "flow_definitions": [],
                    "dataset_definitions": [],
                }
            )
        }
        mock_client.execute_command.return_value = (None, mock_properties, None)

        # Call the function
        result = get_resolved_dataflow_graph(mock_spark_session, "graph-123")

        # Check that the result is correct
        self.assertEqual(result, {"flow_definitions": [], "dataset_definitions": []})

        # Check that the client was called with the correct command
        mock_client.execute_command.assert_called_once()

    @unittest.skipIf(not has_graphviz, "graphviz not installed")
    @patch("pyspark.pipelines.show_cli.get_resolved_dataflow_graph")
    @patch("pyspark.pipelines.show_cli._save_dot_to_file")
    @patch("pyspark.sql.SparkSession")
    def test_show_dataflow_graph_save(self, mock_spark_session, mock_save, mock_get_graph):
        # Set up mocks
        mock_get_graph.return_value = {"flow_definitions": [], "dataset_definitions": []}

        # Create mock args
        args = MagicMock()
        args.dataflow_graph_id = "graph-123"
        args.save = "graph.png"
        args.imgcat = False

        # Call the function
        show_dataflow_graph(args)

        # Check that the save function was called
        mock_save.assert_called_once()

    @unittest.skipIf(not has_graphviz, "graphviz not installed")
    @patch("pyspark.pipelines.show_cli._save_dot_to_file")
    def test_save_dot_to_file(self, mock_render):
        # Create a mock dot object
        mock_dot = MagicMock()

        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix=".png") as temp_file:
            # Call the function
            _save_dot_to_file(mock_dot, temp_file.name)

            # Check that render was called with the correct parameters
            mock_dot.render.assert_called_once()


if __name__ == "__main__":
    unittest.main()
