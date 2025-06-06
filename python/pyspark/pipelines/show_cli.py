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

import errno
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Optional
from pyspark.pipelines.cli import load_pipeline_spec, register_definitions
from pyspark.pipelines.spark_connect_graph_element_registry import (
    SparkConnectGraphElementRegistry,
)
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph
import pyspark.sql.connect.proto as pb2
from pyspark.sql import SparkSession
from pyspark.errors import PySparkValueError

if TYPE_CHECKING:
    try:
        import graphviz  # type: ignore
    except ImportError:
        pass


def show_dataflow_graph(save: Optional[Path], imgcat: bool, spec_path: Path) -> None:
    """Display dataflow graph or save its graphic representation to a file."""
    spec = load_pipeline_spec(spec_path)

    spark_builder = SparkSession.builder
    for key, value in spec.configuration.items():
        spark_builder = spark_builder.config(key, value)

    spark = spark_builder.getOrCreate()

    try:
        # Create dataflow graph
        dataflow_graph_id = create_dataflow_graph(
            spark,
            default_catalog=spec.catalog,
            default_database=spec.database,
            sql_conf=spec.configuration,
        )

        # Register graph elements
        registry = SparkConnectGraphElementRegistry(spark, dataflow_graph_id)
        register_definitions(spec_path, registry, spec)

        # Get the resolved dataflow graph
        dataflow_graph = _get_resolved_dataflow_graph(spark, dataflow_graph_id)

        # Render the graph
        dot = _render_dataflow_graph(dataflow_graph)

        if save and imgcat:
            raise SystemExit(
                "Option --save and --imgcat are mutually exclusive. "
                "Please remove one option to execute the command."
            )

        if save:
            _save_dot_to_file(dot, str(save))
        elif imgcat:
            _display_dot_via_imgcat(dot)
        else:
            print(dot.source)
    finally:
        spark.stop()


def _get_resolved_dataflow_graph(
    spark: SparkSession, dataflow_graph_id: str
) -> pb2.PipelineCommand.GetResolvedDataflowGraph.Response:
    """Get the resolved dataflow graph from the Spark Connect server."""
    inner_command = pb2.PipelineCommand.GetResolvedDataflowGraph(
        dataflow_graph_id=dataflow_graph_id
    )
    command = pb2.Command()
    command.pipeline_command.get_resolved_dataflow_graph.CopyFrom(inner_command)

    # Execute the command and get the result
    (_, properties, _) = spark.client.execute_command(command)
    return properties["pipeline_command_result"].get_resolved_dataflow_graph_result


def _render_dataflow_graph(
    dataflow_graph: pb2.PipelineCommand.GetResolvedDataflowGraph.Response,
) -> "graphviz.Digraph":
    """Render the dataflow graph as a graphviz Digraph."""
    try:
        import graphviz

        dot = graphviz.Digraph(comment="Dataflow Graph")
        dot.attr(dpi="200")
        dot.attr(rankdir="TB")  # Top to bottom layout

        def _flow_id(flow_name: str) -> str:
            return f"flow|{flow_name}"

        def _dataset_id(dataset_name: str) -> str:
            return f"dataset|{dataset_name}"

        # Add nodes for flow definitions
        for flow_def in dataflow_graph.flow_definitions:
            flow_name = flow_def.flow_name
            dot.node(
                _flow_id(flow_name),
                label=flow_name,
                shape="box",
                style="filled",
                fillcolor="lightblue",
            )

        # Add nodes for dataset definitions
        for dataset_def in dataflow_graph.dataset_definitions:
            dataset_name = dataset_def.dataset_name
            dot.node(
                _dataset_id(dataset_name),
                label=dataset_name,
                shape="ellipse",
                style="filled",
                fillcolor="lightgreen",
            )

        # Add edges between flows and datasets
        for flow_def in dataflow_graph.flow_definitions:
            # Add edges from input datasets to flow
            for input_dataset in flow_def.input_dataset_names:
                dot.edge(_dataset_id(input_dataset), _flow_id(flow_def.flow_name))

            # Add edges from flow to output datasets
            dot.edge(_flow_id(flow_def.flow_name), _dataset_id(flow_def.target_dataset_name))

        return dot
    except ImportError:
        raise PySparkValueError(
            errorClass="PACKAGE_NOT_INSTALLED",
            messageParameters={"package_name": "graphviz", "minimum_version": "0.20"},
        )


def _display_dot_via_imgcat(dot: "graphviz.Digraph") -> None:
    """Display the graph using imgcat in the terminal."""
    data = dot.pipe(format="png")
    try:
        with subprocess.Popen("imgcat", stdout=subprocess.PIPE, stdin=subprocess.PIPE) as proc:
            out, err = proc.communicate(data)
            if out:
                print(out.decode("utf-8"))
            if err:
                print(err.decode("utf-8"), file=sys.stderr)
    except OSError as e:
        if e.errno == errno.ENOENT:
            raise SystemExit(
                "Failed to execute. Make sure the imgcat executable is on your system's PATH"
            )
        raise


def _save_dot_to_file(dot: "graphviz.Digraph", filename: str) -> None:
    """Save the graph to a file.

    Args:
        dot: The graphviz Digraph to save
        filename: The path where to save the file
    """
    filename_without_ext, _, ext = filename.rpartition(".")
    if not ext:
        ext = "png"
        filename = f"{filename}.{ext}"

    dot.render(filename=filename_without_ext, format=ext, cleanup=True)
    print(f"File {filename} saved")
