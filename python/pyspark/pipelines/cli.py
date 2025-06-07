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

"""
Implementation of spark-pipelines CLI.

Example usage:
    $ bin/spark-pipelines run --spec /path/to/pipeline.yaml
"""
from contextlib import contextmanager
import argparse
import importlib.util
import os
from pathlib import Path
from typing import Generator, Optional, Tuple

from pyspark.errors import PySparkException
from pyspark.sql import SparkSession
from pyspark.pipelines.graph_element_registry import (
    graph_element_registration_context,
    GraphElementRegistry,
)
from pyspark.pipelines.init_cli import init
from pyspark.pipelines.logging_utils import log_with_curr_timestamp
from pyspark.pipelines.pipeline_spec import PipelineSpec, load_pipeline_spec, find_pipeline_spec
from pyspark.pipelines.show_cli import show_dataflow_graph
from pyspark.pipelines.spark_connect_graph_element_registry import (
    SparkConnectGraphElementRegistry,
)
from pyspark.pipelines.spark_connect_pipeline import (
    create_dataflow_graph,
    start_run,
    handle_pipeline_events,
)


def register_definitions(
    spec_path: Path, registry: GraphElementRegistry, spec: PipelineSpec
) -> None:
    """Register the graph element definitions in the pipeline spec with the given registry.
    - Looks for Python files matching the glob patterns in the spec and imports them.
    - Looks for SQL files matching the blob patterns in the spec and registers thems.
    """
    path = spec_path.parent
    with change_dir(path):
        with graph_element_registration_context(registry):
            log_with_curr_timestamp(f"Loading definitions. Root directory: '{path}'.")
            for definition_glob in spec.definitions:
                glob_expression = definition_glob.include
                matching_files = [p for p in path.glob(glob_expression) if p.is_file()]
                log_with_curr_timestamp(
                    f"Found {len(matching_files)} files matching glob '{glob_expression}'"
                )
                for file in matching_files:
                    if file.suffix == ".py":
                        log_with_curr_timestamp(f"Importing {file}...")
                        module_spec = importlib.util.spec_from_file_location(file.stem, str(file))
                        assert module_spec is not None, f"Could not find module spec for {file}"
                        module = importlib.util.module_from_spec(module_spec)
                        assert (
                            module_spec.loader is not None
                        ), f"Module spec has no loader for {file}"
                        module_spec.loader.exec_module(module)
                    elif file.suffix == ".sql":
                        log_with_curr_timestamp(f"Registering SQL file {file}...")
                        with file.open("r") as f:
                            sql = f.read()
                        file_path_relative_to_spec = file.relative_to(spec_path.parent)
                        registry.register_sql(sql, file_path_relative_to_spec)
                    else:
                        raise PySparkException(
                            errorClass="PIPELINE_UNSUPPORTED_DEFINITIONS_FILE_EXTENSION",
                            messageParameters={"file_path": str(file)},
                        )


@contextmanager
def change_dir(path: Path) -> Generator[None, None, None]:
    """Change the current working directory to the given path and restore it on close()."""
    prev = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev)


def _register_dataflow_graph(spec_path: Path) -> Tuple[SparkSession, str]:
    """Register a dataflow graph with the Spark session."""
    log_with_curr_timestamp(f"Loading pipeline spec from {spec_path}...")
    spec = load_pipeline_spec(spec_path)

    log_with_curr_timestamp("Creating Spark session...")
    spark_builder = SparkSession.builder
    for key, value in spec.configuration.items():
        spark_builder = spark_builder.config(key, value)

    spark = spark_builder.getOrCreate()

    log_with_curr_timestamp("Creating dataflow graph...")
    dataflow_graph_id = create_dataflow_graph(
        spark,
        default_catalog=spec.catalog,
        default_database=spec.database,
        sql_conf=spec.configuration,
    )

    log_with_curr_timestamp("Registering graph elements...")
    registry = SparkConnectGraphElementRegistry(spark, dataflow_graph_id)
    register_definitions(spec_path, registry, spec)
    return spark, dataflow_graph_id


def run(spec_path: Path) -> None:
    spark, dataflow_graph_id = _register_dataflow_graph(spec_path)

    log_with_curr_timestamp("Starting run...")
    result_iter = start_run(spark, dataflow_graph_id)
    try:
        handle_pipeline_events(result_iter)
    finally:
        spark.stop()


def show_graph(save: Optional[Path], imgcat: bool, spec_path: Path) -> None:
    spark, dataflow_graph_id = _register_dataflow_graph(spec_path)
    show_dataflow_graph(save, imgcat, spark, dataflow_graph_id)


def _resolve_spec_path_from_arg(spec_arg: Optional[str]) -> Path:
    if spec_arg is not None:
        spec_path = Path(spec_arg)
        if not spec_path.is_file():
            raise PySparkException(
                errorClass="PIPELINE_SPEC_FILE_DOES_NOT_EXIST",
                messageParameters={"spec_path": spec_arg},
            )
        return spec_path
    else:
        return find_pipeline_spec(Path.cwd())

def main():
    parser = argparse.ArgumentParser(description="Pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # "run" subcommand
    run_parser = subparsers.add_parser("run", help="Run a pipeline.")
    run_parser.add_argument("--spec", help="Path to the pipeline spec.")

    # "init" subcommand
    init_parser = subparsers.add_parser(
        "init",
        help="Generates a simple pipeline project, including a spec file and example definitions.",
    )
    init_parser.add_argument(
        "--name",
        help="Name of the project. A directory with this name will be created underneath the "
        "current directory.",
        required=True,
    )

    # "show-graph" subcommand
    show_graph_parser = subparsers.add_parser(
        "show-graph",
        help="Display the dataflow graph for a pipeline.",
    )
    show_graph_parser.add_argument(
        "--spec",
        help="Path to the pipeline spec. If not provided, will look for a spec file in the current directory.",
    )
    show_graph_parser.add_argument(
        "--save",
        help="Path to save the graph visualization (e.g., 'graph.png', 'graph.pdf').",
    )
    show_graph_parser.add_argument(
        "--imgcat",
        help="Display the graph using imgcat in the terminal. Requires imgcat to be installed.",
        action="store_true",
    )

    args = parser.parse_args()
    assert args.command in ["run", "init", "show-graph"]

    if args.command == "run":
        run(spec_path=_resolve_spec_path_from_arg(args.spec))
    elif args.command == "init":
        init(args.name)
    elif args.command == "show-graph":
        spec_path = _resolve_spec_path_from_arg(args.spec)
        show_graph(
            save=Path(args.save) if args.save else None, spec_path=spec_path, imgcat=args.imgcat
        )

if __name__ == "__main__":
    main()
