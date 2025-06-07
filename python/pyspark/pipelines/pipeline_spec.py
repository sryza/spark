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

import yaml
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from pyspark.errors import PySparkException, PySparkTypeError

PIPELINE_SPEC_FILE_NAMES = ["pipeline.yaml", "pipeline.yml"]


@dataclass(frozen=True)
class DefinitionsGlob:
    """A glob pattern for finding pipeline definitions files."""

    include: str


@dataclass(frozen=True)
class PipelineSpec:
    """Spec for a pipeline.

    :param catalog: The default catalog to use for the pipeline.
    :param database: The default database to use for the pipeline.
    :param configuration: A dictionary of Spark configuration properties to set for the pipeline.
    :param definitions: A list of glob patterns for finding pipeline definitions files.
    """

    catalog: Optional[str]
    database: Optional[str]
    configuration: Mapping[str, str]
    definitions: Sequence[DefinitionsGlob]


def find_pipeline_spec(current_dir: Path) -> Path:
    """Looks in the current directory and its ancestors for a pipeline spec file."""
    while True:
        try:
            candidates = [
                current_dir / spec_file_name for spec_file_name in PIPELINE_SPEC_FILE_NAMES
            ]
            found_files = [candidate for candidate in candidates if candidate.is_file()]
            if len(found_files) == 1:
                return found_files[0]
            elif len(found_files) > 1:
                raise PySparkException(
                    errorClass="MULTIPLE_PIPELINE_SPEC_FILES_FOUND",
                    messageParameters={"dir_path": str(current_dir)},
                )
        except PermissionError:
            raise PySparkException(
                errorClass="PIPELINE_SPEC_FILE_NOT_FOUND",
                messageParameters={"dir_path": str(current_dir)},
            )

        if current_dir.parent == current_dir or not current_dir.parent.exists():
            raise PySparkException(
                errorClass="PIPELINE_SPEC_FILE_NOT_FOUND",
                messageParameters={"dir_path": str(current_dir)},
            )

        current_dir = current_dir.parent


def load_pipeline_spec(spec_path: Path) -> PipelineSpec:
    """Load the pipeline spec from a YAML file at the given path."""
    with spec_path.open("r") as f:
        return unpack_pipeline_spec(yaml.safe_load(f))


def unpack_pipeline_spec(spec_data: Mapping[str, Any]) -> PipelineSpec:
    for key in spec_data.keys():
        if key not in ["catalog", "database", "schema", "configuration", "definitions"]:
            raise PySparkException(
                errorClass="PIPELINE_SPEC_UNEXPECTED_FIELD", messageParameters={"field_name": key}
            )

    return PipelineSpec(
        catalog=spec_data.get("catalog"),
        database=spec_data.get("database", spec_data.get("schema")),
        configuration=validate_str_dict(spec_data.get("configuration", {}), "configuration"),
        definitions=[
            DefinitionsGlob(include=entry["glob"]["include"])
            for entry in spec_data.get("definitions", [])
        ],
    )


def validate_str_dict(d: Mapping[str, str], field_name: str) -> Mapping[str, str]:
    """Raises an error if the dictionary is not a mapping of strings to strings."""
    if not isinstance(d, dict):
        raise PySparkTypeError(
            errorClass="PIPELINE_SPEC_FIELD_NOT_DICT",
            messageParameters={"field_name": field_name, "field_type": type(d).__name__},
        )

    for key, value in d.items():
        if not isinstance(key, str):
            raise PySparkTypeError(
                errorClass="PIPELINE_SPEC_DICT_KEY_NOT_STRING",
                messageParameters={"field_name": field_name, "key_type": type(key).__name__},
            )
        if not isinstance(value, str):
            raise PySparkTypeError(
                errorClass="PIPELINE_SPEC_DICT_VALUE_NOT_STRING",
                messageParameters={
                    "field_name": field_name,
                    "key_name": key,
                    "value_type": type(value).__name__,
                },
            )

    return d
