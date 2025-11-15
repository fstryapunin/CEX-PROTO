from __future__ import annotations

from logging import INFO, ERROR
from pathlib import Path

from data.serializers import CsvSerializer, DataSerializer, JsonSerializer, PickleSerializer, PlainFileSerializer, YamlSerializer
from execution.common import DataInformation
from execution.namespace import NamespaceExecutor
from execution.validation import ValidationException
from meta.meta import MetadataProvider
from pipeline import cex
from log import logger

import pipeline.namespace

meta_provider = MetadataProvider()

class CexExecutor:
    def __init__(self) -> None:
        self.serilizers_by_type: dict[type, DataSerializer] = dict()
        self.default_serializers: list[DataSerializer] = [
            JsonSerializer(), 
            YamlSerializer(), 
            CsvSerializer(), 
            PickleSerializer(), 
            PlainFileSerializer()
        ]
    
    @property
    def cex(self):
        return cex

    def resolve_serializer(self, data: DataInformation) -> DataSerializer | None:
        if data.type in self.serilizers_by_type:
            return self.serilizers_by_type[data.type]
        
        if data.path is not None and data.path.is_file():
            extension = data.path.suffix
            matching_serializers = filter(lambda s: s.matches_file(extension) , self.default_serializers)
            return next(matching_serializers, None)

    #endregion

    #region Path handling

    def resolve_path(self, path: Path | str) -> Path:
        return self.cex.root_path / path

    def execute_pipeline(self, namespace: pipeline.namespace.Namespace):
        try:
            logger.info(f"Validating pipeline {namespace.name}")
            executor = NamespaceExecutor(self, namespace, meta_provider)
            logger.info(f"Preparing pipeline execution {namespace.name}")
            executor.prepare()
            logger.info(f"Executing pipeline {namespace.name}")
            executor.execute()
            logger.info(f"Executed pipeline {namespace.name}")
        except ValidationException as ex:
            logger.log_multiple(ERROR, ex.messages)
        except Exception as ex:
            logger.error(f"Unexpected runtime exception occured when executing pipeline {namespace.name}")
            logger.exception(ex)
