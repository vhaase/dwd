"""dwd ETL processing."""

from typing import Any, Dict, List, Optional, Tuple

from apps_base.base_etl_classes import OCIETLBase
from common.argparse import ParseArguments
from common.dependencies.gcs import AbstractCloudStorage
from common.dependencies.logging import GenericLogging
from injector import inject, noninjectable

from .conf import Config


@inject
@noninjectable("_sys_argv")
def bootstrap(log: GenericLogging, _sys_argv: Optional[List[str]] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Bootstrap callable for dwd.

    Args:
        log: Instance of GCPLog, to be injected.
        _sys_argv: Development option. Allows to override
            retrieving arguments from sys.argv, instead, use the
            provided list.

    Returns:
        Tuple containing class_variables and secret_variables as dict (empty)

    """
    arg = ParseArguments.argument_parse_entity

    class_variables = ParseArguments(sys_argv=_sys_argv)(
        source_bucket=arg(str),
        source_object_path=arg(str),
        destination_bucket=arg(str),
        destination_object_path=arg(str),
    ).parse_as_dict()

    log.info(f"dwd - retrieved class variables: {class_variables}")
    return class_variables


class ETL(OCIETLBase):
    """Process dwd from RZ to CZ.

    Args:
        source_bucket: GCS bucket hosting the source CSV
        source_object_path: CSV URI
        destination_bucket: GCS bucket which will store the
            processed parquet file
        destination_object_path: Target URI
        gcs: Instance of AbstractCloudStorage.
        log: Instance of Generic Logging.

    """

    @inject
    @noninjectable(
        "source_bucket",
        "source_object_path",
        "destination_bucket",
        "destination_object_path",
    )
    def __init__(
        self,
        source_bucket: str,
        source_object_path: str,
        destination_bucket: str,
        destination_object_path: str,
        gcs: AbstractCloudStorage,
        log: GenericLogging,
        conf: Optional[Config] = None,
        *args,
        **kwargs,
    ):
        super().__init__(
            *args,
            source_bucket=source_bucket,
            source_object_path=source_object_path,
            destination_bucket=destination_bucket,
            destination_object_path=destination_object_path,
            gcs=gcs,
            log=log,
            **kwargs,
        )
        self.conf = conf or Config()
        raise NotImplementedError

    def extract(self):
        """Loads data from source bucket."""
        raise NotImplementedError

    def transform(self, *args):
        """Transforms and analyzes tabular data (core ingest/business logic)."""
        raise NotImplementedError

    def load(self, *args) -> Optional[Dict[str, Any]]:
        """Loads processing results to data sink."""
        raise NotImplementedError
