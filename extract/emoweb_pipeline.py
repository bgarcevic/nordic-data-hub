from typing import Any, Optional
import logging
import dlt
from dlt.sources.rest_api import (
    RESTAPIConfig,
    rest_api_resources,
)
from dlt.sources.rest_api import rest_api_source
import logging
import sys
from loguru import logger

class InterceptHandler(logging.Handler):

    @logger.catch(default=True, onerror=lambda _: sys.exit(1))
    def emit(self, record):
        # Get corresponding Loguru level if it exists.
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())

logger_dlt = logging.getLogger("dlt")
logger_dlt.addHandler(InterceptHandler())

logger.add("dlt_loguru.log")

@dlt.source
def emoweb_source(emoweb_username: Optional[str] = dlt.secrets.value, emoweb_password: Optional[str] = dlt.secrets.value) -> Any:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://emoweb.dk/EMOData/EMOData.svc/",
            "auth": (
                {
                    "type": "http_basic",
                    "username": emoweb_username,
                    "password": emoweb_password,
                }
            ),
            "headers": {
                "Content-Type": "application/json"
            },
            "paginator": {
                "type": "single_page",
            }
        },
        # The default configuration for all resources and their endpoints
        "resource_defaults": {
            "write_disposition": "replace"
        },
        "resources": [
            {
                "name": "get_energy_label_change_log",
                "endpoint": {
                    "path": "GetEnergyLabelChangeLog",
                    # Query parameters for the endpoint
                    "params": {
                        "fromDate": "2016-07-01",
                        "toDate": "2024-10-15",
                    },
                    "data_selector": "SearchResults"
                },
            },
        ],
    }

    yield from rest_api_resources(config)


def load_emoweb() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_emoweb",
        destination=dlt.destinations.duckdb("data.duckdb"),
        dataset_name="emoweb",
        dev_mode=True,
    )

    load_info = pipeline.run(emoweb_source())
    print(load_info)

if __name__ == "__main__":
    load_emoweb()
