import dlt
from pipeline_utils import parse_pipeline_args
import logging
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

# Create a logger and set handler
logger = logging.getLogger("dlt")
logger.setLevel(logging.INFO)

@dlt.resource(name="virksomheder", primary_key="cvrNummer")
def search_elastic_search(
    from_timestamp: str =
    dlt.sources.incremental(
        "sidstIndlaest", initial_value="1970-01-01T00:00:00.000"
    )
):
    url = "http://distribution.virk.dk:80"
    index = "cvr-permanent/virksomhed"
    elastic_client = Elasticsearch(
        url,
        http_auth=(
            dlt.secrets["sources.virk_cvr_pipeline.virk_cvr_source.cvr_username"],
            dlt.secrets["sources.virk_cvr_pipeline.virk_cvr_source.cvr_password"],
        ),
        timeout=60,
        max_retries=10,
        retry_on_timeout=True,
    )
    # chunk size
    elastic_search_scan_size = 128
    # server keep alive time
    elastic_search_scroll_time = "5m"
    # set elastic search params
    params = {"scroll": elastic_search_scroll_time, "size": elastic_search_scan_size}
    # create elasticsearch search object
    el_search = Search(using=elastic_client, index=index)

    # Add source filtering for Vrvirksomhed
    el_search = el_search.source(
        [
            "Vrvirksomhed.enhedstype",
            "Vrvirksomhed.virkningsAktoer",
            "Vrvirksomhed.cvrNummer",
            "Vrvirksomhed.virksomhedMetadata",
            "Vrvirksomhed.deltagerRelation",
            "Vrvirksomhed.sidstOpdateret",
            "Vrvirksomhed.samtId",
            "Vrvirksomhed.sidstIndlaest",
            "Vrvirksomhed.enhedsNummer",
        ]
    )

    # Apply range query if timestamp is provided
    logger.info(f"Applying range query for timestamp: {from_timestamp.last_value}")
    el_search = el_search.query(
        "range", **{"Vrvirksomhed.sidstIndlaest": {"gte": from_timestamp.last_value}}
    )

    el_search = el_search.params(**params)
    
    for hit in el_search.scan():
        data = hit.to_dict()
        if 'Vrvirksomhed' in data:
            yield data['Vrvirksomhed']

def main() -> None:
    args = parse_pipeline_args(description="Virk CVR Data Pipeline")

    pipeline = dlt.pipeline(
        pipeline_name="rest_api_virk_cvr_pipeline",
        destination="duckdb",
        dataset_name="det_centrale_virksomhedsregister",
        progress="log",
        dev_mode=args.dev_mode
    )

    # Remove the transformer since we're already getting dictionaries
    pipeline.run(
        data=search_elastic_search(),
        write_disposition=args.write_disposition,

    )
    logger.info(f"Pipeline run completed successfully")
    logger.info(f"Last Trace: {pipeline.last_trace}")

if __name__ == "__main__":
    main()
