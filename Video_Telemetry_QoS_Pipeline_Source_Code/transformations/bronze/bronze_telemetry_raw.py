from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Raw telemetry data ingested from Unity Catalog Volume using Auto Loader",
    cluster_by=["eventDate", "customerId"],
    table_properties={
        "quality": "bronze",
        "domain": "video_analytics",
        "owner": "data-engineering-team",
        "contains_pii": "false",
        "refresh_frequency": "streaming",
        "sla": "near-realtime",
        "source_system": "video_streaming_platform",
        "ingestion_pattern": "auto_loader"
        }
)
@dp.expect("valid_customer_id", "customerId IS NOT NULL AND LENGTH(customerId) > 0")
@dp.expect("valid_client_id", "clientId IS NOT NULL")
@dp.expect("valid_timestamp", "timestampInfo.server > 0")
@dp.expect("valid_event_date", "eventDate IS NOT NULL")
@dp.expect("no_rescued_data", "_rescued_data IS NULL")
def bronze_telemetry_raw():
    """
    Bronze Layer: Raw Video Telemetry Data Ingestion
    
    Purpose:
        Ingests raw video streaming telemetry data from Unity Catalog Volume using Auto Loader.
        Preserves complete fidelity of source data (no transformations) while enforcing
        schema contract via schema hints.
    
    Source:
        Location: /Volumes/workspace/default/hivestreamdata/eventDate=*/
        Format: Parquet files partitioned by eventDate
        Frequency: Continuous streaming (files arrive in near-realtime)
    
    Schema Strategy:
        - Core business fields locked via cloudFiles.schemaHints (type enforcement)
        - Schema evolution mode: addNewColumns (allows new optional metrics from source)
        - Malformed records captured in _rescued_data column for monitoring
    
    Schema Structure:
        - customerId (STRING): Customer identifier
        - contentId (STRING): Video content identifier
        - clientId (STRING): Client device/app identifier
        - timestampInfo (STRUCT): 
            * server (BIGINT): Server-side timestamp (Unix epoch ms)
            * agent (BIGINT): Client-side timestamp (Unix epoch ms)
        - player (STRUCT):
            * bufferings (INT): Number of buffering events
            * bufferingTime (INT): Total buffering duration (ms)
        - totalDistribution (STRUCT): Aggregate traffic distribution
            * sourceTraffic (STRUCT): CDN source metrics (requests, responses, data)
            * p2pTraffic (STRUCT): P2P metrics (requests, responses, data)
        - qualityDistribution (MAP<STRING, STRUCT>): Per-quality-level traffic breakdown
            * Key: Quality level (e.g., "720p", "1080p")
            * Value: STRUCT with sourceTraffic and p2pTraffic metrics
        - eventDate (DATE): Partition key (date of streaming event)
        - _rescued_data (STRING): Auto Loader metadata - captures malformed records
    
    Data Quality:
        Expectations monitor critical fields:
        - customerId: Not null, non-empty
        - clientId: Not null
        - timestampInfo.server: Positive value (valid timestamp)
        - eventDate: Not null (required for partitioning)
        - _rescued_data: Null (monitors for malformed records)
    
    Auto Loader Configuration:
        - Format: Parquet
        - Type inference: Enabled (infers types for new optional fields)
        - Schema evolution: addNewColumns (graceful handling of source schema changes)
        - Partition discovery: eventDate=* pattern
    
    Performance:
        - Liquid clustering: [eventDate, customerId]
        - Efficient for time-based queries and customer-level analysis
        - Streaming ingestion with checkpointing (exactly-once semantics)
    
    Data Retention:
        - Log retention: 90 days (time travel window)
        - Deleted file retention: 90 days (VACUUM protection)
        - Rationale: Raw data can be reprocessed from source if needed
    
    Downstream Consumers:
        - silver_telemetry_enriched (streaming table - transformations)
        - silver_telemetry_quarantine (streaming table - quality issues)
    
    Monitoring:
        - Check _rescued_data expectation for malformed records
        - Track Auto Loader metrics via pipeline event log
        - Alert on schema evolution events (new columns added)
    
    SLA: Near-realtime ingestion (<5 minutes from source to bronze)
    Owner: Data Engineering Team
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        # Schema strategy: Lock down core business fields via hints,
        # allow automatic addition of new optional metrics from source
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaHints", """
            customerId STRING,
            contentId STRING,
            clientId STRING,
            timestampInfo STRUCT<server: BIGINT, agent: BIGINT>,
            player STRUCT<bufferings: INT, bufferingTime: INT>,
            totalDistribution STRUCT<
                sourceTraffic: STRUCT<requests: INT, responses: DOUBLE, requestedData: BIGINT, receivedData: BIGINT>,
                p2pTraffic: STRUCT<requests: INT, responses: DOUBLE, requestedData: BIGINT, receivedData: BIGINT>
            >,
            qualityDistribution MAP<STRING, STRUCT<
                sourceTraffic: STRUCT<requests: INT, responses: DOUBLE, requestedData: BIGINT, receivedData: BIGINT>,
                p2pTraffic: STRUCT<requests: INT, responses: DOUBLE, requestedData: BIGINT, receivedData: BIGINT>
            >>,
            eventDate DATE
        """)
        .load("/Volumes/workspace/default/hivestreamdata/eventDate=*/")
    )
