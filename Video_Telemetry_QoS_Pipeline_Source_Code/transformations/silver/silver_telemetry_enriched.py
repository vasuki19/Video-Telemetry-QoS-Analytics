from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Flattened and enriched telemetry data with exploded quality distribution",
    cluster_by=["eventDate", "customerId", "clientId"],
    table_properties={
        "quality": "silver",
        "domain": "video_analytics",
        "owner": "data-engineering-team",
        "contains_pii": "false",
        "refresh_frequency": "streaming",
        "sla": "near-realtime"
    },
    schema="""
        customerId STRING NOT NULL COMMENT 'Unique customer identifier',
        contentId STRING NOT NULL COMMENT 'Video content identifier being streamed',
        clientId STRING NOT NULL COMMENT 'Client device/app identifier',
        timestamp_server BIGINT NOT NULL COMMENT 'Server-side event timestamp (Unix epoch milliseconds)',
        timestamp_agent BIGINT COMMENT 'Client-side event timestamp (Unix epoch milliseconds), may be null if client clock unavailable',
        buffering_count INT NOT NULL COMMENT 'Number of buffering events during this viewing session',
        buffering_time_ms INT NOT NULL COMMENT 'Total buffering duration in milliseconds',
        buffering_time_sec DOUBLE NOT NULL COMMENT 'Total buffering duration in seconds (derived from buffering_time_ms)',
        quality_level STRING NOT NULL COMMENT 'Video quality level for this row (e.g., 360p, 480p, 720p, 1080p)',
        quality_source_requests INT COMMENT 'Number of CDN source requests for this specific quality level',
        quality_source_data BIGINT COMMENT 'Bytes received from CDN source for this specific quality level',
        quality_p2p_requests INT COMMENT 'Number of P2P requests for this specific quality level',
        quality_p2p_data BIGINT COMMENT 'Bytes received via P2P for this specific quality level',
        quality_data_mb DOUBLE COMMENT 'Total data consumed for this quality level in megabytes (source + P2P)',
        total_source_data BIGINT NOT NULL COMMENT 'Total bytes received from CDN source across all quality levels',
        total_p2p_data BIGINT NOT NULL COMMENT 'Total bytes received via P2P across all quality levels',
        total_data_mb DOUBLE NOT NULL COMMENT 'Total data consumed across all quality levels in megabytes',
        eventDate DATE NOT NULL COMMENT 'Partition key: date of the streaming event (UTC), used for efficient time-based queries'
    """
)
@dp.expect_or_drop("valid_buffering_count", "buffering_count >= 0")
@dp.expect_or_drop("valid_buffering_time", "buffering_time_ms >= 0")
@dp.expect_or_drop("valid_data_volumes", "total_source_data >= 0 AND total_p2p_data >= 0")
@dp.expect_or_drop("valid_quality_level", "quality_level IS NOT NULL")
@dp.expect("reasonable_buffering", "buffering_time_sec <= 3600")
def silver_telemetry_enriched():
    """
    Silver Layer: Enriched Video Telemetry Data
    
    Purpose:
        Transforms raw telemetry by flattening nested JSON structures and exploding
        quality distribution metrics into separate rows per quality level.
        Each output row represents one quality level for one viewer session.
    
    Transformations:
        1. Flatten nested structures (timestampInfo, player, totalDistribution)
        2. Explode qualityDistribution map - one row per quality level per session
        3. Calculate derived metrics:
           - buffering_time_sec (from buffering_time_ms)
           - quality_data_mb (source + P2P data in MB for this quality)
           - total_data_mb (total data across all qualities in MB)
    
    Data Quality:
        Expectations enforce data contract:
        - DROP: Records with negative buffering counts or times
        - DROP: Records with negative data volumes (source or P2P)
        - DROP: Records with null quality levels
        - MONITOR: Records with unreasonable buffering (>1 hour, likely errors)
    
    Schema Contract:
        Explicit schema with column-level comments enforced for downstream stability.
        Breaking changes require schema evolution or new table version.
    
    Performance:
        - Liquid clustering: [eventDate, customerId, clientId]
        - eventDate first enables efficient time-based pruning for queries and late data
        - Supports downstream Materialized View incremental refresh
    
    Upstream Dependencies:
        - bronze_telemetry_raw (streaming table)
    
    Downstream Consumers:
        - gold_viewer_qos_metrics (materialized view - aggregates by customer/client/date)
        - silver_telemetry_quarantine (streaming table - quality issues)
    
    SLA: Near-realtime streaming processing
    Owner: Data Engineering Team
    """
    df = spark.readStream.table("bronze_telemetry_raw")
    
    # Flatten nested structures
    flattened = df.select(
        F.col("customerId"),
        F.col("contentId"),
        F.col("clientId"),
        F.col("timestampInfo.server").alias("timestamp_server"),
        F.col("timestampInfo.agent").alias("timestamp_agent"),
        F.col("player.bufferings").alias("buffering_count"),
        F.col("player.bufferingTime").alias("buffering_time_ms"),
        F.col("totalDistribution.sourceTraffic.requests").alias("total_source_requests"),
        F.col("totalDistribution.sourceTraffic.receivedData").alias("total_source_data"),
        F.col("totalDistribution.p2pTraffic.requests").alias("total_p2p_requests"),
        F.col("totalDistribution.p2pTraffic.receivedData").alias("total_p2p_data"),
        F.col("qualityDistribution"),
        F.col("eventDate")
    )
    
    # Explode quality distribution map to get one row per quality level
    exploded = flattened.select(
        "*",
        F.explode("qualityDistribution").alias("quality_level", "quality_metrics")
    )
    
    # Flatten quality metrics and calculate derived metrics
    enriched = exploded.select(
        F.col("customerId"),
        F.col("contentId"),
        F.col("clientId"),
        F.col("timestamp_server"),
        F.col("timestamp_agent"),
        F.col("buffering_count"),
        F.col("buffering_time_ms"),
        (F.col("buffering_time_ms") / 1000.0).alias("buffering_time_sec"),
        F.col("quality_level"),
        F.col("quality_metrics.sourceTraffic.requests").alias("quality_source_requests"),
        F.col("quality_metrics.sourceTraffic.receivedData").alias("quality_source_data"),
        F.col("quality_metrics.p2pTraffic.requests").alias("quality_p2p_requests"),
        F.col("quality_metrics.p2pTraffic.receivedData").alias("quality_p2p_data"),
        # Calculate total data for this quality level in MB
        ((F.col("quality_metrics.sourceTraffic.receivedData") + 
          F.col("quality_metrics.p2pTraffic.receivedData")) / (1024.0 * 1024.0)).alias("quality_data_mb"),
        # Total data across all qualities
        F.col("total_source_data"),
        F.col("total_p2p_data"),
        ((F.col("total_source_data") + F.col("total_p2p_data")) / (1024.0 * 1024.0)).alias("total_data_mb"),
        F.col("eventDate")
    )
    
    return enriched
