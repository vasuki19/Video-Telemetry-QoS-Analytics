from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Quarantined records with quality issues for investigation and remediation",
    partition_cols=["eventDate", "quarantine_reason"],
    table_properties={
        "quality": "silver",
        "domain": "video_analytics",
        "owner": "data-engineering-team",
        "contains_pii": "false",
        "refresh_frequency": "streaming",
        "purpose": "data_quality_monitoring",
        "alert_threshold": "1000_records_per_day"
    }
)
@dp.expect("has_quarantine_reason", "quarantine_reason IS NOT NULL")
def silver_telemetry_quarantine():
    """
    Silver Layer: Data Quality Quarantine
    
    Purpose:
        Captures and categorizes records that fail data quality checks for investigation
        and remediation. Enables root cause analysis of data quality issues at source.
    
    Schema Strategy:
        Explicit schema NOT enforced to allow flexibility in capturing unexpected quality issues.
        Schema inferred from query output - evolves as new issue types are discovered.
    
    Output Columns:
        - customerId: Customer identifier from failed record
        - contentId: Video content identifier from failed record
        - clientId: Client device/app identifier from failed record
        - timestamp_server: Server-side event timestamp (Unix epoch ms)
        - timestamp_agent: Client-side event timestamp (Unix epoch ms)
        - buffering_count: Number of buffering events (may be invalid)
        - buffering_time_ms: Buffering duration in ms (may be invalid)
        - total_source_requests: CDN source requests
        - total_source_data: Bytes from CDN source (may be invalid)
        - total_p2p_requests: P2P requests
        - total_p2p_data: Bytes via P2P (may be invalid)
        - qualityDistribution: Original quality distribution map
        - eventDate: Date of event (partition key)
        - quarantine_timestamp: When record was quarantined
        - quality_level: Video quality level (may be null)
        - quality_source_requests: CDN requests for this quality
        - quality_source_data: Bytes from CDN for this quality
        - quality_p2p_requests: P2P requests for this quality
        - quality_p2p_data: Bytes via P2P for this quality
        - buffering_time_sec: Buffering duration in seconds
        - quarantine_reason: Categorized failure reason (partition key)
    
    Quarantine Reasons:
        1. negative_buffering_count - Buffering count < 0 (invalid metric)
        2. negative_buffering_time - Buffering time < 0 (invalid metric)
        3. negative_source_data - Source data volume < 0 (invalid metric)
        4. negative_p2p_data - P2P data volume < 0 (invalid metric)
        5. null_quality_level - Quality level is null (required field missing)
        6. excessive_buffering - Buffering time > 1 hour (likely error/outlier)
        7. other - Any other quality issue
    
    Processing Logic:
        1. Read from bronze_telemetry_raw (same source as enriched table)
        2. Flatten nested structures (timestampInfo, player, totalDistribution)
        3. Explode qualityDistribution map (one row per quality level)
        4. Categorize quality issues with quarantine_reason
        5. Filter to keep ONLY failed records
        6. Add quarantine_timestamp for tracking
    
    Partitioning Strategy:
        - eventDate: Enables time-based analysis of quality trends
        - quarantine_reason: Groups failures by type for targeted investigation
    
    Data Retention:
        Quarantined records stored indefinitely for audit and trend analysis.
        Consider implementing retention policy after 90+ days if volume grows large.
    
    Monitoring and Alerts:
        - Monitor count by quarantine_reason daily
        - Alert if any reason exceeds 1000 records/day (configurable threshold)
        - Track trends: increasing failure rates indicate upstream issues
    
    Investigation Workflow:
        1. Query by quarantine_reason to identify failure patterns
        2. Join with source system logs using customerId/contentId/timestamp
        3. Coordinate with upstream team to fix data quality at source
        4. Validate fix by monitoring quarantine volume reduction
    
    Example Queries:
        -- Daily failure summary
        SELECT eventDate, quarantine_reason, COUNT(*) as failure_count
        FROM silver_telemetry_quarantine
        WHERE eventDate >= CURRENT_DATE - INTERVAL 7 DAYS
        GROUP BY eventDate, quarantine_reason
        ORDER BY eventDate DESC, failure_count DESC;
        
        -- Top customers with quality issues
        SELECT customerId, quarantine_reason, COUNT(*) as issue_count
        FROM silver_telemetry_quarantine
        WHERE eventDate >= CURRENT_DATE - INTERVAL 1 DAY
        GROUP BY customerId, quarantine_reason
        ORDER BY issue_count DESC
        LIMIT 100;
    
    Upstream Dependencies:
        - bronze_telemetry_raw (streaming table)
    
    Downstream Consumers:
        - Data quality dashboards
        - Alert systems
        - Source system remediation workflows
    
    Performance:
        - Traditional partitioning (not liquid clustering) for fixed partition pruning
        - Partitioned by eventDate and quarantine_reason for efficient queries
        - Streaming ingestion with checkpointing
    
    SLA: Near-realtime quarantine detection (<5 minutes from ingestion)
    Owner: Data Engineering Team
    Contact: For high quarantine volumes, contact data-engineering-team for investigation
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
        F.col("eventDate"),
        F.current_timestamp().alias("quarantine_timestamp")
    )
    
    # Explode quality distribution
    exploded = flattened.select(
        "*",
        F.explode("qualityDistribution").alias("quality_level", "quality_metrics")
    )
    
    # Identify bad records with reasons
    quarantined = exploded.select(
        "*",
        F.col("quality_metrics.sourceTraffic.requests").alias("quality_source_requests"),
        F.col("quality_metrics.sourceTraffic.receivedData").alias("quality_source_data"),
        F.col("quality_metrics.p2pTraffic.requests").alias("quality_p2p_requests"),
        F.col("quality_metrics.p2pTraffic.receivedData").alias("quality_p2p_data"),
        (F.col("buffering_time_ms") / 1000.0).alias("buffering_time_sec"),
        # Categorize quality issues
        F.when(F.col("buffering_count") < 0, "negative_buffering_count")
         .when(F.col("buffering_time_ms") < 0, "negative_buffering_time")
         .when(F.col("total_source_data") < 0, "negative_source_data")
         .when(F.col("total_p2p_data") < 0, "negative_p2p_data")
         .when(F.col("quality_level").isNull(), "null_quality_level")
         .when(F.col("buffering_time_ms") / 1000.0 > 3600, "excessive_buffering")
         .otherwise("other").alias("quarantine_reason")
    )
    
    # Filter only bad records
    bad_records = quarantined.filter(
        (F.col("buffering_count") < 0) |
        (F.col("buffering_time_ms") < 0) |
        (F.col("total_source_data") < 0) |
        (F.col("total_p2p_data") < 0) |
        (F.col("quality_level").isNull()) |
        (F.col("buffering_time_ms") / 1000.0 > 3600)
    )
    
    return bad_records
