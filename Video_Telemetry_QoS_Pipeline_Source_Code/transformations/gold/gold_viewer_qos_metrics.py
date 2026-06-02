from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Viewer-level QoS metrics aggregated by customer and client with automatic incremental refresh",
    cluster_by=["eventDate", "customerId", "clientId"],
    table_properties={
        "quality": "gold",
        "domain": "video_analytics",
        "owner": "data-engineering-team",
        "contains_pii": "false",
        "refresh_frequency": "incremental",
        "sla": "hourly",
        "aggregation_grain": "customer_client_date"
    },
    schema="""
        customerId STRING NOT NULL COMMENT 'Unique customer identifier',
        clientId STRING NOT NULL COMMENT 'Client device/app identifier',
        eventDate DATE NOT NULL COMMENT 'Date of viewing activity (UTC), used for time-based analysis',
        total_buffering_events BIGINT COMMENT 'Total count of buffering events across all sessions for this customer/client/date',
        total_buffering_time_sec DOUBLE COMMENT 'Total buffering duration in seconds across all sessions',
        avg_buffering_per_session DOUBLE COMMENT 'Average number of buffering events per session',
        avg_buffering_time_sec DOUBLE COMMENT 'Average buffering duration per session in seconds',
        total_data_consumed_mb DOUBLE COMMENT 'Total data consumed in megabytes across all sessions',
        total_source_data_bytes BIGINT COMMENT 'Total bytes received from CDN source across all sessions',
        total_p2p_data_bytes BIGINT COMMENT 'Total bytes received via P2P across all sessions',
        unique_content_count BIGINT COMMENT 'Approximate count of distinct content items viewed',
        total_sessions BIGINT NOT NULL COMMENT 'Total number of viewing sessions aggregated',
        source_traffic_percentage DOUBLE COMMENT 'Percentage of traffic from CDN source (vs P2P), range 0-100',
        buffering_ratio_percentage DOUBLE COMMENT 'Buffering time normalized by data consumption, higher is worse QoS',
        qos_score DOUBLE COMMENT 'Quality of Service score (0-100), higher is better, calculated from buffering metrics',
        qos_category STRING COMMENT 'QoS quality category: Excellent (>=80), Good (>=60), Fair (>=40), Poor (<40)'
    """
)
def gold_viewer_qos_metrics():
    """
    Gold Layer: Viewer Quality of Service (QoS) Metrics
    
    Purpose:
        Aggregates video streaming telemetry to viewer level (customer + client + date)
        to provide actionable QoS insights for monitoring, alerting, and analytics.
    
    Aggregation Logic:
        Groups by: customerId, clientId, eventDate
        
        Metrics calculated:
        - Buffering: Total events, total time, averages per session
        - Data consumption: Total MB consumed, source vs P2P breakdown
        - Content diversity: Unique content count (approximate)
        - Session counts: Total viewing sessions
        
        Derived KPIs:
        - source_traffic_percentage: % of traffic from CDN (vs P2P)
        - buffering_ratio_percentage: Buffering normalized by data consumption
        - qos_score: Composite score (0-100) based on buffering impact
        - qos_category: Human-readable quality tier
    
    QoS Score Formula:
        score = max(0, 100 - (total_buffering_time_sec / 10) - (total_buffering_events * 2))
        
        Interpretation:
        - Excellent (>=80): Minimal buffering, great user experience
        - Good (>=60): Acceptable buffering, satisfactory experience
        - Fair (>=40): Noticeable buffering, degraded experience
        - Poor (<40): Severe buffering, poor user experience
    
    Materialized View Benefits:
        - Handles late data of any age (no watermark limit)
        - Automatically recomputes affected aggregates when upstream data changes
        - Incremental refresh on serverless: only processes new/changed date clusters
        - Full historical aggregates maintained with correct values
        - No streaming state overhead
    
    Performance:
        - Liquid clustering: [eventDate, customerId, clientId]
        - eventDate first enables efficient time-range queries
        - Incremental refresh processes only changed data from silver layer
        - Typical refresh time: 2-5 minutes for daily data
    
    Data Quality:
        - All aggregates are null-safe (handle missing values gracefully)
        - Divisions protected with WHEN clauses to avoid divide-by-zero
        - approx_count_distinct for efficiency (exact counts not required)
    
    Upstream Dependencies:
        - silver_telemetry_enriched (streaming table)
    
    Downstream Consumers:
        - Dashboards: Customer QoS monitoring, SLA tracking
        - Alerts: QoS degradation detection
        - Reports: Executive summaries, trend analysis
    
    SLA: Hourly refresh with automatic late data correction
    Owner: Data Engineering Team
    """
    # Batch read - incremental refresh handled automatically by Delta
    df = spark.read.table("silver_telemetry_enriched")

    # No watermark needed - MV handles late data correctly
    viewer_metrics = df.groupBy("customerId", "clientId", "eventDate") \
        .agg(
            F.sum("buffering_count").alias("total_buffering_events"),
            F.sum("buffering_time_sec").alias("total_buffering_time_sec"),
            F.avg("buffering_count").alias("avg_buffering_per_session"),
            F.avg("buffering_time_sec").alias("avg_buffering_time_sec"),
            F.sum("total_data_mb").alias("total_data_consumed_mb"),
            F.sum("total_source_data").alias("total_source_data_bytes"),
            F.sum("total_p2p_data").alias("total_p2p_data_bytes"),
            F.approx_count_distinct("contentId").alias("unique_content_count"),
            F.count("*").alias("total_sessions")
        )

    # Calculate traffic distribution percentage
    final_metrics = viewer_metrics.select(
        "*",
        F.when((F.col("total_source_data_bytes") + F.col("total_p2p_data_bytes")) > 0, 
               F.col("total_source_data_bytes") / (F.col("total_source_data_bytes") + F.col("total_p2p_data_bytes")) * 100
        ).otherwise(0).alias("source_traffic_percentage"),
        F.when(F.col("total_data_consumed_mb") > 0, 
               (F.col("total_buffering_time_sec") / (F.col("total_data_consumed_mb") * 2)) * 100
        ).otherwise(0).alias("buffering_ratio_percentage")
    )

    # Calculate composite QoS score
    qos_scored = final_metrics.select(
        "*",
        F.greatest(
            F.lit(0),
            F.lit(100) - (F.col("total_buffering_time_sec") / 10) - (F.col("total_buffering_events") * 2)
        ).alias("qos_score")
    )

    # Categorize QoS quality
    final = qos_scored.select(
        "*",
        F.when(F.col("qos_score") >= 80, "Excellent")
         .when(F.col("qos_score") >= 60, "Good")
         .when(F.col("qos_score") >= 40, "Fair")
         .otherwise("Poor").alias("qos_category")
    )

    return final
