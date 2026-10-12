"""Deadlock detection over resource-allocation logs using Apache Spark."""

from .detector import Report, create_spark, detect_deadlocks

__all__ = ["Report", "create_spark", "detect_deadlocks"]
