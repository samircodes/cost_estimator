"""In-app cost/effort estimators.

These modules are the pure-Python port of the former Databricks estimator
notebooks (notebooks/EDH_*_Estimator_Job.py). The app now computes estimates
directly by calling estimate() here instead of triggering a Databricks job,
and databricks_client persists the returned rows to the same Delta tables.

The notebooks are kept in the repo as reference; this package is the live
implementation the app uses.
"""
