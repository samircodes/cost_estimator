"""Live Azure Retail Prices lookups with hardcoded fallbacks.

Port of notebooks/EDH_Azure_Pricing_Utils.py. Each fetch falls back to a
hardcoded constant on any failure (no network, bad response, no match), so the
estimators always produce a result offline. Results are cached per process so a
form submit doesn't refetch on every call.
"""

import os
from functools import lru_cache

import requests

# Sourced from AZURE_REGION so it can vary per environment; defaults to eastus.
AZURE_REGION = os.environ.get("AZURE_REGION", "eastus")

_PRICING_API = "https://prices.azure.com/api/retail/prices"


def _fetch_price(filter_str, item_filter=None, fallback=0.0):
    try:
        resp = requests.get(_PRICING_API, params={"$filter": filter_str}, timeout=10)
        resp.raise_for_status()
        items = resp.json().get("Items", [])
        if item_filter:
            items = [i for i in items if item_filter(i)]
        if items:
            best = min(items, key=lambda x: float(x.get("retailPrice", 999)))
            return float(best["retailPrice"])
    except Exception:
        pass
    return fallback


@lru_cache(maxsize=None)
def fetch_vm_price(sku_name, fallback, region=AZURE_REGION):
    """Linux on-demand $/hr for an Azure VM SKU (e.g. 'Standard_DS3_v2')."""
    return _fetch_price(
        filter_str=(
            f"serviceName eq 'Virtual Machines'"
            f" and armRegionName eq '{region}'"
            f" and armSkuName eq '{sku_name}'"
            f" and priceType eq 'Consumption'"
        ),
        item_filter=lambda i: (
            "Windows" not in i.get("productName", "")
            and "Spot" not in i.get("skuName", "")
            and "Low Priority" not in i.get("skuName", "")
        ),
        fallback=fallback,
    )


@lru_cache(maxsize=None)
def fetch_adls_storage_price(fallback=0.023, region=AZURE_REGION):
    """ADLS Gen2 LRS hot-tier $/GB/month."""
    return _fetch_price(
        filter_str=(
            f"serviceName eq 'Storage'"
            f" and armRegionName eq '{region}'"
            f" and meterName eq 'LRS Data Stored'"
            f" and priceType eq 'Consumption'"
        ),
        item_filter=lambda i: "Data Lake" in i.get("productName", ""),
        fallback=fallback,
    )


@lru_cache(maxsize=None)
def fetch_egress_price(fallback=0.087, region=AZURE_REGION):
    """Outbound internet egress $/GB (Zone 1)."""
    return _fetch_price(
        filter_str=(
            "serviceName eq 'Bandwidth'"
            " and skuName eq 'Outbound Data Transfer Zone 1'"
            " and priceType eq 'Consumption'"
        ),
        item_filter=lambda i: float(i.get("retailPrice", 0)) > 0,
        fallback=fallback,
    )
