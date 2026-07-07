# Databricks notebook source
# EDH Azure Pricing Utility
#
# Fetches live Azure Retail Prices via the public API (no auth required).
# Intended to be included via:  %run ./EDH_Azure_Pricing_Utils
#
# All fetch functions fall back to a hardcoded constant when the API is
# unavailable (network error, no results, bad response, etc.), so the
# estimator notebooks continue to produce results even without internet
# access from the cluster.
#
# Prices NOT covered by the Azure API (remain hardcoded in each notebook):
#   - Databricks DBU price (Databricks-specific, not in Azure Retail API)
#   - ExpressRoute unlimited / VPN gateway (contractual, not published)
#   - Delta transaction overhead

# COMMAND ----------

import requests as _rq

# ── Region configuration ──────────────────────────────────────────────────────
# Update to match your Databricks workspace's Azure region.
# Common values: "eastus", "eastus2", "westeurope", "uksouth"
AZURE_REGION = "eastus"

_PRICING_API = "https://prices.azure.com/api/retail/prices"


# ── Core fetch helper ─────────────────────────────────────────────────────────

def _fetch_azure_price(
    filter_str: str,
    item_filter=None,
    fallback: float = 0.0,
    label: str = "",
) -> float:
    """
    Queries the Azure Retail Prices API with `filter_str`.
    Applies `item_filter` (callable) to narrow the returned items.
    Returns the lowest matching retail price, or `fallback` on any failure.
    """
    try:
        resp = _rq.get(_PRICING_API, params={"$filter": filter_str}, timeout=10)
        resp.raise_for_status()
        items = resp.json().get("Items", [])
        if item_filter:
            items = [i for i in items if item_filter(i)]
        if items:
            best = min(items, key=lambda x: float(x.get("retailPrice", 999)))
            price = float(best["retailPrice"])
            print(f"[Azure Pricing] {label}: ${price:.5f}  (live)")
            return price
        print(f"[Azure Pricing] {label}: no items returned — using fallback ${fallback}")
    except Exception as exc:
        print(f"[Azure Pricing] {label}: API error ({exc!r}) — using fallback ${fallback}")
    return fallback


# ── Public fetch functions ────────────────────────────────────────────────────

def fetch_vm_price(sku_name: str, fallback: float, region: str = AZURE_REGION) -> float:
    """
    Linux on-demand price per hour for an Azure VM SKU.
    sku_name: full ARM SKU name, e.g. 'Standard_DS3_v2', 'Standard_D32ds_v5'
    """
    return _fetch_azure_price(
        filter_str=(
            f"serviceName eq 'Virtual Machines'"
            f" and armRegionName eq '{region}'"
            f" and armSkuName eq '{sku_name}'"
            f" and priceType eq 'Consumption'"
        ),
        item_filter=lambda i: (
            "Windows" not in i.get("productName", "")
            and "Spot"         not in i.get("skuName", "")
            and "Low Priority" not in i.get("skuName", "")
        ),
        fallback=fallback,
        label=f"VM {sku_name} Linux on-demand ({region})",
    )


def fetch_adls_storage_price(fallback: float = 0.023, region: str = AZURE_REGION) -> float:
    """
    Azure Data Lake Storage Gen2 LRS hot-tier storage price per GB per month.
    """
    return _fetch_azure_price(
        filter_str=(
            f"serviceName eq 'Storage'"
            f" and armRegionName eq '{region}'"
            f" and meterName eq 'LRS Data Stored'"
            f" and priceType eq 'Consumption'"
        ),
        item_filter=lambda i: "Data Lake" in i.get("productName", ""),
        fallback=fallback,
        label=f"ADLS Gen2 LRS data stored ({region})",
    )


def fetch_egress_price(fallback: float = 0.087, region: str = AZURE_REGION) -> float:
    """
    Azure outbound internet egress price per GB (Zone 1 — Americas / Europe).
    Note: bandwidth pricing in the Azure API is not region-scoped the same way
    as compute, so we search globally and take the Zone 1 entry.
    """
    return _fetch_azure_price(
        filter_str=(
            "serviceName eq 'Bandwidth'"
            " and skuName eq 'Outbound Data Transfer Zone 1'"
            " and priceType eq 'Consumption'"
        ),
        # Exclude the free-tier $0 row (first 5 GB/month)
        item_filter=lambda i: float(i.get("retailPrice", 0)) > 0,
        fallback=fallback,
        label="Internet egress Zone 1",
    )


# COMMAND ----------

print(
    "[Azure Pricing] Utility ready.\n"
    "  fetch_vm_price(sku_name, fallback)     — Linux VM on-demand $/hr\n"
    "  fetch_adls_storage_price(fallback)     — ADLS Gen2 LRS $/GB/month\n"
    "  fetch_egress_price(fallback)           — Outbound internet $/GB\n"
    f"  Region: {AZURE_REGION}  (change AZURE_REGION to match your workspace)"
)
