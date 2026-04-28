import re
import statistics
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup
from cachetools import TTLCache


FX_KEYS = [
    "bcv",
    "monitor",
    "compuesto",
    "binance",
    "usdt",
    "dolartoday",
]

DOLARAPI_BASE = "https://ve.dolarapi.com/v1"
BCV_URL = "https://www.bcv.org.ve/seccionportal/tipo-de-cambio-oficial-del-bcv"

BINANCE_P2P_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
BYBIT_P2P_URL = "https://api2.bybit.com/fiat/otc/item/online"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json,text/html,*/*",
}

bcv_cache = TTLCache(maxsize=10, ttl=30 * 60)
p2p_cache = TTLCache(maxsize=10, ttl=2 * 60)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_float(value) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    text = text.replace("\xa0", " ")
    text = text.replace("Bs.", "")
    text = text.replace("Bs", "")
    text = text.replace("VES", "")
    text = text.replace(" ", "")

    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")

    match = re.search(r"\d+(\.\d+)?", text)
    if not match:
        return None

    return float(match.group(0))


def average_available(values: List[Optional[float]]) -> Optional[float]:
    nums = [float(v) for v in values if v is not None and v > 0]
    if not nums:
        return None
    return round(sum(nums) / len(nums), 6)


def median_first_prices(prices: List[float], n: int = 5) -> Optional[float]:
    clean = [float(p) for p in prices if p is not None and p > 0]
    if not clean:
        return None
    return round(statistics.median(clean[:n]), 6)


def fetch_dolarapi_bcv_usd() -> Optional[float]:
    url = f"{DOLARAPI_BASE}/dolares/oficial"
    r = requests.get(url, headers=HEADERS, timeout=12)
    r.raise_for_status()
    data = r.json()
    return to_float(data.get("promedio") or data.get("venta") or data.get("compra"))


def fetch_dolarapi_bcv_eur() -> Optional[float]:
    url = f"{DOLARAPI_BASE}/euros/oficial"
    r = requests.get(url, headers=HEADERS, timeout=12)
    r.raise_for_status()
    data = r.json()
    return to_float(data.get("promedio") or data.get("venta") or data.get("compra"))


def fetch_bcv_direct_scraping() -> Dict[str, Optional[float]]:
    r = requests.get(BCV_URL, headers=HEADERS, timeout=15, verify=False)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")
    page_text = soup.get_text(" ", strip=True)

    def extract(kind: str) -> Optional[float]:
        selectors = {
            "usd": ["#dolar", "div[id*=dolar]", "span[id*=dolar]"],
            "eur": ["#euro", "div[id*=euro]", "span[id*=euro]"],
        }

        for selector in selectors[kind]:
            node = soup.select_one(selector)
            if node:
                value = to_float(node.get_text(" ", strip=True))
                if value:
                    return value

        pattern = r"(USD|Dólar|Dolar).*?(\d+[.,]\d+)" if kind == "usd" else r"(EUR|Euro).*?(\d+[.,]\d+)"
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            return to_float(match.group(2))

        return None

    return {
        "bcv": extract("usd"),
        "dolartoday": extract("eur"),
    }


def get_bcv_values() -> Dict[str, Optional[float]]:
    cache_key = "bcv_values"

    if cache_key in bcv_cache:
        return bcv_cache[cache_key]

    bcv = None
    euro = None

    try:
        bcv = fetch_dolarapi_bcv_usd()
        euro = fetch_dolarapi_bcv_eur()
    except Exception:
        try:
            scraped = fetch_bcv_direct_scraping()
            bcv = scraped.get("bcv")
            euro = scraped.get("dolartoday")
        except Exception:
            pass

    result = {
        "bcv": bcv,
        "dolartoday": euro,
    }

    bcv_cache[cache_key] = result
    return result


def fetch_binance_p2p_usdt_ves() -> Optional[float]:
    payload = {
        "asset": "USDT",
        "fiat": "VES",
        "tradeType": "SELL",
        "page": 1,
        "rows": 10,
        "payTypes": [],
        "publisherType": None,
    }

    r = requests.post(
        BINANCE_P2P_URL,
        json=payload,
        headers=HEADERS,
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()

    prices = []
    for item in data.get("data", []):
        adv = item.get("adv", {})
        price = to_float(adv.get("price"))
        if price:
            prices.append(price)

    return median_first_prices(prices, n=5)


def fetch_bybit_p2p_usdt_ves() -> Optional[float]:
    payloads = [
        {
            "userId": "",
            "tokenId": "USDT",
            "currencyId": "VES",
            "payment": [],
            "side": "1",
            "size": "10",
            "page": "1",
            "amount": "",
            "authMaker": False,
            "canTrade": False,
        },
        {
            "userId": "",
            "tokenId": "USDT",
            "currencyId": "VES",
            "payment": [],
            "side": "0",
            "size": "10",
            "page": "1",
            "amount": "",
            "authMaker": False,
            "canTrade": False,
        },
    ]

    for payload in payloads:
        try:
            r = requests.post(
                BYBIT_P2P_URL,
                json=payload,
                headers=HEADERS,
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()

            result = data.get("result") or {}
            items = result.get("items") or result.get("list") or []

            prices = []
            for item in items:
                price = to_float(item.get("price") or item.get("fixedPrice"))
                if price:
                    prices.append(price)

            value = median_first_prices(prices, n=5)
            if value:
                return value
        except Exception:
            continue

    return None


def get_p2p_values() -> Dict[str, Optional[float]]:
    cache_key = "p2p_values"

    if cache_key in p2p_cache:
        return p2p_cache[cache_key]

    binance = None
    bybit = None

    try:
        binance = fetch_binance_p2p_usdt_ves()
    except Exception:
        binance = None

    try:
        bybit = fetch_bybit_p2p_usdt_ves()
    except Exception:
        bybit = None

    result = {
        "binance": binance,
        "usdt": bybit,
    }

    p2p_cache[cache_key] = result
    return result


def build_fx_snapshot() -> Dict[str, Any]:
    bcv_values = get_bcv_values()
    p2p_values = get_p2p_values()

    bcv = bcv_values.get("bcv")
    binance = p2p_values.get("binance")
    bybit = p2p_values.get("usdt")
    euro = bcv_values.get("dolartoday")

    monitor = average_available([bcv, binance, bybit])

    return {
        "timestamp_utc": now_iso(),
        "sources": {
            "bcv": {"value": bcv},
            "monitor": {"value": monitor},
            "compuesto": {"value": None},
            "binance": {"value": binance},
            "usdt": {"value": bybit},
            "dolartoday": {"value": euro},
        },
    }


def build_flat_fx_values() -> Dict[str, Optional[float]]:
    snapshot = build_fx_snapshot()

    return {
        "bcv": snapshot["sources"]["bcv"]["value"],
        "monitor": snapshot["sources"]["monitor"]["value"],
        "compuesto": snapshot["sources"]["compuesto"]["value"],
        "binance": snapshot["sources"]["binance"]["value"],
        "usdt": snapshot["sources"]["usdt"]["value"],
        "dolartoday": snapshot["sources"]["dolartoday"]["value"],
    }


if __name__ == "__main__":
    print(build_flat_fx_values())