# yesg/main.py
import calendar
import json
import random
import time
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from curl_cffi import requests


@dataclass
class YahooESGClient:
    impersonate: str = "chrome123"
    timeout: int = 30
    # backoff + retry tuning
    max_retries: int = 4              # total attempts for 429/5xx (not counting the one-time reauth retry)
    backoff_base: float = 0.8         # seconds, exponential base
    backoff_cap: float = 8.0          # max sleep seconds
    jitter_frac: float = 0.25         # ±25% jitter

    def __post_init__(self):
        self.session = requests.Session(impersonate=self.impersonate)
        self._cookie: Optional[str] = None   # "A1=..."
        self._crumb: Optional[str] = None

    # ---------- low-level auth helpers ----------
    def _reauth(self, reset_session: bool = True) -> None:
        """Forget auth state and reacquire cookie + crumb."""
        try:
            self.session.close()
        except Exception:
            pass
        if reset_session:
            self.session = requests.Session(impersonate=self.impersonate)
        self._cookie = None
        self._crumb = None
        self._ensure_cookie()
        self._ensure_crumb()

    def _ensure_cookie(self) -> str:
        if self._cookie:
            return self._cookie
        url = (
            "https://query1.finance.yahoo.com/v7/finance/quote"
            "?fields=regularMarketChangePercent%2CregularMarketTime%2CregularMarketChange%2CregularMarketPrice%2CregularMarketVolume"
            "&formatted=true&imgHeights=50&imgLabels=logoUrl&imgWidths=50"
            "&symbols=WMT%2CTGT%2CHD%2CX%2CNISTF%2C%5ESPX%2CGM%2CMBGAF%2CACDVF%2CMDLZ%2CPEP%2CGIS%2CFLYY%2CLOW%2CBTC-USD%2CKODK"
            "&enablePrivateCompany=true&overnightPrice=true&lang=en-US&region=US"
        )
        r = self.session.get(url, timeout=self.timeout)
        a1 = r.cookies.get("A1") or self.session.cookies.get("A1")
        if not a1:
            raise RuntimeError("Failed to obtain Yahoo A1 cookie")
        self._cookie = "A1=" + a1
        return self._cookie

    def _default_headers(self) -> dict:
        cookie = self._ensure_cookie()
        return {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/110.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://finance.yahoo.com/",
            "Origin": "https://finance.yahoo.com",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Cookie": cookie,
        }

    def _ensure_crumb(self) -> str:
        if self._crumb:
            return self._crumb
        r = self.session.get(
            "https://query2.finance.yahoo.com/v1/test/getcrumb",
            headers=self._default_headers(),
            timeout=self.timeout,
        )
        r.raise_for_status()
        self._crumb = r.text.strip()
        if not self._crumb:
            raise RuntimeError("Failed to obtain Yahoo crumb")
        return self._crumb

    # ---------- utility ----------
    def _sleep_backoff(self, attempt: int):
        """Exponential backoff with jitter: attempt is 0-based."""
        base = min(self.backoff_cap, self.backoff_base * (2 ** attempt))
        # jitter in ±jitter_frac
        jitter = base * self.jitter_frac
        delay = max(0.0, base + random.uniform(-jitter, jitter))
        time.sleep(delay)

    # ---------- generic request with auto reauth + backoff ----------
    def _request(self, method: str, url: str, *, need_crumb: bool = False,
                 params: Optional[dict] = None, headers: Optional[dict] = None, **kwargs):
        """
        Send a request with:
          - crumb injection if needed
          - one-time reauth+retry on 401/403 or 'invalid crumb'
          - exponential backoff retries for 429 and 5xx
        """
        if headers is None:
            headers = self._default_headers()

        def _do(params_override=None):
            p = dict(params or {})
            if params_override:
                p.update(params_override)
            if need_crumb:
                self._ensure_crumb()
                p.setdefault("crumb", self._crumb)
            return self.session.request(
                method, url, params=p, headers=headers, timeout=self.timeout, **kwargs
            )

        # First try
        resp = _do()

        def _crumb_invalid(r):
            try:
                body = r.text.lower()
                return ("crumb" in body and "invalid" in body) or ("unauthoriz" in body)
            except Exception:
                return False

        # One-time reauth on 401/403 or invalid-crumb-as-400
        if resp.status_code in (401, 403) or (resp.status_code == 400 and _crumb_invalid(resp)):
            self._reauth(reset_session=True)
            headers = self._default_headers()
            resp = _do({"crumb": self._crumb} if need_crumb else None)

        # Backoff loop for 429 + 5xx
        attempt = 0
        while (resp.status_code == 429 or 500 <= resp.status_code < 600) and attempt < self.max_retries:
            self._sleep_backoff(attempt)
            attempt += 1
            resp = _do()

        resp.raise_for_status()
        return resp

    # ---------- public API ----------
    def get_esg_short(self, ticker: str) -> pd.DataFrame:
        data = self._fetch_quote_summary_esg(ticker)
        totalScore = data.get("totalEsg", {}).get("fmt", "-")
        EScore = data.get("environmentScore", {}).get("fmt", "-")
        SScore = data.get("socialScore", {}).get("fmt", "-")
        GScore = data.get("governanceScore", {}).get("fmt", "-")

        date = "-"
        if "ratingYear" in data:
            try:
                month = calendar.month_abbr[data.get("ratingMonth")]
                date = f"{month} {data['ratingYear']}"
            except Exception:
                date = str(data.get("ratingYear", "-"))

        return pd.DataFrame(
            {
                "Ticker": ticker,
                "Total-Score": totalScore,
                "E-Score": EScore,
                "S-Score": SScore,
                "G-Score": GScore,  # fixed
                "Last Rated": date,
            },
            index=[0],
        )

    def get_esg_full(self, ticker: str) -> pd.DataFrame:
        data = self._fetch_quote_summary_esg(ticker)

        def g(path, default="-"):
            cur = data
            for k in path.split("."):
                if not isinstance(cur, dict) or k not in cur:
                    return default
                cur = cur[k]
            return cur

        totalScore = g("totalEsg.fmt")
        EScore = g("environmentScore.fmt")
        SScore = g("socialScore.fmt")
        GScore = g("governanceScore.fmt")

        date = "-"
        if "ratingYear" in data:
            try:
                month = calendar.month_abbr[data.get("ratingMonth")]
                date = f"{month} {data['ratingYear']}"
            except Exception:
                date = str(data.get("ratingYear", "-"))

        relatedControversy = (
            ",".join(g("relatedControversy", []))
            if isinstance(g("relatedControversy", []), list)
            else g("relatedControversy", "-")
        )

        flags = [
            ("adult", "Adult Entertainment"),
            ("alcoholic", "Alcoholic Beverages"),
            ("animalTesting", "Animal Testing"),
            ("catholic", "Catholic Values"),
            ("controversialWeapons", "Controversial Weapons"),
            ("smallArms", "Small Arms"),
            ("furLeather", "Fur and Specialty Leather"),
            ("gambling", "Gambling"),
            ("gmo", "GMO"),
            ("militaryContract", "Military Contracting"),
            ("nuclear", "Nuclear"),
            ("pesticides", "Pesticides"),
            ("palmOil", "Palm Oil"),
            ("coal", "Thermal Coal"),
            ("tobacco", "Tobacco Products"),
        ]
        controversial_areas = ", ".join(name for key, name in flags if data.get(key)) or "-"

        row = {
            "Ticker": ticker,
            "Total-Score": totalScore,
            "E-Score": EScore,
            "S-Score": SScore,
            "G-Score": GScore,
            "Last Rated": date,
            "ESG Performance": g("esgPerformance"),
            "peer Group": g("peerGroup"),
            "Highest Controversy": g("highestControversy"),
            "peer Count": g("peerCount"),
            "total Percentile": g("percentile.raw"),
            "environment Percentile": g("environmentPercentile"),
            "social Percentile": g("socialPercentile"),
            "governance Percentile": g("governancePercentile"),
            "related Controversy": relatedControversy,
            "min peer ESG": g("peerEsgScorePerformance.min"),
            "avg peer ESG": g("peerEsgScorePerformance.avg"),
            "max peer ESG": g("peerEsgScorePerformance.max"),
            "min peer Environment": g("peerEnvironmentPerformance.min"),
            "avg peer Environment": g("peerEnvironmentPerformance.avg"),
            "max peer Environment": g("peerEnvironmentPerformance.max"),
            "min peer Social": g("peerSocialPerformance.min"),
            "avg peer Social": g("peerSocialPerformance.avg"),
            "max peer Social": g("peerSocialPerformance.max"),
            "min peer Governance": g("peerGovernancePerformance.min"),
            "avg peer Governance": g("peerGovernancePerformance.avg"),
            "max peer Governance": g("peerGovernancePerformance.max"),
            "min Highest Controversy": g("peerHighestControversyPerformance.min"),
            "avg Highest Controversy": g("peerHighestControversyPerformance.avg"),
            "max Highest Controversy": g("peerHighestControversyPerformance.max"),
            "Controversial Business Areas": controversial_areas,
        }
        return pd.DataFrame([row])

    def get_historic_esg(self, ticker: str) -> pd.DataFrame:
        r = self._request(
            "GET",
            "https://query2.finance.yahoo.com/v1/finance/esgChart",
            need_crumb=True,
            params={"symbol": ticker},
            headers=self._default_headers(),
        )
        js = r.json()
        try:
            series = js["esgChart"]["result"][0]["symbolSeries"]
        except Exception:
            raise RuntimeError(f"No ESG history found for ticker '{ticker}'")
        df = pd.DataFrame(series)
        df["Date"] = pd.to_datetime(df["timestamp"], unit="s")
        df = df.rename(
            columns={
                "esgScore": "Total-Score",
                "environmentScore": "E-Score",
                "socialScore": "S-Score",
                "governanceScore": "G-Score",
            }
        )
        return df[["Date", "Total-Score", "E-Score", "S-Score", "G-Score"]].set_index("Date")

    # ---------- internal fetch ----------
    def _fetch_quote_summary_esg(self, ticker: str) -> dict:
        r = self._request(
            "GET",
            f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}",
            need_crumb=True,
            params={"modules": "esgScores"},
            headers=self._default_headers(),
        )
        data = json.loads(r.text)
        try:
            return data["quoteSummary"]["result"][0]["esgScores"]
        except Exception:
            raise RuntimeError(f"No ESG scores available for ticker '{ticker}'")