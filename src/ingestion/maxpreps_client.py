import os
import sys
import time

import requests


def _load_scraper_class():
    package_path = os.path.expanduser("~/Documents/maxpreps_scraper")
    if package_path not in sys.path:
        sys.path.insert(0, package_path)
    from maxpreps_scraper.scraper import MaxPrepsScraper

    return MaxPrepsScraper


class EmptyScrapeResultError(RuntimeError):
    """Raised when MaxPreps returns no rows for an ingestion unit."""


class IncompleteScrapeError(RuntimeError):
    """Raised when a contests scrape drops too many schools to trust as complete."""


class MaxPrepsClient:
    """Small retrying boundary around the existing scraper package."""

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 Chrome/131.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    def __init__(
        self,
        scraper=None,
        max_retries=4,
        initial_backoff_seconds=2.0,
        request_delay_seconds=1.0,
        requests_per_second=None,
        min_completeness_ratio=0.9,
        school_retry_attempts=0,
        school_retry_delay_seconds=5.0,
        sleeper=time.sleep,
    ):
        self.scraper = scraper or _load_scraper_class()(
            requests_per_second=requests_per_second,
            # Reuse the same knobs that drive our own outer whole-call retry
            # (below) to also configure the scraper's per-HTTP-request retry —
            # same exponential-backoff shape, same units (seconds), one config
            # surface instead of two.
            max_retries=max_retries,
            backoff_factor=initial_backoff_seconds,
            school_retry_attempts=school_retry_attempts,
            school_retry_delay_seconds=school_retry_delay_seconds,
        )
        self.max_retries = max_retries
        self.initial_backoff_seconds = initial_backoff_seconds
        self.request_delay_seconds = request_delay_seconds
        self.min_completeness_ratio = min_completeness_ratio
        self.sleeper = sleeper
        if hasattr(self.scraper, "session"):
            self.scraper.session.headers.update(self.DEFAULT_HEADERS)

    def fetch(self, ingestion_type, state, sport, season, boys=True, cities=None):
        method = getattr(self.scraper, f"get_{ingestion_type}")
        arguments = {"state": state, "sport": sport, "year": season, "boys": boys}
        if ingestion_type == "contests":
            arguments["cities"] = cities

        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                result = method(**arguments)
                if ingestion_type == "contests":
                    # Check completeness before the raw emptiness check below:
                    # schools_discovered > 0 but schools_scraped == 0 means
                    # every school was blocked/failed (e.g. a full IP block),
                    # which is a transient-ish, retry-worth condition — not the
                    # same as genuinely finding zero schools for this
                    # state/season, which is not worth retrying.
                    discovered = result.attrs.get("schools_discovered")
                    scraped = result.attrs.get("schools_scraped")
                    if discovered and scraped / discovered < self.min_completeness_ratio:
                        raise IncompleteScrapeError(
                            f"Only {scraped}/{discovered} schools scraped for "
                            f"contests {state}/{sport}/{season} "
                            f"({scraped / discovered:.0%})"
                        )
                if result.empty:
                    raise EmptyScrapeResultError(
                        f"MaxPreps returned no rows for {ingestion_type} "
                        f"{state}/{sport}/{season}"
                    )
                return result
            except (requests.RequestException, TimeoutError, ConnectionError, IncompleteScrapeError) as error:
                last_error = error
                if attempt == self.max_retries:
                    break
                self.sleeper(self.initial_backoff_seconds * (2 ** (attempt - 1)))
        raise RuntimeError(
            f"MaxPreps {ingestion_type} failed after {self.max_retries} attempts"
        ) from last_error