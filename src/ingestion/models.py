from dataclasses import dataclass


@dataclass(frozen=True)
class IngestionUnit:
    """Smallest independently runnable MaxPreps ingestion unit."""

    ingestion_type: str
    state: str
    sport: str
    season: str
    boys: bool = True

    VALID_TYPES = frozenset({"contests", "rankings", "districts"})

    def __post_init__(self):
        if self.ingestion_type not in self.VALID_TYPES:
            raise ValueError(f"Unsupported ingestion type: {self.ingestion_type}")
        if len(self.state) != 2 or not self.state.isalpha():
            raise ValueError("state must be a two-letter abbreviation")
        if not self.sport:
            raise ValueError("sport must not be empty")
        if not self.season:
            raise ValueError("season must not be empty")

    @property
    def gender(self):
        return "boys" if self.boys else "girls"

    @property
    def unit_id(self):
        return ":".join(
            (self.ingestion_type, self.state, self.sport, self.gender, self.season)
        )

    def as_metadata(self):
        return {
            "INGESTION_TYPE": self.ingestion_type,
            "STATE": self.state,
            "SPORT": self.sport,
            "GENDER": self.gender,
            "SEASON": self.season,
        }