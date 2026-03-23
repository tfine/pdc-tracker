import re

# Agency name normalization map
AGENCY_ALIASES = {
    "DCLA%": "DCLA (Percent for Art)",
    "DCLA": "Dept. of Cultural Affairs",
    "DDC": "Dept. of Design & Construction",
    "DPR": "Dept. of Parks & Recreation",
    "DEP": "Dept. of Environmental Protection",
    "DOT": "Dept. of Transportation",
    "HPD": "Dept. of Housing Preservation & Development",
    "SBS": "Dept. of Small Business Services",
    "SCA": "School Construction Authority",
    "EDC": "Economic Development Corp.",
    "NYCHA": "NYC Housing Authority",
    "DSNY": "Dept. of Sanitation",
    "NYPD": "NYC Police Department",
    "FDNY": "NYC Fire Department",
}

BOROUGH_ALIASES = {
    "MN": "Manhattan",
    "BK": "Brooklyn",
    "BX": "Bronx",
    "QN": "Queens",
    "SI": "Staten Island",
    "Manhattan": "Manhattan",
    "Brooklyn": "Brooklyn",
    "Bronx": "Bronx",
    "Queens": "Queens",
    "Staten Island": "Staten Island",
}


def normalize_title(title: str) -> str:
    """Normalize a project title for comparison."""
    title = re.sub(r"\s+", " ", title).strip()
    # Remove trailing periods
    title = title.rstrip(".")
    return title


def normalize_agency(code: str | None) -> str | None:
    """Expand agency code to full name if known."""
    if not code:
        return None
    code = code.strip()
    return AGENCY_ALIASES.get(code, code)


def normalize_borough(val: str | None) -> str | None:
    """Normalize borough names/abbreviations."""
    if not val:
        return None
    return BOROUGH_ALIASES.get(val.strip(), val.strip())
