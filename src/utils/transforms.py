"""Reusable column transformations. No pipeline dependencies — unit testable."""
from pyspark.sql.functions import col, lower, trim, regexp_replace, split, element_at

BR_STATES = ("ac|al|ap|am|ba|ce|df|es|go|ma|mt|ms|mg|pa|pb|pr|pe|"
             "pi|rj|rn|rs|ro|rr|sc|sp|se|to")


def normalise_city(city_col):
    """Clean Brazilian city names.

    Handles the real mess in this dataset:
      'sao paulo sp'          -> 'sao paulo'
      'sao paulo - sp'        -> 'sao paulo'
      'maua/sao paulo'        -> 'maua'
      '  SAO   PAULO  '       -> 'sao paulo'

    Known limitation: does not fix typos such as 'sao paulop'.
    Fuzzy matching is out of scope — documented in data_quality_rules.md.
    """
    c = lower(trim(city_col))
    c = trim(element_at(split(c, "/"), 1))
    c = regexp_replace(c, r"[\s\-]+(" + BR_STATES + r")$", "")
    c = regexp_replace(c, r"\s+", " ")
    return trim(c)


def clean_state(state_col):
    return upper_trim(state_col)


def upper_trim(c):
    from pyspark.sql.functions import upper
    return upper(trim(c))