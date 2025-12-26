from __future__ import annotations

from typing import List

from .models import TaskSpec


def build_tasks(fiction_only: bool) -> List[TaskSpec]:
    publishers = [
        ("JPS", "publisher_seed"),
        ("Schocken", "publisher_seed"),
        ("Jewish Publication Society", "publisher_seed"),
        ("Yad Vashem", "publisher_seed"),
        ("Ktav", "publisher_seed"),
        ("Gefen", "publisher_seed"),
        ("Ben Yehuda Press", "publisher_seed"),
        ("Toby Press", "publisher_seed"),
        ("Academic Studies Press", "publisher_seed"),
    ]

    subjects = [
        ("Judaism", "subject_seed"),
        ("Jewish history", "subject_seed"),
        ("Holocaust", "subject_seed"),
        ("Israel", "subject_seed"),
        ("Antisemitism", "subject_seed"),
        ("Jewish fiction", "subject_seed"),
        ("Yiddish language", "subject_seed"),
        ("Hebrew language", "subject_seed"),
        ("Kabbalah", "subject_seed"),
    ]

    base_queries = [
        "Jewish", "Judaism", "Jews", "Hebrew", "Yiddish",
        "Talmud", "Torah", "Rabbi", "Synagogue",
        "Hasidic", "Kabbalah",
        "Israel", "Jerusalem", "Zionism",
        "Holocaust", "Shoah", "Antisemitism", "Pogrom",
        "Passover", "Hanukkah", "Yom Kippur", "Rosh Hashanah", "Shabbat",
        "Bar Mitzvah", "Bat Mitzvah",
        "Sephardic", "Ashkenazi",
    ]

    intent_queries = [
        "Jewish memoir",
        "Jewish biography",
        "Jewish cookbook kosher",
        "Jewish education",
        "Jewish philosophy",
        "Jewish prayer siddur",
        "Talmud commentary",
        "Torah commentary",
        "History of Israel",
        "Israel politics",
        "Holocaust memoir",
        "Holocaust survivor testimony",
        "Antisemitism history",
        "Jewish diaspora",
        "Jewish immigration",
    ]

    if fiction_only:
        base_queries += ["Jewish novel", "Israeli novel", "Holocaust novel", "Jewish fiction", "Israeli fiction"]
        intent_queries += ["Jewish mystery", "Jewish romance", "Israeli thriller", "Holocaust fiction", "Jewish short stories"]

    out: List[TaskSpec] = []
    for pub, g in publishers:
        out.append(TaskSpec(endpoint="publisher", query=pub, group=g))
    for sub, g in subjects:
        out.append(TaskSpec(endpoint="subject", query=sub, group=g))
    for q in base_queries:
        out.append(TaskSpec(endpoint="search", query=q, group="alpha"))
    for q in intent_queries:
        out.append(TaskSpec(endpoint="search", query=q, group="intent"))

    seen = set()
    ded = []
    for t in out:
        k = (t.endpoint, t.query.lower().strip())
        if k in seen:
            continue
        seen.add(k)
        ded.append(t)
    return ded
