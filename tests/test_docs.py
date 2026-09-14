"""The documentation cross-references, checked like code.

LEARN.md points forward at assignments ("**Assignments: 5, 8, 10.**") and
ASSIGNMENTS.md points back at LEARN sections ("*LEARN §A2, §A4.*"). Those two
sets drifted apart once already: the forward pointers were written by hand after
the fact and several of them named an assignment about a completely different
topic.

Prose cannot be type-checked, but a cross-reference can. These tests fail if the
two directions disagree, if either points at something that does not exist, or
if an assignment number is skipped -- which is exactly the class of error a
reader hits and an author never notices.
"""

from __future__ import annotations

import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
LEARN = (ROOT / "LEARN.md").read_text()
ASSIGNMENTS = (ROOT / "ASSIGNMENTS.md").read_text()
ROADMAP = (ROOT / "ROADMAP.md").read_text()
README = (ROOT / "README.md").read_text()
DOCS = {"LEARN.md": LEARN, "ASSIGNMENTS.md": ASSIGNMENTS,
        "ROADMAP.md": ROADMAP, "README.md": README}


def learn_sections() -> set[str]:
    return set(re.findall(r"^## ([A-C]\d+)\.", LEARN, re.M))


def assignment_titles() -> dict[int, str]:
    return {
        int(n): t
        for n, t in re.findall(r"^### (\d+)\. (.+?) ●", ASSIGNMENTS, re.M)
    }


def forward_pointers() -> dict[str, set[str]]:
    """LEARN section -> the assignments it sends you to."""
    out: dict[str, set[str]] = {}
    current = None
    for line in LEARN.splitlines():
        heading = re.match(r"^## ([A-C]\d+)\.", line)
        if heading:
            current = heading.group(1)
        pointer = re.match(r"^\*\*Assignments?: ([^*]+)\*\*", line)
        if pointer and current:
            out.setdefault(current, set()).update(
                tok.strip().rstrip(".") for tok in pointer.group(1).split(",")
            )
    return out


def back_pointers() -> dict[int, set[str]]:
    """Assignment number -> the LEARN sections it says it drills."""
    out: dict[int, set[str]] = {}
    for m in re.finditer(r"^### (\d+)\..*?\n\*LEARN ([^.]*)\.", ASSIGNMENTS, re.M):
        out[int(m.group(1))] = set(re.findall(r"§([A-C]\d+)", m.group(2)))
    return out


def test_assignments_are_numbered_without_gaps():
    numbers = sorted(assignment_titles())
    assert numbers == list(range(1, len(numbers) + 1)), numbers


def test_every_assignment_declares_a_learn_section():
    missing = sorted(set(assignment_titles()) - set(back_pointers()))
    assert not missing, f"assignments with no '*LEARN §..*' line: {missing}"


def test_back_pointers_name_real_learn_sections():
    known = learn_sections()
    bad = {n: sorted(s - known) for n, s in back_pointers().items() if s - known}
    assert not bad, f"assignments citing non-existent LEARN sections: {bad}"


def test_forward_pointers_name_real_assignments():
    known = {str(n) for n in assignment_titles()} | {"capstone"}
    bad = {s: sorted(v - known) for s, v in forward_pointers().items() if v - known}
    assert not bad, f"LEARN sections citing non-existent assignments: {bad}"


def test_the_two_directions_agree():
    """The heart of it: if LEARN §A4 sends you to assignment 6, assignment 6
    must say it drills §A4. A one-directional reference is how a reader ends up
    on an exercise about something else entirely."""
    back = back_pointers()
    disagreements = []
    for section, numbers in forward_pointers().items():
        for raw in sorted(numbers):
            if raw == "capstone":
                continue
            n = int(raw)
            if section not in back.get(n, set()):
                disagreements.append(
                    f"LEARN §{section} -> assignment {n} "
                    f"({assignment_titles().get(n, '?')}), but that assignment "
                    f"cites {sorted(back.get(n, set())) or 'nothing'}"
                )
    assert not disagreements, "\n".join(disagreements)


def test_every_assignment_is_reachable_from_learn():
    """A reader working through LEARN should be sent to all of them."""
    pointed_at = {n for v in forward_pointers().values() for n in v}
    orphans = sorted(str(n) for n in assignment_titles() if str(n) not in pointed_at)
    assert not orphans, f"assignments no LEARN section points to: {orphans}"


def test_roadmap_only_cites_real_assignments():
    cited = set()
    for line in ROADMAP.splitlines():
        if "Assignment" in line:
            cited.update(re.findall(r"\b(\d{1,2})\b", line.split("Assignment", 1)[1]))
    known = {str(n) for n in assignment_titles()}
    assert cited, "the roadmap should send people to assignments"
    assert not (cited - known), f"roadmap cites unknown assignments: {sorted(cited - known)}"


@pytest.mark.parametrize("name", sorted(DOCS))
def test_inline_assignment_references_exist(name):
    """Catches 'see Assignment 31' in running prose."""
    known = set(assignment_titles())
    bad = [
        int(n) for n in re.findall(r"[Aa]ssignments?\s+(\d{1,2})\b", DOCS[name])
        if int(n) not in known
    ]
    assert not bad, f"{name} references non-existent assignments: {sorted(set(bad))}"


@pytest.mark.parametrize("name", sorted(DOCS))
def test_internal_links_resolve(name):
    """Relative links and heading anchors, checked the way GitHub builds them:
    lowercase, drop punctuation, then each remaining space becomes a hyphen --
    which is why 'Part A - DuckDB' yields a double hyphen."""
    def slug(heading: str) -> str:
        h = heading.strip().lstrip("#").strip().lower()
        return re.sub(r"[^\w\s-]", "", h).replace(" ", "-")

    anchors = {
        f: {slug(l) for l in text.splitlines() if l.startswith("#")}
        for f, text in DOCS.items()
    }
    broken = []
    for match in re.finditer(r"\[[^\]]+\]\(([^)]+)\)", DOCS[name]):
        target = match.group(1)
        if target.startswith(("http://", "https://")):
            continue
        path, _, anchor = target.partition("#")
        path = path or name
        if not (ROOT / path).exists():
            broken.append(f"missing file: {target}")
        elif anchor and anchor not in anchors.get(path, set()):
            broken.append(f"missing anchor: {target}")
    assert not broken, f"{name}: " + "; ".join(broken)


def test_the_roadmap_sends_you_to_every_assignment():
    """The roadmap is the intended path through the material, so an assignment
    missing from it is one nobody will ever be told to do."""
    cited = set()
    for line in ROADMAP.splitlines():
        if "Assignment" in line:
            cited.update(re.findall(r"\b(\d{1,2})\b", line.split("Assignment", 1)[1]))
    orphans = sorted(
        n for n in assignment_titles() if str(n) not in cited
    )
    assert not orphans, f"assignments the roadmap never mentions: {orphans}"


@pytest.mark.skipif(
    not __import__("os").environ.get("CHECK_EXTERNAL_LINKS"),
    reason="set CHECK_EXTERNAL_LINKS=1 to hit the network",
)
def test_external_links_resolve():
    """Opt-in, because the rest of this suite must run offline.

    Documentation links rot -- DuckDB in particular reorganises its docs between
    minor versions. Run this before publishing:  CHECK_EXTERNAL_LINKS=1 pytest
    """
    import urllib.error
    import urllib.request

    urls = set()
    for text in DOCS.values():
        urls |= set(re.findall(r"\]\((https?://[^)]+)\)", text))
        urls |= set(re.findall(r"<(https?://[^>]+)>", text))
    urls = {u for u in urls if "localhost" not in u}

    broken = []
    for url in sorted(urls):
        request = urllib.request.Request(url, method="HEAD",
                                         headers={"User-Agent": "duckflow-docs-check"})
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                if response.status >= 400:
                    broken.append(f"{response.status} {url}")
        except urllib.error.HTTPError as exc:
            if exc.code >= 400:
                broken.append(f"{exc.code} {url}")
        except Exception as exc:  # noqa: BLE001 -- a flaky network is not a doc bug
            broken.append(f"?? {url} ({type(exc).__name__})")
    assert not broken, "\n".join(broken)
