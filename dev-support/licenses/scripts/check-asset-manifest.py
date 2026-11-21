#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Per-push source-tree licence gates (requirements 7 stage B, 48, 53).

1. R7 stage B: every path-style entry in manifest-assets.txt must resolve
   to at least one file or directory in the SOURCE TREE.
2. R48: every path the source-release LICENSE.txt names (its bundled
   third-party section, including "For <path>:" text headers) must resolve
   in the source tree, and every path-style entry of manifest-assets.txt
   must be covered by LICENSE.txt — what ships in the binary is also in the
   source release.
3. R53 (licenses-binary part of R10): the texts/ store is the single home
   of the license texts and the tarball's licenses-binary/ directory is
   GENERATED from LICENSE-binary (R13, emitter in
   generate-archive-manifest.py). Every component-specific store text —
   anything not exempted in texts-generic — must still be referenced:
   its slug (case-insensitive, whole word) or the token recorded for it
   in licenses-binary-aliases must appear in LICENSE-binary, or the
   generated directory would ship a text for a component that is gone.
   An alias row with no text, a texts-generic row with no text, or a
   slug both aliased and exempted fails.

The checks below describe gate 1; gates 2 and 3 reuse its path resolution.

The curated non-jar asset manifest (manifest-assets.txt, "section<TAB>line")
feeds the R40 generator; a stale path in it silently puts a listed-yet-absent
entry into LICENSE-binary (and hides assets that shipped in its place — the
d3-3.5.17 -> d3-v4.1.1 class). This gate fails closed on any entry that no
longer resolves, so the curation cannot rot.

Entry kinds:
- path-style: contains a filesystem path (relative to the repo root).
  Braces expand ({a,b} and {a|b} both mean alternation), trailing globs
  (*, dir/*) glob, a trailing parenthetical note is stripped. An entry
  matches if ANY expanded form resolves to an existing file or directory.
- package-style: "name vX.Y" / "name X.Y" npm-style lines (e.g.
  "leveldb v1.13", "atlaskit/... 1.7.4") have no source-tree path; they
  are skipped here and validated by the archive-side asset gate instead.

A node_modules entry names a directory npm creates at build time; it
resolves when the package.json next to it exists.

Usage: check-asset-manifest.py [repo-root] [manifest-assets.txt]
Defaults: repo root three levels up from this script; manifest from the
curation/ sibling directory. Exit 0 when every gate passes; exit 1 listing every failure (fail
closed: a missing LICENSE.txt, LICENSE-binary, texts/ store, texts-generic
or aliases file is a failure too).
"""

import fnmatch
import os
import re
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# curated inputs live in the curation/ sibling directory; the text
# store stays at the tooling root
CURATED_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, '..', 'curation'))
TOOLING_ROOT = os.path.normpath(os.path.join(SCRIPT_DIR, '..'))
DEFAULT_MANIFEST = os.path.join(CURATED_DIR, "manifest-assets.txt")
ALIASES = os.path.join(CURATED_DIR, "licenses-binary-aliases")
TEXTS_STORE = os.path.join(TOOLING_ROOT, "texts")
TEXTS_GENERIC = os.path.join(CURATED_DIR, "texts-generic")
NOTICE_ALIASES = os.path.join(CURATED_DIR, "notice-aliases")
NOTICE_EXCEPTIONS = os.path.join(CURATED_DIR, "notice-exceptions")

# a LICENSE.txt line naming a bundled path: "hadoop-x/..." or "For hadoop-x/...:"
LICENSE_TXT_PATH = re.compile(r"^(?:For )?(hadoop-[\w.-]+/\S+?):?$")

# "name 1.2.3" / "name v1.2.3" (optionally scoped "org/name 1.2.3") —
# a package coordinates line, not a filesystem path.
PACKAGE_STYLE = re.compile(r"^\S+( \S+)? v?[0-9][\w.:-]*$")


def expand_braces(pattern):
    """Expand the outermost {a,b} / {a|b} group into separate patterns."""
    start = pattern.find("{")
    if start == -1:
        return [pattern]
    depth = 0
    for i in range(start, len(pattern)):
        if pattern[i] == "{":
            depth += 1
        elif pattern[i] == "}":
            depth -= 1
            if depth == 0:
                inner = pattern[start + 1 : i]
                prefix, suffix = pattern[:start], pattern[i + 1 :]
                out = []
                for alt in re.split(r"[,|]", inner):
                    if alt == "":
                        continue
                    out.extend(expand_braces(prefix + alt + suffix))
                return out or [pattern]
    # unbalanced brace: treat literally
    return [pattern]


def parse_entries(manifest_path):
    """Yield (section, raw_line) for every non-comment entry."""
    if not os.path.isfile(manifest_path) or os.path.getsize(manifest_path) == 0:
        sys.exit(f"FAIL: cannot read curated asset manifest ({manifest_path})")
    with open(manifest_path, encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            line = raw.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            if "\t" not in line:
                sys.exit(
                    f"FAIL: malformed line {lineno} in {manifest_path} "
                    f"(expected section<TAB>line): {line!r}"
                )
            section, entry = line.split("\t", 1)
            yield section, entry


def strip_note(entry):
    """Drop a trailing parenthetical note from a path-style entry."""
    note = re.search(r"\s+\([^()]*\)\s*$", entry)
    return entry[: note.start()] if note else entry


def match_count(entry, repo_root):
    """Number of source-tree files/dirs the path-style entry resolves to."""
    total = 0
    for pattern in expand_braces(strip_note(entry)):
        full = os.path.join(repo_root, pattern)
        if os.path.exists(full):  # bare file or directory
            total += 1
            continue
        if os.path.basename(full) == "node_modules":
            # created by npm at build time: its package.json must exist
            if os.path.isfile(os.path.join(os.path.dirname(full), "package.json")):
                total += 1
            continue
        if any(ch in pattern for ch in "*?["):
            # glob is rooted: /abs/dir/*.js must still match files under it
            base, tail = os.path.split(full)
            if os.path.isdir(base):
                total += sum(
                    1 for name in os.listdir(base) if fnmatch.fnmatch(name, tail)
                )
    return total


def license_txt_paths(license_txt):
    """(lineno, path) for every bundled path LICENSE.txt names."""
    if not os.path.isfile(license_txt):
        sys.exit(f"FAIL: cannot read {license_txt}")
    with open(license_txt, encoding="utf-8") as fh:
        lines = fh.read().splitlines()
    start = next(
        (i for i, l in enumerate(lines) if l.startswith("This product bundles")),
        None,
    )
    if start is None:
        sys.exit(f"FAIL: no bundled third-party section in {license_txt}")
    out = []
    for i in range(start, len(lines)):
        m = LICENSE_TXT_PATH.match(lines[i].strip())
        if m:
            out.append((i + 1, m.group(1)))
    if not out:
        sys.exit(f"FAIL: no bundled paths parsed from {license_txt}")
    return out


def covered(path, license_paths):
    """True when LICENSE.txt names the path or a directory above it."""
    return any(path == lp or path.startswith(lp.rstrip("/") + "/")
               for lp in license_paths)


def check_license_txt(repo_root, manifest_paths):
    """R48: LICENSE.txt paths resolve; manifest paths are covered by it."""
    failures = []
    license_txt = os.path.join(repo_root, "LICENSE.txt")
    entries = license_txt_paths(license_txt)
    expanded = set()
    for lineno, path in entries:
        if match_count(path, repo_root) == 0:
            failures.append(f"LICENSE.txt:{lineno} names a path that does "
                            f"not exist: {path}")
        expanded.update(expand_braces(path))
    for section, entry in manifest_paths:
        for path in expand_braces(strip_note(entry.strip())):
            if not covered(path, expanded):
                failures.append(f"[{section}] {path} ships in the binary "
                                "(manifest-assets.txt) but LICENSE.txt does "
                                "not list it")
    print(f"LICENSE.txt: {len(entries)} bundled paths checked, "
          f"{len(manifest_paths)} manifest paths checked for coverage")
    return failures


def check_licenses_binary(repo_root, aliases_path, store_dir=TEXTS_STORE,
                          generic_path=TEXTS_GENERIC):
    """R53: every component-specific text in the texts/ store is still
    referenced by LICENSE-binary (the tarball's licenses-binary/
    directory is generated from the manifest and the store, R13)."""
    failures = []
    manifest_file = os.path.join(repo_root, "LICENSE-binary")
    if not os.path.isfile(manifest_file):
        sys.exit(f"FAIL: cannot read {manifest_file}")
    if not os.path.isdir(store_dir):
        sys.exit(f"FAIL: no texts/ store directory in {store_dir}")
    if not os.path.isfile(aliases_path):
        sys.exit(f"FAIL: cannot read {aliases_path}")
    if not os.path.isfile(generic_path):
        sys.exit(f"FAIL: cannot read {generic_path}")
    with open(manifest_file, encoding="utf-8") as fh:
        manifest = fh.read().lower()
    aliases = {}
    with open(aliases_path, encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            line = raw.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            fields = line.split("\t")
            if len(fields) == 2:  # legacy component row
                fields = fields[:1] + ["component"] + fields[1:]
            if len(fields) != 3:
                sys.exit(f"FAIL: malformed line {lineno} in {aliases_path} "
                         f"(expected slug<TAB>component|section<TAB>token): "
                         f"{line!r}")
            slug, kind, token = fields
            if kind not in ("component", "section"):
                sys.exit(f"FAIL: malformed line {lineno} in {aliases_path} "
                         f"(kind must be component or section, got {kind!r})")
            token = token.strip()
            # an empty token builds a vacuous whole-word pattern that
            # matches anything — the gate would silently pass
            if not token:
                sys.exit(f"FAIL: empty token on line {lineno} in "
                         f"{aliases_path}")
            if slug in aliases:
                sys.exit(f"FAIL: duplicate slug {slug!r} in {aliases_path}")
            aliases[slug] = (kind, token)
    generic = load_texts_generic()
    slugs = sorted(
        name[len("LICENSE-"):-len(".txt")]
        for name in os.listdir(store_dir)
        if name.startswith("LICENSE-") and name.endswith(".txt")
    )
    if not slugs:
        sys.exit(f"FAIL: no LICENSE-*.txt texts in {store_dir}")
    slug_set = set(slugs)
    both = sorted(set(aliases) & generic)
    if both:
        failures.append(
            f"slugs both aliased and exempted in texts-generic: "
            f"{', '.join(both)} — pick one (aliased texts must be "
            "referenced; exempted texts never ship)")
    for label, names in (("licenses-binary-aliases", set(aliases)),
                         ("texts-generic", generic)):
        for slug in sorted(names - slug_set):
            failures.append(
                f"{label}: {slug!r} has no LICENSE-{slug}.txt in the "
                f"store ({store_dir}) — drop the row")
    # staleness: a component-specific text the manifest no longer
    # references would ship (via the R13 emitter) for a component that
    # is gone — self-cleaning: delete the text, or record the listed
    # component's name in licenses-binary-aliases. Section-kind rows
    # (a section's generic text) are exempt: a section may legitimately
    # vanish with the dependency set and reappear later.
    checked = 0
    for slug in slugs:
        if slug in generic or aliases.get(slug, ("component", ""))[0] == "section":
            continue
        checked += 1
        token = aliases.get(slug, ("component", slug))[1]
        # whole-word: a short slug (go, asm, tree) must not match inside
        # another word of the licence prose ("goodwill")
        word = r"(?<![a-z0-9])" + re.escape(token.lower()) + r"(?![a-z0-9])"
        if not re.search(word, manifest):
            failures.append(
                f"texts/LICENSE-{slug}.txt: no component or section in "
                f"LICENSE-binary matches {token!r} — delete the stale "
                "text, or record the listed component's (or section's) "
                "name in licenses-binary-aliases")
    print(f"texts store: {len(slugs)} texts ({checked} checked against "
          f"LICENSE-binary, {len(generic)} exempted in texts-generic, "
          f"{sum(1 for k, _ in aliases.values() if k == 'section')} section "
          f"texts by title alias)")
    return failures


def parse_binary_aliases():
    """slug -> (kind, token) for EVERY row of the R53 aliases file
    (the same tokens the R13 emitter matches). Fails closed on an
    empty token (a vacuous whole-word pattern would match anything)."""
    rows = {}
    with open(ALIASES, encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            line = raw.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            fields = line.split("\t")
            if len(fields) == 2:
                kind, slug, token = "component", fields[0], fields[1]
            elif len(fields) == 3:
                slug, kind, token = fields
            else:
                sys.exit(f"FAIL: malformed line {lineno} in {ALIASES}: {line!r}")
            if kind not in ("component", "section"):
                sys.exit(f"FAIL: bad kind {kind!r} on line {lineno} in "
                         f"{ALIASES}: {line!r}")
            token = token.strip()
            if not token:
                sys.exit(f"FAIL: empty token on line {lineno} in {ALIASES}: "
                         f"{line!r}")
            if slug in rows:
                sys.exit(f"FAIL: duplicate slug {slug!r} on line {lineno} in "
                         f"{ALIASES}")
            rows[slug] = (kind, token)
    return rows


def load_texts_generic():
    """The texts-generic slugs: store texts that NEVER ship (kept in
    the store only for license-NAME resolution). Fails closed on a
    duplicate row."""
    generic = set()
    with open(TEXTS_GENERIC, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.split("#")[0].strip()
            if not line:
                continue
            if line in generic:
                sys.exit(f"FAIL: duplicate slug {line!r} in {TEXTS_GENERIC}")
            generic.add(line)
    return generic


def check_notice_coverage(repo_root):
    """R14: attribution-required licenses are reflected in the shipped
    NOTICE file. Every component line of an attribution-required
    LICENSE-binary section (all but the Public Domain section) is
    covered when it is mentioned in NOTICE-binary, or a shipped
    component license text that carries the copyright/attribution line
    covers it, or a notice-exceptions row records why neither applies
    (self-cleaning: an exception row whose component is no longer
    uncovered is stale and must be dropped)."""
    manifest_file = os.path.join(repo_root, "LICENSE-binary")
    notice_file = os.path.join(repo_root, "NOTICE-binary")
    if not os.path.isfile(manifest_file):
        sys.exit(f"FAIL: cannot read {manifest_file}")
    if not os.path.isfile(notice_file):
        sys.exit(f"FAIL: cannot read {notice_file}")
    with open(notice_file, encoding="utf-8") as fh:
        notice = fh.read().lower()
    notice_aliases = {}
    if not os.path.isfile(NOTICE_ALIASES):
        sys.exit(f"FAIL: cannot read {NOTICE_ALIASES}")
    with open(NOTICE_ALIASES, encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            line = raw.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            if "\t" not in line:
                sys.exit(f"FAIL: malformed line {lineno} in {NOTICE_ALIASES} "
                         f"(expected token<TAB>notice-phrase): {line!r}")
            token, phrase = line.split("\t", 1)
            token = token.strip()
            # an empty token never matches a real artifactId, but it is
            # malformed input — reject it rather than rely on the
            # lookup's accidental fail-closed behavior
            if not token:
                sys.exit(f"FAIL: empty token on line {lineno} in "
                         f"{NOTICE_ALIASES}")
            phrase = phrase.strip()
            # an empty phrase builds a vacuous whole-word pattern that
            # matches anything — an uncovered component would pass
            if not phrase:
                sys.exit(f"FAIL: empty notice-phrase on line {lineno} in "
                         f"{NOTICE_ALIASES}")
            if token in notice_aliases:
                sys.exit(f"FAIL: duplicate token {token!r} in {NOTICE_ALIASES}")
            notice_aliases[token] = phrase
    exceptions = {}
    if not os.path.isfile(NOTICE_EXCEPTIONS):
        sys.exit(f"FAIL: cannot read {NOTICE_EXCEPTIONS}")
    with open(NOTICE_EXCEPTIONS, encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            line = raw.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            if "\t" not in line:
                sys.exit(f"FAIL: malformed line {lineno} in "
                         f"{NOTICE_EXCEPTIONS} (expected "
                         f"token<TAB>justification): {line!r}")
            token, why = line.split("\t", 1)
            token = token.strip()
            if not token:
                sys.exit(f"FAIL: empty token on line {lineno} in "
                         f"{NOTICE_EXCEPTIONS}")
            if token in exceptions:
                sys.exit(f"FAIL: duplicate token {token!r} in "
                         f"{NOTICE_EXCEPTIONS}")
            if not why.strip():
                sys.exit(f"FAIL: line {lineno} in {NOTICE_EXCEPTIONS} has "
                         "no justification (requirement 52)")
            exceptions[token] = why.strip()

    # the component lines of every titled, attribution-required section
    coord_re = re.compile(r"^[\w.\-]+:[\w.\-]+:[\w.\-]+$")
    with open(manifest_file, encoding="utf-8") as fh:
        lines = fh.read().splitlines()
    titles = []
    for i, line in enumerate(lines):
        underline = lines[i + 1].strip() if i + 1 < len(lines) else ""
        if line.strip() and underline and len(underline) >= 4 \
                and set(underline) == {"-"}:
            titles.append((i, line.strip()))
    if not titles:
        sys.exit(f"FAIL: no titled sections parsed from {manifest_file}")
    required = []  # (title, g:a:v)
    for idx, (line_no, title) in enumerate(titles):
        if title.lower() == "public domain":
            continue  # no attribution required
        end = titles[idx + 1][0] if idx + 1 < len(titles) else len(lines)
        for line in lines[line_no + 2:end]:
            stripped = line.strip()
            if coord_re.match(stripped):
                required.append((title, stripped))
    if not required:
        sys.exit("FAIL: no attribution-required components parsed from "
                 f"{manifest_file}")

    # shipped component texts that carry a copyright line: a component
    # is covered by one when the text's reference token (slug or
    # component-kind alias token) matches the component's manifest line
    # (the same token the R13 emitter uses to decide the text ships).
    # Only texts the emitter would SHIP may attribute: a texts-generic
    # text never ships (its slug can false-match a coordinate word —
    # 'json' inside 'javax.json' — while the text is absent from the
    # tarball), and a section text attributes the section, never a
    # single component.
    if not os.path.isfile(ALIASES):
        sys.exit(f"FAIL: cannot read {ALIASES}")
    if not os.path.isfile(TEXTS_GENERIC):
        sys.exit(f"FAIL: cannot read {TEXTS_GENERIC}")
    alias_rows = parse_binary_aliases()
    alias_tokens = {slug: token for slug, (kind, token)
                    in alias_rows.items() if kind == "component"}
    generic_slugs = load_texts_generic()
    copyright_tokens = []
    for name in sorted(os.listdir(TEXTS_STORE)):
        if not (name.startswith("LICENSE-") and name.endswith(".txt")):
            continue
        slug = name[len("LICENSE-"):-len(".txt")]
        if (slug in generic_slugs
                or alias_rows.get(slug, ("component", ""))[0] == "section"):
            continue
        path = os.path.join(TEXTS_STORE, name)
        if os.path.getsize(path) == 0:
            sys.exit(f"FAIL: empty store text: {path}")
        with open(path, encoding="utf-8") as fh:
            body = fh.read()
        if not re.search(r"copyright", body, re.I):
            continue
        token = alias_tokens.get(slug, slug)
        copyright_tokens.append(token)

    def mentioned(token):
        word = r"(?<![a-z0-9])" + re.escape(token.lower()) + r"(?![a-z0-9])"
        return re.search(word, notice)

    def text_covers(coord):
        low = coord.lower()
        for token in copyright_tokens:
            word = (r"(?<![a-z0-9])" + re.escape(token.lower())
                    + r"(?![a-z0-9])")
            if re.search(word, low):
                return True
        return False

    uncovered = []
    initial_exceptions = len(exceptions)
    for title, coord in required:
        artifact = coord.split(":")[1]
        tokens = [artifact]
        if artifact in notice_aliases:
            tokens.append(notice_aliases[artifact])
        if any(mentioned(t) for t in tokens) or text_covers(coord):
            continue
        if artifact in exceptions:
            exceptions.pop(artifact)  # consumed; stale otherwise
            continue
        uncovered.append((title, coord, artifact))
    final_failures = []
    for title, coord, artifact in uncovered:
        final_failures.append(
            f"[{title}] {coord}: not in NOTICE-binary, its shipped "
            "license text carries no copyright line, and no "
            f"notice-exceptions row covers {artifact!r} — add an "
            "attribution to NOTICE-binary, a component text with the "
            "copyright line, or a justified notice-exceptions row")
    for token in sorted(exceptions):  # rows left unconsumed are stale
        final_failures.append(
            f"notice-exceptions: {token!r} covers no uncovered component "
            "— the component gained NOTICE mention or a copyright-"
            "carrying text; drop the row")
    consumed = initial_exceptions - len(exceptions)
    # notice-aliases rows are staleness-checked: their token is an
    # artifactId LICENSE-binary lists; when the component is gone the
    # row must be dropped (header promise, self-cleaning)
    listed_artifacts = {coord.split(":")[1] for _, coord in required}
    stale_alias_tokens = sorted(set(notice_aliases) - listed_artifacts)
    for token in stale_alias_tokens:
        final_failures.append(
            f"notice-aliases: {token!r} is no longer a listed component — "
            "drop the row")
    print(f"NOTICE-binary: {len(required)} attribution-required components "
          f"({len(required) - len(uncovered) - consumed} covered by mention "
          f"or text, {consumed} exception rows used, "
          f"{len(notice_aliases) - len(stale_alias_tokens)} notice aliases)")
    return final_failures


def main():
    args = sys.argv[1:]
    manifest = DEFAULT_MANIFEST
    repo_root = None
    for arg in args:
        if arg.endswith("manifest-assets.txt"):
            manifest = arg
        elif repo_root is None:
            repo_root = arg
    if repo_root is None:
        repo_root = os.path.abspath(
            os.path.join(SCRIPT_DIR, os.pardir, os.pardir, os.pardir)
        )
    if not os.path.isdir(repo_root):
        sys.exit(f"FAIL: repo root does not exist: {repo_root}")

    path_entries = []
    package_entries = 0
    for section, entry in parse_entries(manifest):
        if PACKAGE_STYLE.match(entry.strip()):
            package_entries += 1
        else:
            path_entries.append((section, entry))

    if not path_entries and package_entries == 0:
        sys.exit(f"FAIL: no entries parsed from {manifest}")

    unresolved = []
    for section, entry in path_entries:
        n = match_count(entry.strip(), repo_root)
        if n == 0:
            unresolved.append((section, entry))
    failed = False

    print(
        f"asset manifest: {len(path_entries)} path-style entries checked, "
        f"{package_entries} package-style entries skipped "
        "(package-style entries are validated by the archive asset gate)"
    )
    if unresolved:
        print(
            f"FAIL: {len(unresolved)} curated asset entries resolve to "
            f"nothing in the source tree ({repo_root}) — prune or fix them "
            "(a stale entry hides the asset that shipped in its place):",
            file=sys.stderr,
        )
        for section, entry in unresolved:
            print(f"  [{section}] {entry}", file=sys.stderr)
        failed = True
    else:
        print("asset manifest gate passed: every path-style entry resolves")

    for name, problems in (
        ("LICENSE.txt gate", check_license_txt(repo_root, path_entries)),
        ("texts-store gate", check_licenses_binary(repo_root, ALIASES)),
        ("NOTICE-binary gate", check_notice_coverage(repo_root)),
    ):
        if problems:
            print(f"FAIL: {name}: {len(problems)} problem(s):", file=sys.stderr)
            for problem in problems:
                print(f"  {problem}", file=sys.stderr)
            failed = True
        else:
            print(f"{name} passed")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
