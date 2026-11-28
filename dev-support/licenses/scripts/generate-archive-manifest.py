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
"""R40 archive-derived manifest generator.

Derives LICENSE-binary from the distributed archives' enumerated
contents (the archive-jars.tsv files produced by
enumerate-archive-jars.py), resolving each third-party artifact's
license from the collected dependency inventory and applying the
curated multi-license policy (the name→section tables in this script).

Fail-closed by design: an archive artifact absent from the inventory,
a partial row that resists normalization, an uncurated multi-license
set, a malformed TSV row, or a zero-row enumeration is a hard error,
never a silent omission.

NOTICE-binary is NOT generated here: its per-component attribution
corpus is curated prose that cannot be derived from jar metadata. It
stays hand-maintained and is validated by the R10 drift gate.

Usage (from anywhere; curated-file defaults resolve from the curation/
sibling directory, the text store from the tooling root):
  generate-archive-manifest.py --inventory THIRD-PARTY.txt \
      --archive-tsv tsv1 [tsv2 ...] --out LICENSE-binary

R13 text bundling — derive the tarball's licenses-binary/ directory
from a manifest and the versioned texts/ store (fail-closed):
  generate-archive-manifest.py --emit-licenses-dir-only \
      --manifest LICENSE-binary --licenses-dir <dir>
or pass --licenses-dir alongside the full-mode flags to emit the
directory from the freshly generated manifest.
"""

import argparse
import collections
import os
import re
import shutil
import sys
from typing import NoReturn

# The inventory line: "(License) (License2) Name (g:a:v - url)" — the
# license field may carry several paren groups (multi-license POMs).
INVENTORY_LINE = re.compile(
    r'^\s*(?P<lics>(?:\([^)]+\)\s*)+)'
    r'(?P<name>.+?)\s'
    r'\((?P<g>[^:()]+):(?P<a>[^:()]+):(?P<v>[^)\s]+)'
    r'(?:\s-\s.*)?\)\s*$'
)
LICENSE_GROUPS = re.compile(r'\(([^)]+)\)')

# Manifest section order and headers. The apache section has no title —
# its template (manifest-template.txt) carries the ASF preamble and the
# Apache-2.0 text. Every other section references its text in the
# licenses-binary/ store shipped next to the manifest.
SECTIONS = [
    ('apache', None),
    ('bsd2', 'BSD 2-Clause'),
    ('bsd3', 'BSD 3-Clause'),
    ('mit', 'MIT License'),
    ('cddl', 'CDDL 1.1 + GPLv2 with classpath exception'),
    ('edl', 'Eclipse Distribution License (EDL) 1.0'),
    ('epl1', 'Eclipse Public License 1.0'),
    ('epl2', 'Eclipse Public License (EPL) 2.0'),
    ('combined',
     'Eclipse Public License (EPL) 2.0 with some parts being\n'
     'GNU General Public License (GPL), Version 2, With Classpath Exception,\n'
     'Apache License, 2.0 or Public Domain'),
    ('jdom', 'JDOM License'),
    ('isc', 'ISC License'),
    ('pd', 'Public Domain'),
    ('dom4j', 'Dom4J license'),
]

SECTION_KEYS = {key for key, _ in SECTIONS}

# The combined (jersey/hk2) section carries a fixed attribution note.
SECTION_NOTES = {
    'combined': 'See also https://github.com/eclipse-ee4j/jersey/blob/master/NOTICE.md',
}

# Line emitted once between the Apache section and the other sections.
INTERLUDE = [
    'This product bundles various third-party components under other open source',
    'licenses. This section summarizes those components and their licenses.',
    'See licenses-binary/ for text of these licenses.',
]

# Inventory license name -> section (single-license resolution).
NAME_TO_SECTION = {
    'The Apache Software License, Version 2.0': 'apache',
    'BSD 2-Clause License': 'bsd2',
    'Revised BSD': 'bsd3',
    'The Go license': 'bsd3',  # Google's BSD-3-clause-style Go license (re2j)
    'MIT License': 'mit',
    # the license-maven-plugin truncates the closing paren when the
    # POM's license name itself ends in a paren group (3.5's
    # azure-keyvault-core POM declares 'The MIT License (MIT)'):
    # THIRD-PARTY.txt carries 'The MIT License (MIT'
    'The MIT License (MIT': 'mit',
    'Bouncy Castle Licence': 'mit',
    'CDDL + GPLv2 with classpath exception': 'cddl',
    'EDL 1.0': 'edl',
    'Eclipse Public License - Version 1.0': 'epl1',
    'EPL 2.0': 'epl2',
    'Public Domain': 'pd',
}

# Multi-license policy (R28), applied in order to the license-name SET:
# 1. Apache-2.0 present -> apache section.
# 2. EDL 1.0 present -> edl section.
# 3. EPL 2.0 present -> epl2 section.
# Exceptions ahead of the rules:
#   - the jersey/hk2 runtime keeps the combined section (their NOTICE.md
#     requires the mixed statement),
#   - named per-artifact sections (jdom2, dom4j) keep their own text,
#   - hdrhistogram (BSD-2 + Public Domain) keeps BSD-2 — the concrete
#     permissive text (R28 rule 4).
# Keys are GROUP prefixes: the group matches exactly or starts with
# key + '.'.
FAMILY_EXCEPTIONS = {
    'org.glassfish.jersey': 'combined',
    'org.glassfish.hk2': 'combined',
    'org.jdom': 'jdom',
    'org.dom4j': 'dom4j',
    'org.hdrhistogram': 'bsd2',
}


def fail(message) -> NoReturn:
    sys.stderr.write('FAIL: %s\n' % message)
    sys.exit(2)


def open_or_fail(path, what):
    try:
        return open(path)
    except OSError as err:
        fail('cannot read %s (%s): %s' % (what, path, err))


def section_for(ga, licenses):
    """Resolve the manifest section for one artifact. Fail-closed."""
    group = ga.split(':', 1)[0]
    for prefix, sec in FAMILY_EXCEPTIONS.items():
        if group == prefix or group.startswith(prefix + '.'):
            return sec
    names = set(licenses)
    if 'The Apache Software License, Version 2.0' in names:
        return 'apache'  # R28 rule 1 (jetty, jna, javassist, ...)
    if 'EDL 1.0' in names:
        return 'edl'     # R28 rule 2 (eclipse-collections, EclipseLink)
    if 'EPL 2.0' in names:
        return 'epl2'    # R28 rule 3 (jakarta duals)
    if len(names) == 1:
        name = next(iter(names))
        if name in NAME_TO_SECTION:
            return NAME_TO_SECTION[name]
    fail('unresolvable license for %s: %r — add a rule to '
         'the name→section tables of this generator'
         % (ga, sorted(names)))


def version_is_own(group):
    return (group.lower() == 'org.apache.hadoop'
            or group.lower().startswith('org.apache.hadoop.'))


def stem_from_filename(path_field):
    """Right-to-left digit-segment scan: drop everything from the last
    trailing segment that starts with a digit (the version). None when
    no version-looking segment exists (the row resists normalization)."""
    base = path_field.split('/')[-1]
    if base.endswith('.jar'):
        base = base[:-4]
    segments = base.split('-')
    for i in range(len(segments) - 1, 0, -1):
        if segments[i][:1].isdigit():
            return '-'.join(segments[:i]).lower()
    return None


def load_mappings(path, what):
    mapping = {}
    for line in open_or_fail(path, what):
        stripped = line.split('#')[0].split()
        if len(stripped) >= 2:
            mapping[stripped[0].lower()] = stripped[1].lower()
    return mapping


def load_archive_rows(paths, mappings_path):
    """Third-party rows of enumeration TSVs as (jar path, g:a, version),
    with the archive-jar-mappings promotion applied (same rule as the
    gate), plus the rows that still resist normalization (the gate's R36
    failure class) as 'tsv:line  path (stem ...)' strings — never dropped
    silently; the caller decides whether they fail. Shared with
    jar-layout-report.py."""
    mapping = load_mappings(mappings_path, 'archive-jar-mappings')
    rows = []
    unresolved = []
    for path in paths:
        for lineno, line in enumerate(open_or_fail(path, 'archive TSV'),
                                      start=1):
            fields = line.rstrip('\n').split('\t')
            if len(fields) != 5:
                fail('%s:%d: malformed TSV row (need 5 tab-separated '
                     'fields path/group/artifact/version/source, got %d): %r'
                     % (path, lineno, len(fields), line.rstrip('\n')))
            jar_path, group, artifact, version, _source = fields
            if group and not artifact:
                fail('%s:%d: malformed TSV row (group without artifact): %r'
                     % (path, lineno, fields))
            if not version:
                fail('%s:%d: TSV row without a version: %r'
                     % (path, lineno, fields))
            if not group:
                # Partial row: identify via the curated mapping, from the
                # manifest/filename stem.
                stem = (artifact or '').lower()
                if not stem:
                    stem = stem_from_filename(jar_path)
                if not stem or stem not in mapping:
                    unresolved.append('%s:%d  %s (stem %r)'
                                      % (path, lineno, jar_path, stem))
                    continue
                group, artifact = mapping[stem].split(':')[:2]
            if version_is_own(group):
                continue  # own jars are covered by the top-level LICENSE
            rows.append((jar_path, (group + ':' + artifact).lower(), version))
    return rows, unresolved


def load_archive_ga_versions(paths, mappings_path):
    """g:a -> set of shipping versions, from enumeration TSVs. Fails on
    rows that resist normalization."""
    rows, unresolved = load_archive_rows(paths, mappings_path)
    ga_versions = collections.defaultdict(set)
    for _jar_path, ga, version in rows:
        ga_versions[ga].add(version)
    if not rows:
        fail('zero enumerated third-party jars across %d TSV(s) — '
             'refusing to generate an empty manifest' % len(paths))
    if unresolved:
        sys.stderr.write(
            'FAIL: jars in the archive that resist g:a normalization '
            '(add a curated mapping to archive-jar-mappings):\n')
        for row in unresolved:
            sys.stderr.write('  %s\n' % row)
        sys.exit(2)
    return ga_versions


def load_inventory(path):
    """g:a -> (license names, inventory-resolved version)."""
    inventory = {}
    for line in open_or_fail(path, 'inventory'):
        match = INVENTORY_LINE.match(line.rstrip('\n'))
        if match:
            licenses = LICENSE_GROUPS.findall(match.group('lics'))
            inventory[(match.group('g') + ':' + match.group('a')).lower()] = (
                licenses, match.group('v'))
    if not inventory:
        fail('no parsable inventory lines in %s' % path)
    return inventory


def load_assets(path):
    """section<TAB>line — non-jar entries (native/source/webapp paths,
    npm packages) listed per section, in curated order."""
    assets = collections.defaultdict(list)
    for line in open_or_fail(path, 'assets file'):
        stripped = line.rstrip('\n')
        if not stripped or stripped.lstrip().startswith('#'):
            continue
        sec, _, text = stripped.partition('\t')
        if sec not in SECTION_KEYS:
            fail('assets file names unknown section %r' % sec)
        if not text:
            fail('assets line without tab-separated section: %r' % stripped)
        assets[sec].append(text)
    return assets


def load_embedded(path):
    """g:a coordinates embedded unrelocated in a shipping jar (R37);
    their licenses apply although no standalone jar is enumerated."""
    embedded = []
    for line in open_or_fail(path, 'shaded-jars'):
        stripped = line.split('#')[0].strip()
        if stripped:
            embedded.append(stripped.split()[0].lower())
    return embedded


# ---------------------------------------------------------------------------
# R13: derive the shipped licenses-binary/ directory.
#
# The texts/ store (dev-support/licenses/texts) is the single home of the
# license texts; the tarball's licenses-binary/ directory is GENERATED
# from the manifest, never hand-maintained. A store text ships iff the
# manifest references it: its slug (or the token recorded for it in
# licenses-binary-aliases, which doubles as the ship-reference map)
# appears in the manifest as a whole word, case-insensitive — the same
# rule the per-push R53 gate applies. Texts listed in texts-generic are
# resolve-store generics (apache-2.0, lgpl-2.1, ...) that the manifest
# does not reference per component; they never ship. Fail-closed: an
# empty store or manifest, a text whose file is missing or empty, an
# output directory that is already populated, or a titled section no
# shipped text covers is a hard error — a component whose license text
# ships nowhere must fail the build, never pass silently.
# ---------------------------------------------------------------------------

def load_aliases(path):
    """slug<TAB>kind<TAB>token rows (kind: component|section); returns
    slug -> token. Section rows ship-when-referenced but are exempt
    from the gate's staleness check; both kinds are emission inputs."""
    aliases = {}
    for lineno, raw in enumerate(open_or_fail(path, 'aliases file'), 1):
        line = raw.rstrip('\n')
        if not line.strip() or line.lstrip().startswith('#'):
            continue
        fields = line.split('\t')
        if len(fields) == 2:  # legacy component row
            fields = fields[:1] + ['component'] + fields[1:]
        if len(fields) != 3:
            fail('malformed line %d in %s (expected '
                 'slug<TAB>component|section<TAB>token): %r'
                 % (lineno, path, line))
        slug, kind, token = fields
        if kind not in ('component', 'section'):
            fail('malformed line %d in %s (kind must be component or '
                 'section, got %r)' % (lineno, path, kind))
        token = token.strip()
        # an empty token builds a vacuous whole-word pattern that
        # matches anything — the gate would silently pass
        if not token:
            fail('empty token on line %d in %s' % (lineno, path))
        if slug in aliases:
            fail('duplicate slug %r in %s' % (slug, path))
        aliases[slug] = token
    return aliases


def load_generic_texts(path):
    """Slugs of resolve-store generics that never ship (no manifest
    reference is expected for them)."""
    generic = set()
    for line in open_or_fail(path, 'texts-generic file'):
        stripped = line.split('#')[0].strip()
        if stripped:
            generic.add(stripped)
    return generic


def store_slugs(store_dir):
    """Sorted LICENSE-<slug>.txt slugs in the store; fail-closed on an
    empty or missing store."""
    if not os.path.isdir(store_dir):
        fail('license-text store not found: %s' % store_dir)
    slugs = sorted(
        name[len('LICENSE-'):-len('.txt')]
        for name in os.listdir(store_dir)
        if name.startswith('LICENSE-') and name.endswith('.txt'))
    if not slugs:
        fail('no LICENSE-*.txt texts in the store %s' % store_dir)
    return slugs


def token_pattern(token):
    """Whole-word, case-insensitive matcher for a reference token."""
    return re.compile(r'(?<![a-z0-9])' + re.escape(token.lower())
                      + r'(?![a-z0-9])', re.IGNORECASE)


def parse_manifest_sections(manifest_path):
    """Ordered (title, body_text) for every titled section of a
    generated manifest: a title line followed by a dashed underline.
    Everything before the first title (the Apache template and its
    component list) is not a section here."""
    lines = open_or_fail(manifest_path, 'manifest').read().splitlines()
    if not any(line.strip() for line in lines):
        fail('empty manifest %s' % manifest_path)
    titles = []
    for i, line in enumerate(lines):
        underline = lines[i + 1].strip() if i + 1 < len(lines) else ''
        if line.strip() and underline and len(underline) >= 4 \
                and set(underline) == {'-'}:
            titles.append((i, line.strip()))
    sections = []
    for idx, (line_no, title) in enumerate(titles):
        end = titles[idx + 1][0] if idx + 1 < len(titles) else len(lines)
        # body starts under the dashed underline
        body = '\n'.join(lines[line_no + 2:end])
        sections.append((title, body))
    return sections


def derive_shipped_slugs(manifest_path, store_dir, aliases, generic):
    """The store slugs the manifest references (the R53 ship set)."""
    with open_or_fail(manifest_path, 'manifest') as fh:
        manifest_text = fh.read()
    if not manifest_text.strip():
        fail('empty manifest %s' % manifest_path)
    slugs = store_slugs(store_dir)
    unknown = sorted(set(aliases) - set(slugs))
    if unknown:
        fail('licenses-binary-aliases rows with no text in %s: %s'
             % (store_dir, ', '.join(unknown)))
    overlap = sorted(set(aliases) & set(generic))
    if overlap:
        fail('slugs both aliased and listed in texts-generic: %s'
             % ', '.join(overlap))
    unknown_generic = sorted(set(generic) - set(slugs))
    if unknown_generic:
        fail('texts-generic rows with no text in %s: %s'
             % (store_dir, ', '.join(unknown_generic)))
    shipped = set()
    for slug in slugs:
        if slug in generic:
            continue
        token = aliases.get(slug, slug)
        if token_pattern(token).search(manifest_text):
            shipped.add(slug)
    # R53, fail-closed direction: every titled section of the
    # manifest must have its license text shipped — covered by a text
    # whose token matches the section title or any line of its body.
    sections = parse_manifest_sections(manifest_path)
    if not sections:
        fail('manifest %s carries no titled license sections' % manifest_path)
    for title, body in sections:
        section_text = title + '\n' + body
        if not any(
                token_pattern(aliases.get(slug, slug)).search(section_text)
                for slug in shipped):
            fail('section %r has no shipped license text — every component '
                 'listed under it ships with no text (add the section text '
                 'to the store, or a licenses-binary-aliases row)'
                 % title)
    return shipped


def emit_licenses_dir(manifest_path, store_dir, aliases_path, generic_path,
                      out_dir):
    """Copy the referenced texts from the store into a fresh out_dir."""
    shipped = derive_shipped_slugs(manifest_path, store_dir,
                                   load_aliases(aliases_path),
                                   load_generic_texts(generic_path))
    if os.path.exists(out_dir) and os.listdir(out_dir):
        fail('licenses-dir %s exists and is not empty — refuse to '
             'overwrite' % out_dir)
    os.makedirs(out_dir, exist_ok=True)
    for slug in sorted(shipped):
        src = os.path.join(store_dir, 'LICENSE-%s.txt' % slug)
        if not os.path.isfile(src) or os.path.getsize(src) == 0:
            fail('store text for %r is missing or empty: %s' % (slug, src))
        shutil.copyfile(src, os.path.join(out_dir,
                                          'LICENSE-%s.txt' % slug))
    print('emitted %d license text(s) into %s' % (len(shipped), out_dir))
    return shipped


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    # curated inputs live in the curation/ sibling directory; the text
    # store stays at the tooling root
    curated = os.path.normpath(os.path.join(here, '..', 'curation'))
    store = os.path.normpath(os.path.join(here, '..', 'texts'))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', default=None,
                        help='THIRD-PARTY.txt from the collected-licenses artifact')
    parser.add_argument('--archive-tsv', default=None, nargs='+',
                        help='archive-jars.tsv files (one or more archives)')
    parser.add_argument('--mappings',
                        default=os.path.join(curated, 'archive-jar-mappings'),
                        help='archive-jar-mappings curation file')
    parser.add_argument('--shaded', default=os.path.join(curated, 'shaded-jars'),
                        help='shaded-jars curation file (embedded coordinates)')
    parser.add_argument('--assets', default=os.path.join(curated, 'manifest-assets.txt'),
                        help='non-jar entries, "section<TAB>line" per line')
    parser.add_argument('--template', default=os.path.join(curated, 'manifest-template.txt'),
                        help='content emitted before the artifact lists '
                             '(ASF preamble + Apache-2.0 text + bundle wording)')
    parser.add_argument('--out', default=None, help='output LICENSE-binary path')
    # R13: derive the shipped licenses-binary/ directory from a manifest
    parser.add_argument('--emit-licenses-dir-only', action='store_true',
                        help='emit only the licenses-binary/ directory '
                             '(no manifest generation): derive the ship set '
                             'from --manifest and copy the texts from the '
                             'store')
    parser.add_argument('--manifest', default=None,
                        help='LICENSE-binary to derive the ship set from '
                             '(--emit-licenses-dir-only, or --licenses-dir '
                             'in full mode)')
    parser.add_argument('--licenses-dir', default=None,
                        help='output directory for the generated '
                             'licenses-binary/ texts (must not exist or '
                             'be empty)')
    parser.add_argument('--store', default=store,
                        help='the versioned license-text store')
    parser.add_argument('--aliases',
                        default=os.path.join(curated, 'licenses-binary-aliases'),
                        help='licenses-binary-aliases curation file '
                             '(slug<TAB>manifest token)')
    parser.add_argument('--generic', default=os.path.join(curated, 'texts-generic'),
                        help='texts-generic file (store slugs that never '
                             'ship)')
    args = parser.parse_args()

    if args.emit_licenses_dir_only:
        if not args.manifest or not args.licenses_dir:
            parser.error('--emit-licenses-dir-only requires --manifest '
                         'and --licenses-dir')
        emit_licenses_dir(args.manifest, args.store, args.aliases,
                          args.generic, args.licenses_dir)
        return
    if not args.inventory or not args.archive_tsv or not args.out:
        parser.error('manifest generation requires --inventory, '
                     '--archive-tsv and --out')

    ga_versions = load_archive_ga_versions(args.archive_tsv, args.mappings)
    inventory = load_inventory(args.inventory)
    assets = load_assets(args.assets)

    # Shaded-jar embedded coordinates (R37): artifacts whose classes ship
    # unrelocated inside a top-level jar. Some also ship standalone in
    # another tree — possibly at a different version (protobuf-java 2.5.0
    # standalone, 3.25.5 embedded in hadoop-client-runtime) — so the
    # inventory-resolved version merges into the shipping-version set
    # rather than being skipped when the GA is already enumerated.
    for ga in load_embedded(args.shaded):
        if ga not in inventory:
            fail('shaded-jars lists %s but the inventory does not know it' % ga)
        licenses, version = inventory[ga]
        ga_versions.setdefault(ga, set()).add(version)

    sections = collections.defaultdict(list)
    for ga in sorted(ga_versions):
        if ga not in inventory:
            fail('%s ships in an archive but is absent from the collected '
                 'inventory — the graph never saw it' % ga)
        licenses, _ = inventory[ga]
        sections[section_for(ga, licenses)].append(
            (ga, sorted(ga_versions[ga])))

    try:
        out = open(args.out, 'w')
    except OSError as err:
        fail('cannot write output (%s): %s' % (args.out, err))
    with out:
        for line in open_or_fail(args.template, 'template'):
            out.write(line)
        first_section = True
        for key, title in SECTIONS:
            entries = sections.get(key, [])
            if not entries and key not in assets:
                continue
            if key == 'apache':
                out.write('\nThe following components are included:\n\n')
            else:
                if first_section:
                    out.write('\n'.join(INTERLUDE) + '\n')
                out.write('\n%s\n%s\n' % (title, '-' * max(len(title), 4)))
            if key in SECTION_NOTES:
                out.write('\n%s\n' % SECTION_NOTES[key])
            if key in assets:
                out.write('\n')
                for text in assets[key]:
                    out.write(text + '\n')
            if entries:
                out.write('\n')
            for ga, versions in entries:
                for version in versions:
                    out.write('%s:%s\n' % (ga, version))
            first_section = False
    ga_count = sum(len(v) for v in sections.values())
    version_count = sum(len(versions)
                        for entries in sections.values()
                        for _, versions in entries)
    print('generated %s: %d g:a (%d version lines), %d asset lines, '
          '%d sections' % (args.out, ga_count, version_count,
                           sum(len(v) for v in assets.values()),
                           len(sections)))
    if args.licenses_dir:
        # R13: emit the shipped license-text directory from the freshly
        # generated manifest (must agree with what the committed manifest
        # derives — the R40/R53 gates check both).
        emit_licenses_dir(args.out, args.store, args.aliases,
                          args.generic, args.licenses_dir)


if __name__ == '__main__':
    main()
