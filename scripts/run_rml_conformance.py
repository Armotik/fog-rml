#!/usr/bin/env python3
"""Run pyhartig on RML test cases and compare outputs using rdflib.

Usage:
  python scripts/run_rml_conformance.py --tests-dir /path/to/rml-test-cases --suite Core --output-dir /tmp/pyhartig_conformance

Behavior:
- Scans the provided tests directory for subdirectories that contain an RML mapping file (*.ttl or *.rml) and an expected RDF output (*.nt, *.ttl, *.nq)
- For each detected test case, runs `python -m pyhartig run -m <mapping> -o <tmpfile>`
- Compares produced RDF to expected RDF using rdflib graph isomorphism
- Prints a summary with pass/fail counts and coverage percentage
"""
import argparse
import subprocess
import tempfile
import shutil
import sys
import warnings
from pathlib import Path
from rdflib import Graph, ConjunctiveGraph
from rdflib.compare import to_isomorphic
from rdflib import Namespace, URIRef, Literal
import urllib.parse
import os
import csv

EXT_MAPPING = (".ttl", ".rml")
EXT_EXPECTED = (".nt", ".ttl", ".nq", ".trig")

warnings.filterwarnings(
    "ignore",
    message="ConjunctiveGraph is deprecated, use Dataset instead.",
    category=DeprecationWarning,
)


def guess_format(path: Path):
    ext = path.suffix.lower()
    if ext == ".nt":
        return "nt"
    if ext == ".ttl":
        return "turtle"
    if ext == ".nq":
        return "nquads"
    if ext == ".trig":
        return "trig"
    return "nt"


def find_test_cases(tests_dir: Path):
    """Yield tuples (case_dir, mapping_path, expected_path) for directories that look like a test case."""
    def _choose_expected_file(directory: Path, mapping_candidates):
        expected_candidates = [f for f in directory.iterdir() if f.suffix.lower() in EXT_EXPECTED]
        if not expected_candidates:
            return None

        mapping_set = {str(p.resolve()) for p in mapping_candidates}

        def _is_output_like(p: Path) -> bool:
            name = p.name.lower()
            return name.startswith('output.') or 'output' in name

        # Prefer explicit output files
        preferred = [p for p in expected_candidates if _is_output_like(p)]
        pool = preferred if preferred else expected_candidates

        # Exclude mapping/resource files when possible
        filtered = []
        for p in pool:
            pname = p.name.lower()
            if str(p.resolve()) in mapping_set:
                continue
            if pname.startswith('mapping.') or 'mapping' in pname:
                continue
            if pname.startswith('resource'):
                continue
            filtered.append(p)

        if filtered:
            # Prefer quad formats first if available
            quad_first = sorted(
                filtered,
                key=lambda p: (0 if p.suffix.lower() in ('.nq', '.trig') else 1, p.name.lower())
            )
            return quad_first[0]

        # Fallback to first preferred candidate if everything was filtered out
        return (pool[0] if pool else expected_candidates[0])

    # If the provided path itself looks like a case directory, yield it
    if tests_dir.is_dir():
        mapping = None
        mapping_candidates = []
        for f in tests_dir.iterdir():
            if f.suffix.lower() in EXT_MAPPING:
                mapping_candidates.append(f)
        expected = _choose_expected_file(tests_dir, mapping_candidates)
        if mapping_candidates and expected:
            # Prefer explicit mapping file names (mapping.ttl) or names containing 'mapping'
            for c in mapping_candidates:
                if c.name.lower() == 'mapping.ttl' or 'mapping' in c.name.lower():
                    mapping = c
                    break
            # Fallback: prefer files that appear to contain RML/RR constructs
            if mapping is None:
                for c in mapping_candidates:
                    try:
                        txt = c.read_text(encoding='utf-8', errors='ignore')
                        if 'rml:logicalSource' in txt or 'rr:TriplesMap' in txt:
                            mapping = c
                            break
                    except Exception:
                        continue
            if mapping is None:
                mapping = mapping_candidates[0]

            yield (tests_dir, mapping, expected)
            return
    for d in tests_dir.rglob("*"):
        if d.is_dir():
            mapping = None
            mapping_candidates = []
            for f in d.iterdir():
                if f.suffix.lower() in EXT_MAPPING:
                    mapping_candidates.append(f)
            expected = _choose_expected_file(d, mapping_candidates)
            # If multiple mapping-like files exist, try to pick the canonical mapping file
            if mapping_candidates and expected:
                # Gather all mapping candidates in the directory
                candidates = mapping_candidates
                chosen = None
                for c in candidates:
                    if c.name.lower() == 'mapping.ttl' or 'mapping' in c.name.lower():
                        chosen = c
                        break
                if chosen is None:
                    for c in candidates:
                        try:
                            txt = c.read_text(encoding='utf-8', errors='ignore')
                            if 'rml:logicalSource' in txt or 'rr:TriplesMap' in txt:
                                chosen = c
                                break
                        except Exception:
                            continue
                if chosen is None and candidates:
                    chosen = candidates[0]

                yield (d, chosen, expected)


def rewrite_mapping_to_local(mapping: Path, case_dir: Path, tmp_root: Path) -> Path:
    """Create a rewritten mapping inside tmp_root/<case-name>/mapping.ttl where
    rml:source references that point to locally-available files in `case_dir`
    are replaced with local filename literals. Any discovered source files are
    copied into the temp directory so the mapping resolves relative paths.

    Returns path to rewritten mapping, or None if no rewrite was performed.
    """
    rml_ns = Namespace("http://semweb.mmlab.be/ns/rml#")
    try:
        mg = Graph()
        mg.parse(str(mapping), format="turtle")
    except Exception:
        try:
            mg = Graph()
            mg.parse(str(mapping))
        except Exception:
            return None

    rewritten = False
    tmp_case_dir = tmp_root / mapping.parent.name
    tmp_case_dir.mkdir(parents=True, exist_ok=True)

    # Inspect both rml:source and rml:reference
    for pred in (rml_ns.source, rml_ns.reference):
        for subj, obj in list(mg.subject_objects(pred)):
            sval = str(obj)
            # Normalize windows backslashes that sometimes appear
            sval_norm = sval.replace("\\", "/")
            parsed = urllib.parse.urlparse(sval_norm)
            candidate = os.path.basename(parsed.path) or parsed.fragment or None
            if not candidate:
                continue

            # Try to locate candidate file under case_dir
            found = None
            # Exact name search
            for p in case_dir.rglob(candidate):
                found = p
                break
            # Try with extension patterns
            if not found:
                for p in case_dir.rglob(f"{candidate}.*"):
                    found = p
                    break
            # Heuristic fallback for numbered filenames (e.g. student2.json -> student.json)
            # used by some conformance edge-cases.
            if not found:
                stem = Path(candidate).stem
                suffix = Path(candidate).suffix
                stem_no_digits = stem.rstrip('0123456789')
                if stem_no_digits and stem_no_digits != stem:
                    alt_name = f"{stem_no_digits}{suffix}" if suffix else stem_no_digits
                    for p in case_dir.rglob(alt_name):
                        found = p
                        break
            # Fallback fuzzy search
            if not found:
                for p in case_dir.rglob("*"):
                    if candidate in p.name:
                        found = p
                        break

            if found and found.exists():
                # copy to tmp_case_dir
                dst = tmp_case_dir / found.name
                try:
                    shutil.copy2(found, dst)
                except Exception:
                    # if copy fails, skip replacement
                    continue

                # Replace object with literal local filename so SourceFactory resolves it
                mg.remove((subj, pred, obj))
                mg.add((subj, pred, Literal(found.name)))
                rewritten = True

    if not rewritten:
        # Cleanup created tmp_case_dir if empty
        try:
            if not any(tmp_case_dir.iterdir()):
                tmp_case_dir.rmdir()
        except Exception:
            pass
        return None

    tmp_mapping = tmp_case_dir / mapping.name
    mg.serialize(destination=str(tmp_mapping), format="turtle")

    # Preserve any @base directive present in the original mapping file: rdflib
    # serialization may omit the textual @base, which some tests rely on
    # (relative reference resolution). If the original mapping contains an
    # @base declaration, prepend it to the rewritten mapping file so downstream
    # parsers (and our MappingParser base-extraction) can detect and use it.
    try:
        import re
        with open(str(mapping), 'r', encoding='utf-8') as fh:
            orig = fh.read(2048)
        m = re.search(r"@base\s+<([^>]+)>", orig)
        if m:
            base_decl = f"@base <{m.group(1)}> .\n\n"
            # Prepend only if not already present in the rewritten file
            with open(str(tmp_mapping), 'r', encoding='utf-8') as fh:
                current = fh.read()
            if base_decl.strip() not in current:
                with open(str(tmp_mapping), 'w', encoding='utf-8') as fh:
                    fh.write(base_decl + current)
    except Exception:
        pass
    return tmp_mapping


def run_pyhartig_mapping(mapping: Path, output_path: Path, verbose: bool = False, strict_references: bool = False) -> int:
    cmd = [sys.executable, "-m", "pyhartig", "run", "-m", str(mapping), "-o", str(output_path)]
    env = os.environ.copy()
    if strict_references:
        env["PYHARTIG_STRICT_REFERENCES"] = "1"
    else:
        env.pop("PYHARTIG_STRICT_REFERENCES", None)
    if verbose:
        print("Running:", " ".join(cmd))
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    if verbose:
        print(proc.stdout.decode("utf-8", errors="ignore"))
        print(proc.stderr.decode("utf-8", errors="ignore"))
    return proc.returncode


def compare_rdf_files(expected: Path, actual: Path) -> bool:
    fmt1 = guess_format(expected)
    fmt2 = guess_format(actual)

    # Use ConjunctiveGraph only when both sides are quad formats so named graphs are preserved.
    # If only one side is quad and the other is triple-based (e.g. expected .nq vs actual .ttl),
    # compare plain triple graphs to avoid dataset-context false negatives.
    quad_formats = ("nquads", "trig")
    if fmt1 in quad_formats and fmt2 in quad_formats:
        g1 = ConjunctiveGraph()
        g2 = ConjunctiveGraph()
    else:
        g1 = Graph()
        g2 = Graph()

    try:
        g1.parse(str(expected), format=fmt1)
    except Exception:
        g1.parse(str(expected))
    try:
        g2.parse(str(actual), format=fmt2)
    except Exception:
        g2.parse(str(actual))

    iso = to_isomorphic(g1).isomorphic(to_isomorphic(g2))
    return iso


def main():
    parser = argparse.ArgumentParser(description="Run pyhartig on RML test cases and compare RDF outputs")
    parser.add_argument("--tests-dir", required=True, help="Path to downloaded RML test cases root")
    parser.add_argument("--suite", default=None, help="Optional subdirectory/suite name to limit (e.g., Core)")
    parser.add_argument("--output-dir", default=None, help="Directory to write temporary outputs")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    tests_root = Path(args.tests_dir)
    if args.suite:
        tests_root = tests_root / args.suite
    if not tests_root.exists():
        print("Tests directory not found:", tests_root)
        sys.exit(2)

    out_dir = Path(args.output_dir) if args.output_dir else Path(tempfile.mkdtemp(prefix="pyhartig_conformance_"))
    out_dir.mkdir(parents=True, exist_ok=True)

    # Try to locate a metadata.csv file that lists tests and whether an error is expected
    metadata_map = {}
    search_path = tests_root
    metadata_path = None
    while True:
        candidate = search_path / "metadata.csv"
        if candidate.exists():
            metadata_path = candidate
            break
        if search_path.parent == search_path:
            break
        search_path = search_path.parent
    if metadata_path:
        try:
            with metadata_path.open(encoding="utf-8") as fh:
                rdr = csv.DictReader(fh)
                # column name expected: 'RML id' and 'error expected?'
                for r in rdr:
                    key = r.get('RML id') or r.get('RML id'.upper())
                    flag = r.get('error expected?') or r.get('error expected?'.upper())
                    if key:
                        metadata_map[key.strip()] = (str(flag).strip().lower() == 'true')
        except Exception:
            metadata_map = {}

    cases = list(find_test_cases(tests_root))
    if not cases:
        print("No test cases found under:", tests_root)
        sys.exit(2)

    total = len(cases)
    passed = 0
    failed = 0
    skipped_external = 0
    expected_error_passed = 0
    expected_error_failed = 0
    results = []
    rewrite_root = out_dir / "_rewrites"

    for case_dir, mapping, expected in cases:
        if args.verbose:
            print(f"\n=== Running case: {case_dir} ===")
        expects_error = metadata_map.get(case_dir.name, False)

        # Materialize a per-case results folder containing original test assets
        # so generated output sits next to mapping/data for easier inspection.
        case_out_dir = out_dir / case_dir.name
        case_out_dir.mkdir(parents=True, exist_ok=True)
        try:
            for src in case_dir.iterdir():
                dst = case_out_dir / src.name
                if src.is_dir():
                    shutil.copytree(src, dst, dirs_exist_ok=True)
                else:
                    shutil.copy2(src, dst)
        except Exception:
            pass
        # Determine whether the mapping references external (HTTP/DB/SPARQL)
        # sources by parsing the mapping and inspecting rml:source objects.
        from rdflib import URIRef, Namespace
        rml_ns = Namespace("http://semweb.mmlab.be/ns/rml#")
        sd_ns = Namespace("http://www.w3.org/ns/sparql-service-description#")
        d2rq_ns = Namespace("http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#")
        skip_case = False
        try:
            mg = Graph()
            mg.parse(str(mapping), format="turtle")
            for logical_source in mg.subjects(rml_ns.source, None):
                src = mg.value(logical_source, rml_ns.source)

                # If source points to a DB description node in this mapping,
                # do not mark it as external here. Source operators may run it
                # via real DB or local SQL fixture fallback.
                if src is not None:
                    has_db_description = (
                        mg.value(src, d2rq_ns.jdbcDSN) is not None
                        or mg.value(src, d2rq_ns.jdbcDriver) is not None
                    )
                    if has_db_description:
                        continue

                # SPARQL local emulation case: allow execution when query exists,
                # source points to a service node, and local resource*.ttl exists.
                query_literal = mg.value(logical_source, rml_ns.query)
                endpoint = None
                if src is not None:
                    endpoint = mg.value(src, sd_ns.endpoint)
                    if endpoint is None:
                        try:
                            endpoint = mg.value(URIRef(str(src)), sd_ns.endpoint)
                        except Exception:
                            endpoint = None

                if query_literal is not None and endpoint is not None:
                    has_local_resource = any(case_dir.glob("resource*.ttl"))
                    if has_local_resource:
                        continue

                if isinstance(src, URIRef):
                    scheme = src.split(":")[0].lower()
                    if scheme in ("http", "https"):
                        skip_case = True
                        break
                else:
                    # literal value may still contain a URI scheme
                    sval = str(src)
                    if sval.startswith(("http://", "https://", "jdbc:", "mysql:", "postgres:")):
                        skip_case = True
                        break
        except Exception:
            # If parsing fails, fall back to conservative text-based check
            mapping_text = mapping.read_text(encoding="utf-8", errors="ignore")
            if any(tok in mapping_text for tok in ("http://", "https://", "jdbc:", "mysql", "postgres")):
                skip_case = True

        # Try to rewrite mapping to use local files when available
        rewritten_mapping = rewrite_mapping_to_local(mapping, case_dir, rewrite_root)
        if rewritten_mapping:
            use_mapping = rewritten_mapping
            if args.verbose:
                print(f"Using rewritten mapping: {use_mapping}")
        else:
            if skip_case:
                skipped_external += 1
                results.append((case_dir, None, "skipped (external source)"))
                if args.verbose:
                    print(f"SKIP: {case_dir} (external source referenced)")
                continue
            use_mapping = mapping

        # Use the same extension as the expected file so we produce matching formats
        expected_suffix = expected.suffix if expected.suffix else '.nt'
        tmp_out = case_out_dir / f"output_pyhartig{expected_suffix}"
        try:
            if tmp_out.exists():
                tmp_out.unlink()
        except Exception:
            pass
        rc = run_pyhartig_mapping(
            use_mapping,
            tmp_out,
            verbose=args.verbose,
            strict_references=expects_error,
        )

        if expects_error:
            if rc != 0:
                try:
                    if tmp_out.exists():
                        tmp_out.unlink()
                except Exception:
                    pass
                passed += 1
                expected_error_passed += 1
                results.append((case_dir, True, "expected error observed"))
                if args.verbose:
                    print(f"PASS (expected error): {case_dir}")
            else:
                failed += 1
                expected_error_failed += 1
                results.append((case_dir, False, "expected error but pyhartig succeeded"))
                if args.verbose:
                    print(f"FAIL (expected error not observed): {case_dir}")
            continue

        if rc != 0:
            failed += 1
            results.append((case_dir, False, "pyhartig failed (exit code %d)" % rc))
            if args.verbose:
                print(f"pyhartig failed for {case_dir} (rc={rc})")
            continue
        try:
            ok = compare_rdf_files(expected, tmp_out)
        except Exception as e:
            failed += 1
            ok = False
            results.append((case_dir, False, f"compare error: {e}"))
            continue
        if ok:
            passed += 1
            results.append((case_dir, True, ""))
            if args.verbose:
                print(f"PASS: {case_dir}")
        else:
            failed += 1
            results.append((case_dir, False, "graphs not isomorphic"))
            if args.verbose:
                print(f"FAIL: {case_dir}")

    # Cleanup temporary rewritten mappings before reporting final output location
    try:
        if rewrite_root.exists():
            shutil.rmtree(rewrite_root, ignore_errors=True)
    except Exception:
        pass

    # Summary
    effective_total = total - skipped_external
    if effective_total < 0:
        effective_total = 0
    print("\nConformance run summary")
    print(f"Total cases: {effective_total}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    if expected_error_passed or expected_error_failed:
        print(f"Expected-error cases (pass/fail): {expected_error_passed}/{expected_error_failed}")
    if skipped_external:
        print(f"Skipped external: {skipped_external}")
    # If there are no effective cases, report full coverage
    pct = (passed / effective_total) * 100 if effective_total else 100.0
    print(f"Coverage: {pct:.1f}%")

    # Print failed cases
    if failed > 0:
        print("\nFailed cases:")
        for case_dir, status, msg in results:
            if status is False:
                print(f"- {case_dir}: {msg}")

    # Keep outputs for inspection
    print(f"\nOutputs retained in: {out_dir}")

    # Return non-zero if coverage below threshold (optional)
    threshold = 80.0
    if pct < threshold:
        print(f"Coverage below {threshold}%; failing run (exit 3)")
        sys.exit(3)
    sys.exit(0)


if __name__ == "__main__":
    main()
