import pytest
import os
import json
from pathlib import Path
from pyhartig.mapping.MappingParser import MappingParser
from pyhartig.algebra.Terms import IRI, Literal
from pyhartig.namespaces import XSD_STRING
from datetime import datetime


class TestGithubGitlabUseCase:
    """
    Self-contained integration test for the GitHub/GitLab Fusion Use Case.
    Located in: tests/use_cases/github_gitlab/
    """

    @pytest.fixture
    def use_case_dir(self):
        """Returns the directory containing this test file."""
        return Path(__file__).parent

    @pytest.fixture
    def debug_logger(self):
        def log(section, message):
            print(f"\n{'=' * 80}")
            print(f"[DEBUG] {section}")
            print(f"{'-' * 80}")
            print(message)
            print(f"{'=' * 80}\n")

        return log

    def test_fusion_execution(self, use_case_dir, debug_logger):
        """
        Executes the fusion mapping.
        Ensures relative paths in RML are resolved correctly by switching CWD.
        """
        mapping_file = "data/fusion_mapping.ttl"
        github_json = "data/github_issues.json"
        gitlab_json = "data/gitlab_issues.json"

        # 1. Verify Environment
        # Ensure all files are present in the use case folder
        missing_files = []
        if not (use_case_dir / mapping_file).exists(): missing_files.append(mapping_file)
        if not (use_case_dir / github_json).exists(): missing_files.append(github_json)
        if not (use_case_dir / gitlab_json).exists(): missing_files.append(gitlab_json)

        if missing_files:
            pytest.fail(f"Missing files in {use_case_dir}: {missing_files}")

        debug_logger("Configuration",
                     f"Working Directory: {use_case_dir}\n"
                     f"Mapping: {mapping_file}\n"
                     f"Sources: {github_json}, {gitlab_json}")

        # 2. Execute Pipeline (Context Manager for safe CWD switching)
        # We switch to the directory so the Parser finds "github_issues.json" locally
        original_cwd = os.getcwd()
        try:
            os.chdir(use_case_dir)

            parser = MappingParser(mapping_file)
            pipeline = parser.parse()
            results = pipeline.execute()

            graph = parser.explain()
            debug_logger("Pipeline Explanation", graph)

            parser.save_explanation("pipeline_explanation.json", format="json")
            parser.save_explanation("pipeline_explanation.txt", format="text")

        finally:
            os.chdir(original_cwd)

        debug_logger("Results", f"Total Triples Generated: {len(results)}")

        # Debug: Print first few results to understand structure
        debug_logger("Sample Results", "\n".join([str(row) for row in results[:5]]))

        # Save results to files for reporting
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = use_case_dir / "output"
        output_dir.mkdir(exist_ok=True)

        # 1. Save results as JSON
        json_output = output_dir / f"results_{timestamp}.json"
        results_data = []
        for row in results:
            result_entry = {}
            for key, value in row.items():
                if isinstance(value, IRI):
                    result_entry[key] = {"type": "IRI", "value": value.value}
                elif isinstance(value, Literal):
                    result_entry[key] = {
                        "type": "Literal",
                        "value": value.lexical_form,
                        "datatype": value.datatype_iri if value.datatype_iri else None
                    }
                else:
                    result_entry[key] = {"type": "string", "value": str(value)}
            results_data.append(result_entry)

        with open(json_output, 'w', encoding='utf-8') as f:
            json.dump(results_data, f, indent=2, ensure_ascii=False)

        debug_logger("Output Files", f"JSON results saved to: {json_output}")

        # 2. Save results as N-Triples (RDF format)
        nt_output = output_dir / f"graph_{timestamp}.nt"
        with open(nt_output, 'w', encoding='utf-8') as f:
            for row in results:
                subject = row.get("subject")
                predicate = row.get("predicate")
                obj = row.get("object")

                if subject and predicate and obj:
                    # Format subject
                    if isinstance(subject, IRI):
                        subj_str = f"<{subject.value}>"
                    else:
                        subj_str = f"<{str(subject)}>"

                    # Format predicate
                    if isinstance(predicate, IRI):
                        pred_str = f"<{predicate.value}>"
                    else:
                        pred_str = f"<{str(predicate)}>"

                    # Format object
                    if isinstance(obj, IRI):
                        obj_str = f"<{obj.value}>"
                    elif isinstance(obj, Literal):
                        # Escape quotes and backslashes
                        lex = obj.lexical_form.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
                        if obj.datatype_iri and obj.datatype_iri != XSD_STRING.value:
                            obj_str = f'"{lex}"^^<{obj.datatype_iri}>'
                        else:
                            obj_str = f'"{lex}"'
                    else:
                        # Fallback for strings
                        obj_val = str(obj).replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
                        obj_str = f'"{obj_val}"'

                    f.write(f"{subj_str} {pred_str} {obj_str} .\n")

        debug_logger("Output Files", f"N-Triples graph saved to: {nt_output}")

        # 3. Save a summary report
        summary_output = output_dir / f"summary_{timestamp}.txt"
        with open(summary_output, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("GitHub/GitLab Fusion Use Case - Execution Report\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Execution Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Mapping File: {mapping_file}\n")
            f.write(f"Source Files: {github_json}, {gitlab_json}\n\n")

            f.write(f"Total Triples Generated: {len(results)}\n\n")

            # Collect statistics
            subjects = set()
            predicates = set()
            creators = set()
            sources = set()

            for row in results:
                subj = row.get("subject")
                pred = row.get("predicate")
                obj = row.get("object")

                if subj:
                    if isinstance(subj, IRI):
                        subjects.add(subj.value)
                    else:
                        subjects.add(str(subj))

                if pred:
                    pred_str = pred.value if isinstance(pred, IRI) else str(pred)
                    predicates.add(pred_str)

                    if "creator" in pred_str.lower():
                        val = obj.lexical_form if isinstance(obj, Literal) else (obj.value if isinstance(obj, IRI) else str(obj))
                        creators.add(val)

                    if "source" in pred_str.lower():
                        val = obj.lexical_form if isinstance(obj, Literal) else (obj.value if isinstance(obj, IRI) else str(obj))
                        sources.add(val)

            f.write("Statistics:\n")
            f.write("-" * 80 + "\n")
            f.write(f"Unique Subjects: {len(subjects)}\n")
            f.write(f"Unique Predicates: {len(predicates)}\n")
            f.write(f"Creators Found: {len(creators)}\n")
            f.write(f"Sources Found: {len(sources)}\n\n")

            f.write("Predicates Used:\n")
            for pred in sorted(predicates):
                f.write(f"  - {pred}\n")
            f.write("\n")

            f.write("Creators:\n")
            for creator in sorted(creators):
                f.write(f"  - {creator}\n")
            f.write("\n")

            f.write("Sources:\n")
            for source in sorted(sources):
                f.write(f"  - {source}\n")
            f.write("\n")

            f.write("Sample Triples (first 10):\n")
            f.write("-" * 80 + "\n")
            for i, row in enumerate(results[:10]):
                f.write(f"\n[Triple {i+1}]\n")
                for key, value in row.items():
                    if isinstance(value, IRI):
                        f.write(f"  {key}: <{value.value}>\n")
                    elif isinstance(value, Literal):
                        f.write(f"  {key}: \"{value.lexical_form}\"")
                        if value.datatype_iri and value.datatype_iri != XSD_STRING.value:
                            f.write(f"^^<{value.datatype_iri}>")
                        f.write("\n")
                    else:
                        f.write(f"  {key}: {value}\n")

        debug_logger("Output Files", f"Summary report saved to: {summary_output}")

        # 3. Validate Fusion Logic
        creators = set()
        sources = set()

        for row in results:
            pred = str(row.get("predicate", ""))
            obj = row.get("object")
            # Helper to extract string value
            val = obj.value if hasattr(obj, 'value') else (
                obj.lexical_form if hasattr(obj, 'lexical_form') else str(obj))

            if "creator" in pred:
                creators.add(val)
            if "source" in pred:
                sources.add(val)

        debug_logger("Analysis",
                     f"Creators Found: {creators}\n"
                     f"Sources Found: {sources}")

        # Assertions
        assert "Armotik" in creators, "Missing GitHub data (Armotik)"
        # Check for GitLab user (flexible match)
        assert any("import_user" in c or "Import User" in c for c in creators), "Missing GitLab data"

        assert "GitHub" in sources
        assert "GitLab" in sources

        debug_logger("Success", "Use Case Validated: Unified Graph Created.")
