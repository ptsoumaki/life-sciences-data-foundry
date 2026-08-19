"""
Module: mlflow_tracker.py
Description: GxP governance module providing Great Expectations data contract evaluation
             and MLflow audit lineage for 21 CFR Part 11 compliance. Computes SHA-256
             checksums for both input datasets and rule specifications to guarantee
             immutable provenance records.

Public API:
    compute_sha256          -- File integrity checksum for audit records.
    load_dataset            -- Multi-format dataset loader (CSV/TSV/Parquet/JSON).
    evaluate_data_contract  -- GxP contract gate with MLflow lineage logging.
    run_governance_pipeline -- End-to-end governance pipeline entry point.
"""

import json
import os
import sys
import tempfile
from typing import Any

import great_expectations as ge
import mlflow
import pandas as pd

from governance.crypto import compute_sha256


def generate_default_clinical_sample(target_path: str) -> pd.DataFrame:
    """Writes a minimal synthetic OMOP CDM v5.4 PERSON dataset to disk and returns it.

    Used as a fallback by load_dataset when the requested path does not exist,
    allowing smoke-tests and demos to run without external data files.
    """
    sample_data = {
        "person_id": [1001, 1002, 1003, 1004],
        "gender_concept_id": [8507, 8532, 8507, 8532],
        "year_of_birth": [1985, 1992, 1978, 2001],
        "birth_datetime": [
            "1985-06-15T08:30:00Z",
            "1992-11-03T14:20:00Z",
            "1978-01-22T19:00:00Z",
            "2001-04-18T11:45:00Z",
        ],
        "race_concept_id": [8527, 8527, 8527, 8527],
        "ethnicity_concept_id": [38003564, 38003564, 38003564, 38003564],
    }
    df = pd.DataFrame(sample_data)
    os.makedirs(os.path.dirname(os.path.abspath(target_path)), exist_ok=True)
    df.to_csv(target_path, index=False)
    print(f"[INFO] Generated synthetic clinical test data at: {target_path}")
    return df


def load_dataset(data_path: str) -> pd.DataFrame:
    """Loads dataset from CSV, TSV, Parquet, or JSON format into Pandas DataFrame."""
    if not os.path.exists(data_path):
        # Restrict synthetic fallback to explicit sample paths; raising for any other missing
        # file prevents silent GxP data substitution on misconfigured production inputs.
        if "sample" in data_path.lower():
            return generate_default_clinical_sample(data_path)
        raise FileNotFoundError(f"Input data file not found at: {data_path}")

    if data_path.endswith(".csv"):
        return pd.read_csv(data_path)
    elif data_path.endswith(".tsv"):
        return pd.read_csv(data_path, sep="\t")
    elif data_path.endswith(".parquet"):
        return pd.read_parquet(data_path)
    elif data_path.endswith(".json"):
        return pd.read_json(data_path)
    else:
        return pd.read_csv(data_path)


def evaluate_data_contract(
    df: Any,
    rules_path: str = "governance/rules.json",
    experiment_name: str = "gxp_clinical_governance",
    dataset_source_path: str | None = None,
    strict: bool = False,
) -> dict[str, Any]:
    """Evaluates a Great Expectations data contract and logs the audit trail to MLflow.

    Converts the input DataFrame to Pandas, builds an ephemeral GE context from the
    rules JSON specification, runs all configured expectations, and records GxP metrics
    and SHA-256 checksums to the active (or newly started) MLflow run.

    Args:
        df: Pandas or PySpark DataFrame to validate.
        rules_path: Path to the Great Expectations JSON contract specification.
        experiment_name: MLflow experiment name for GxP audit tracking.
        dataset_source_path: Optional path to the source data file; when provided,
            a SHA-256 checksum is computed and logged for immutable provenance.
        strict: When True, raises RuntimeError if any expectation fails.

    Returns:
        dict with keys:
            success (bool), total_records (int), evaluated_expectations (int),
            successful_expectations (int), unsuccessful_expectations (int),
            success_rate (float, 0-100), run_id (str).
    """
    # Resolve against repo root to support invocation from any working directory.
    resolved_rules_path = rules_path
    if not os.path.exists(resolved_rules_path):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        candidate = os.path.join(base_dir, rules_path)
        if os.path.exists(candidate):
            resolved_rules_path = candidate
        else:
            raise FileNotFoundError(f"Data contract rules not found at: {rules_path}")

    # GE 1.x requires a Pandas DataFrame; convert PySpark inputs before validation.
    if hasattr(df, "toPandas"):
        pdf = df.toPandas()
    elif isinstance(df, pd.DataFrame):
        pdf = df
    else:
        pdf = pd.DataFrame(df)

    # Normalise temporal columns to ISO-8601 strings; GE regex expectations
    # require consistent string representations across all datetime dtypes.
    for col_name in pdf.columns:
        if pd.api.types.is_datetime64_any_dtype(pdf[col_name]):
            pdf[col_name] = pdf[col_name].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif pdf[col_name].dtype == "object":
            pdf[col_name] = pdf[col_name].apply(
                lambda val: val.isoformat() if hasattr(val, "isoformat") else val
            )

    clean_experiment_name = experiment_name.lstrip("/")
    try:
        mlflow.set_experiment(clean_experiment_name)
    except Exception as exp_err:
        print(f"[MLFLOW WARNING] Could not set experiment '{clean_experiment_name}': {exp_err}")

    rules_checksum = (
        compute_sha256(resolved_rules_path) if os.path.exists(resolved_rules_path) else "N/A"
    )
    data_checksum = (
        compute_sha256(dataset_source_path)
        if (dataset_source_path and os.path.exists(dataset_source_path))
        else "in_memory_dataframe"
    )

    # Load suite config; the `meta` block carries compliance metadata logged as MLflow params.
    with open(resolved_rules_path) as f:
        suite_config = json.load(f)

    meta = suite_config.get("meta", {})

    active_run = mlflow.active_run()
    is_nested_run = active_run is not None
    # Do not wrap an existing outer run in a context manager — that would end it on __exit__.
    run: Any
    if is_nested_run:
        run = active_run
    else:
        run = mlflow.start_run(run_name="gxp_data_contract_gate")

    try:
        mlflow.log_param("data_input_path", dataset_source_path or "in_memory_dataframe")
        mlflow.log_param("data_sha256", data_checksum)
        mlflow.log_param("rules_sha256", rules_checksum)
        mlflow.log_param("execution_environment", os.getenv("ENVIRONMENT", "dev"))
        mlflow.log_param(
            "compliance_standard", meta.get("compliance_standard", "FDA_21_CFR_Part_11")
        )
        mlflow.log_param("target_schema", meta.get("target_schema", "OMOP_CDM_v5.4"))

        # Build an ephemeral GE context; fall back to get_* on repeated same-process calls
        # to avoid DuplicateKeyError without requiring a fresh interpreter each time.
        try:
            context = ge.get_context(mode="ephemeral")
            ds: Any
            try:
                ds = context.data_sources.add_pandas("gxp_pandas_source")
            except Exception:
                ds = context.data_sources.get("gxp_pandas_source")

            try:
                asset = ds.add_dataframe_asset("gxp_clinical_asset")
            except Exception:
                asset = ds.get_asset("gxp_clinical_asset")

            try:
                batch_def = asset.add_batch_definition_whole_dataframe("gxp_batch")
            except Exception:
                batch_def = asset.get_batch_definition("gxp_batch")

            suite_name = suite_config.get("expectation_suite_name", "gxp_suite")
            try:
                suite = context.suites.add(ge.ExpectationSuite(name=suite_name))
            except Exception:
                suite = context.suites.get(suite_name)

            for exp_dict in suite_config.get("expectations", []):
                exp_type = exp_dict.get("expectation_type", "")
                kwargs = exp_dict.get("kwargs", {})
                pascal_name = "".join(word.capitalize() for word in exp_type.split("_"))
                if hasattr(ge.expectations, pascal_name):
                    exp_cls = getattr(ge.expectations, pascal_name)
                    suite.add_expectation(exp_cls(**kwargs))
                else:
                    print(
                        f"[GxP WARNING] Unknown expectation type '{exp_type}' (class {pascal_name}) skipped."
                    )

            if len(suite.expectations) == 0 and len(suite_config.get("expectations", [])) > 0:
                print(
                    f"[GxP WARNING] Rules config contained {len(suite_config.get('expectations', []))} expectations but 0 were valid."
                )

            # Hash suffix satisfies GE's uniqueness constraint when the same suite name is
            # reused across multiple calls within a single ephemeral context.
            val_def_name = f"gxp_val_def_{abs(hash(suite_name))}"
            try:
                val_def = context.validation_definitions.add(
                    ge.ValidationDefinition(name=val_def_name, data=batch_def, suite=suite)
                )
            except Exception:
                val_def = context.validation_definitions.get(val_def_name)

            results = val_def.run(batch_parameters={"dataframe": pdf})

            evaluated_expectations = len(results.results)
            successful_expectations = sum(1 for r in results.results if r.success)
            unsuccessful_expectations = evaluated_expectations - successful_expectations
            success_rate = (
                (successful_expectations / evaluated_expectations * 100.0)
                if evaluated_expectations > 0
                else 0.0
            )
            validation_passed = results.success
            res_dict = {
                "success": validation_passed,
                "statistics": {
                    "evaluated_expectations": evaluated_expectations,
                    "successful_expectations": successful_expectations,
                    "unsuccessful_expectations": unsuccessful_expectations,
                    "success_percent": success_rate,
                },
            }
        except Exception as e:
            print(
                f"[GxP WARNING] Great Expectations suite execution failed; contract metrics zeroed: {e}"
            )
            evaluated_expectations, successful_expectations, unsuccessful_expectations = 0, 0, 0
            success_rate = 0.0
            validation_passed = False
            res_dict = {"success": False, "error": str(e)}

        # Log GxP metrics and archive the contract spec and validation result as MLflow artifacts.
        mlflow.log_metric("total_records_ingested", len(pdf))
        mlflow.log_metric("evaluated_expectations", evaluated_expectations)
        mlflow.log_metric("successful_expectations", successful_expectations)
        mlflow.log_metric("unsuccessful_expectations", unsuccessful_expectations)
        mlflow.log_metric("expectation_success_rate", success_rate)
        mlflow.log_metric("gxp_gate_passed", 1.0 if validation_passed else 0.0)

        try:
            mlflow.log_artifact(resolved_rules_path, artifact_path="governance_contracts")
            with tempfile.TemporaryDirectory() as tmp_dir:
                results_output_path = os.path.join(tmp_dir, "validation_results.json")
                with open(results_output_path, "w", encoding="utf-8") as f:
                    json.dump(res_dict, f, indent=2)
                mlflow.log_artifact(results_output_path, artifact_path="audit_reports")
        except Exception as art_err:
            print(f"[MLFLOW WARNING] Could not log audit artifact: {art_err}")

        # For nested runs, re-query the active run to get the current run_id rather than
        # relying on the stale reference captured before the try block.
        run_id = (
            mlflow.active_run().info.run_id
            if is_nested_run
            else getattr(getattr(run, "info", None), "run_id", None) or "unknown_run"
        )
        if not validation_passed:
            print(
                f"[WARNING] GxP Data Contract Gate: {unsuccessful_expectations} expectations evaluated for review. Run ID: {run_id}"
            )
        else:
            print(f"[SUCCESS] GxP Data Contract Gate Passed. Run ID: {run_id}")
        print(
            f"[METRIC] Evaluated: {evaluated_expectations} | Passed: {successful_expectations} | Pass Rate: {success_rate:.1f}%"
        )

    finally:
        # End only runs opened by this function; caller-owned outer runs must remain active.
        if not is_nested_run:
            mlflow.end_run()

    if strict and not validation_passed:
        raise RuntimeError(
            f"GxP Data Contract Validation failed! {unsuccessful_expectations} expectation(s) failed."
        )

    result_dict = {
        "success": validation_passed,
        "total_records": len(pdf),
        "evaluated_expectations": evaluated_expectations,
        "successful_expectations": successful_expectations,
        "unsuccessful_expectations": unsuccessful_expectations,
        "success_rate": success_rate,
        "run_id": run_id,
    }
    if "error" in res_dict:
        result_dict["error"] = res_dict["error"]

    return result_dict


def run_governance_pipeline(data_path: str, rules_path: str, experiment_name: str) -> dict[str, Any]:
    """Loads a dataset and evaluates the given data contract, logging results to MLflow.

    Convenience wrapper around load_dataset + evaluate_data_contract for
    CLI and scripted governance pipeline execution.
    """
    df = load_dataset(data_path)
    return evaluate_data_contract(
        df,
        rules_path=rules_path,
        experiment_name=experiment_name,
        dataset_source_path=data_path,
    )


if __name__ == "__main__":
    data_target = sys.argv[1] if len(sys.argv) > 1 else "governance/sample_clinical.csv"
    rules_target = sys.argv[2] if len(sys.argv) > 2 else "governance/rules.json"
    run_governance_pipeline(data_target, rules_target, "gxp_clinical_governance")
