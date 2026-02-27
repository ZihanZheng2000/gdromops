import pickle
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd

from .core.hmdt_cart import (
    GDROM_predict,
    GDROM_predict_no_iteration,
    train_cart_model,
    train_hmdt,
    validate_hmdt,
    validate_hmdt_pbias,
)
from .core.metric import validate
from .core.rule_output import get_ct, get_rt


REQUIRED_COLUMNS = ["Inflow", "Storage", "Release", "PDSI", "DOY"]
NORMALIZE_COLUMNS = ["Inflow", "Storage", "Release"]


def _upsert_training_summary(training_summary_path: Path, result: dict) -> Path:
    training_summary_path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "GRAND_ID": str(result["target_id"]),
        "best_its": result.get("best_its"),
        "best_num_state": result.get("best_num_state"),
        "complexity_free_k": result.get("selection_rule", {}).get("complexity_free_k"),
        "nse_gain_per_extra_k": result.get("selection_rule", {}).get("nse_gain_per_extra_k"),
        "NSE_iterative_storage": result.get("metrics_iterative_storage", {}).get("NSE"),
        "KGE_iterative_storage": result.get("metrics_iterative_storage", {}).get("KGE"),
        "NSE_observed_storage": result.get("metrics_observed_storage", {}).get("NSE"),
        "KGE_observed_storage": result.get("metrics_observed_storage", {}).get("KGE"),
    }
    incoming = pd.DataFrame([row])

    if training_summary_path.exists():
        summary_df = pd.read_csv(training_summary_path)
        if "GRAND_ID" in summary_df.columns:
            summary_df["GRAND_ID"] = summary_df["GRAND_ID"].astype(str).str.strip()
            incoming_id = row["GRAND_ID"]
            summary_df = summary_df[summary_df["GRAND_ID"] != incoming_id]
            summary_df = pd.concat([summary_df, incoming], ignore_index=True)
        else:
            summary_df = pd.concat([summary_df, incoming], ignore_index=True)
    else:
        summary_df = incoming

    summary_df.to_csv(training_summary_path, index=False)
    return training_summary_path


def _prepare_target_data(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    if "Inflow" not in data.columns and "NetInflow" in data.columns:
        data = data.rename(columns={"NetInflow": "Inflow"})

    missing = [c for c in REQUIRED_COLUMNS if c not in data.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    data = data.dropna(subset=REQUIRED_COLUMNS).copy()
    if len(data) < 30:
        raise ValueError(f"Not enough valid rows after cleaning: {len(data)}")
    return data


def _normalize_by_storage_cap(df: pd.DataFrame, storage_cap: float) -> pd.DataFrame:
    if storage_cap <= 0:
        raise ValueError(f"storage_cap must be positive, got {storage_cap}")
    data = df.copy()
    for col in NORMALIZE_COLUMNS:
        data[col] = data[col].astype(float) / float(storage_cap)
    return data


def _load_storage_cap_from_summary(summary_path: Path, target_id: str) -> float:
    if not summary_path.exists():
        raise FileNotFoundError(f"Summary file not found: {summary_path}")
    summary_df = pd.read_csv(summary_path)
    summary_match = summary_df[summary_df["GRAND_ID"].astype(str).str.strip() == target_id]
    if summary_match.empty:
        raise ValueError(f"GRAND_ID={target_id} not found in summary file.")
    if "STORAGE_CAP" not in summary_match.columns:
        raise ValueError("summary file must include STORAGE_CAP column.")
    return float(summary_match.iloc[0]["STORAGE_CAP"])


def _normalize_storage_cap_source(raw: str) -> str:
    token = str(raw).strip().lower()
    if token == "data_max":
        return "input_max_storage"
    return token


def _split_data(target_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    n_total = len(target_data)
    train_end = int(0.64 * n_total)
    val_end = int(0.80 * n_total)
    if train_end <= 0 or val_end <= train_end or val_end >= n_total:
        raise ValueError("Invalid split points. Need more rows in target_data.")

    train_data = target_data.iloc[:train_end].copy()
    validation_data = target_data.iloc[train_end:val_end].copy()
    test_data = target_data.iloc[val_end:].copy()
    return train_data, validation_data, test_data


def _select_best_hyperparameters(
    nse_scores: Dict[int, List[float]],
    trained_models_dict: Dict[int, list],
    best_its: Optional[int],
    best_num_state: Optional[int],
    complexity_free_k: int,
    nse_gain_per_extra_k: float,
) -> Tuple[int, int]:
    if best_its is not None and best_num_state is not None:
        return int(best_its), int(best_num_state)

    candidates = []
    for its, models in trained_models_dict.items():
        if not models:
            continue
        scores = nse_scores.get(its, [])
        for k in range(1, len(models) + 1):
            nse = None
            if k - 1 < len(scores):
                score = scores[k - 1]
                if score is not None and not pd.isna(score):
                    nse = float(score)
            penalty = nse_gain_per_extra_k * max(0, k - int(complexity_free_k))
            adjusted = (nse - penalty) if nse is not None else None
            candidates.append(
                {
                    "its": int(its),
                    "k": int(k),
                    "nse": nse,
                    "penalty": float(penalty),
                    "adjusted": adjusted,
                }
            )

    if not candidates:
        raise ValueError("No trained HMDT model available for hyperparameter selection.")

    filtered = candidates
    if best_its is not None:
        filtered = [c for c in filtered if c["its"] == int(best_its)]
    if best_num_state is not None:
        filtered = [c for c in filtered if c["k"] == int(best_num_state)]
    if not filtered:
        raise ValueError("No candidate matches the provided best_its / best_num_state constraints.")

    valid = [c for c in filtered if c["adjusted"] is not None]
    if valid:
        best = sorted(valid, key=lambda c: (c["adjusted"], c["nse"], -c["k"], -c["its"]), reverse=True)[0]
        return int(best["its"]), int(best["k"])

    fallback = sorted(filtered, key=lambda c: (c["k"], c["its"]))[0]
    return int(fallback["its"]), int(fallback["k"])


def train_res_r_model(
    target_id: Union[str, int],
    target_data: pd.DataFrame,
    storage_cap: float,
    module_condition_dir: Path,
    modules_dir: Path,
    its_values: Iterable[int] = (6, 7, 8),
    best_its: Optional[int] = None,
    best_num_state: Optional[int] = None,
    complexity_free_k: int = 3,
    nse_gain_per_extra_k: float = 0.02,
    save_models_dir: Optional[Path] = None,
) -> dict:
    cleaned = _prepare_target_data(target_data)
    train_data, validation_data, test_data = _split_data(cleaned)

    trained_models_dict = {}  # type: Dict[int, list]
    nse_scores = {}  # type: Dict[int, List[float]]
    pbias_scores = {}  # type: Dict[int, List[float]]

    for its in its_values:
        its_value = int(its)
        models = train_hmdt(train_data, its_value)
        trained_models_dict[its_value] = models
        nse_scores[its_value] = validate_hmdt(validation_data, models)
        pbias_scores[its_value] = validate_hmdt_pbias(validation_data, models)

    selected_its, selected_state = _select_best_hyperparameters(
        nse_scores=nse_scores,
        trained_models_dict=trained_models_dict,
        best_its=best_its,
        best_num_state=best_num_state,
        complexity_free_k=complexity_free_k,
        nse_gain_per_extra_k=nse_gain_per_extra_k,
    )
    selected_models = trained_models_dict[selected_its]
    if selected_state < 1 or selected_state > len(selected_models):
        raise ValueError(
            f"Selected state {selected_state} is out of range for ITS={selected_its}. "
            f"Available states: 1..{len(selected_models)}"
        )

    hmdt_model = selected_models[selected_state - 1]
    ct_model = train_cart_model(train_data, hmdt_model, selected_state)

    test_data_updated = GDROM_predict(test_data.copy(), hmdt_model, ct_model)
    test_data_updated = GDROM_predict_no_iteration(test_data_updated, hmdt_model, ct_model)
    observed_release = test_data_updated["Release"].values
    simulated_release_iter = test_data_updated["Simulated_Release"].values
    simulated_release_no_iter = test_data_updated["Simulated_Release_no_iteration"].values
    metrics_iter = validate(observed_release, simulated_release_iter)
    metrics_no_iter = validate(observed_release, simulated_release_no_iter)

    module_condition_dir.mkdir(parents=True, exist_ok=True)
    modules_dir.mkdir(parents=True, exist_ok=True)

    features = ["Inflow", "Storage", "PDSI", "DOY"]
    target_id_str = str(target_id)
    if selected_state != 1:
        ct_rules = get_ct(ct_model, storage_cap, features, class_names=ct_model.classes_.tolist())
        ct_out = module_condition_dir / f"{target_id_str}.txt"
        ct_out.write_text("\n".join(ct_rules) + "\n", encoding="utf-8")

    for index, rt in enumerate(hmdt_model.model):
        rt_class_names = rt.classes_.tolist() if hasattr(rt, "classes_") else [None]
        rt_rules = get_rt(rt, storage_cap, features, class_names=rt_class_names)
        rt_out = modules_dir / f"{target_id_str}_{index}.txt"
        rt_out.write_text("\n".join(rt_rules) + "\n", encoding="utf-8")

    if save_models_dir is not None:
        save_models_dir.mkdir(parents=True, exist_ok=True)
        with (save_models_dir / f"{target_id_str}_HMDT.model").open("wb") as f:
            pickle.dump(hmdt_model, f)
        with (save_models_dir / f"{target_id_str}_CT.model").open("wb") as f:
            pickle.dump(ct_model, f)

    return {
        "target_id": target_id_str,
        "best_its": selected_its,
        "best_num_state": selected_state,
        "selection_rule": {
            "complexity_free_k": int(complexity_free_k),
            "nse_gain_per_extra_k": float(nse_gain_per_extra_k),
        },
        "nse_scores": nse_scores,
        "pbias_scores": pbias_scores,
        "metrics_iterative_storage": metrics_iter,
        "metrics_observed_storage": metrics_no_iter,
    }


def train_res_r_from_paths(
    target_id: Union[str, int],
    target_data_path: Union[str, Path],
    summary_path: Optional[Union[str, Path]],
    output_root: Union[str, Path],
    its_values: Iterable[int] = (6, 7, 8),
    best_its: Optional[int] = None,
    best_num_state: Optional[int] = None,
    complexity_free_k: int = 3,
    nse_gain_per_extra_k: float = 0.02,
    storage_cap_source: str = "summary",
    save_models_dir: Optional[Union[str, Path]] = None,
    training_summary_path: Optional[Union[str, Path]] = None,
) -> dict:
    target_data_path = Path(target_data_path)
    summary_path_obj = Path(summary_path) if summary_path is not None else None
    output_root = Path(output_root)
    models_dir = Path(save_models_dir) if save_models_dir is not None else None

    if not target_data_path.exists():
        raise FileNotFoundError(f"Target data file not found: {target_data_path}")

    target_id_str = str(target_id).strip()
    cleaned_data = _prepare_target_data(pd.read_csv(target_data_path))

    storage_cap_source = _normalize_storage_cap_source(storage_cap_source)

    if storage_cap_source == "summary":
        if summary_path_obj is None:
            raise ValueError("summary_path is required when storage_cap_source='summary'.")
        storage_cap = _load_storage_cap_from_summary(summary_path_obj, target_id_str)
        normalized_data = cleaned_data
        normalization_applied = False
    elif storage_cap_source == "input_max_storage":
        storage_cap = float(cleaned_data["Storage"].max())
        normalized_data = _normalize_by_storage_cap(cleaned_data, storage_cap)
        normalization_applied = True
    else:
        raise ValueError(f"Unsupported storage_cap_source: {storage_cap_source}")

    module_condition_dir = output_root / "Res_R" / "module_conditions"
    modules_dir = output_root / "Res_R" / "modules"

    result = train_res_r_model(
        target_id=target_id_str,
        target_data=normalized_data,
        storage_cap=storage_cap,
        module_condition_dir=module_condition_dir,
        modules_dir=modules_dir,
        its_values=its_values,
        best_its=best_its,
        best_num_state=best_num_state,
        complexity_free_k=complexity_free_k,
        nse_gain_per_extra_k=nse_gain_per_extra_k,
        save_models_dir=models_dir,
    )
    if training_summary_path is None:
        training_summary_file = output_root / "Res_R" / "training_summary.csv"
    else:
        training_summary_file = Path(training_summary_path)
    written_summary = _upsert_training_summary(training_summary_file, result)
    result["training_summary_path"] = str(written_summary)
    result["storage_cap_source"] = storage_cap_source
    result["storage_cap_used"] = float(storage_cap)
    result["normalization_applied"] = bool(normalization_applied)
    return result
