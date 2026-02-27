import argparse
import json
from typing import List

from .res_r import train_res_r_from_paths


def _parse_its_values(raw: str) -> List[int]:
    values = []
    for token in raw.split(","):
        token = token.strip()
        if token:
            values.append(int(token))
    if not values:
        raise ValueError("its-values must contain at least one integer.")
    return values


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train GDROM Res_R model and export rule text files.")
    parser.add_argument("--target-id", required=True, help="Reservoir GRAND_ID.")
    parser.add_argument("--target-data-path", required=True, help="Path to reservoir normalized time-series CSV.")
    parser.add_argument("--summary-path", required=True, help="Path to reservoir metadata CSV.")
    parser.add_argument("--output-root", required=True, help="Output root folder for Operation Rules - GDROMs.")
    parser.add_argument(
        "--its-values",
        default="6,7,8",
        help="Comma-separated ITS values for HMDT training (default: 6,7,8).",
    )
    parser.add_argument("--best-its", type=int, default=None, help="Optional fixed ITS value.")
    parser.add_argument("--best-num-state", type=int, default=None, help="Optional fixed module count (K).")
    parser.add_argument(
        "--save-models-dir",
        default=None,
        help="Optional directory to save trained HMDT/CART model files.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    result = train_res_r_from_paths(
        target_id=args.target_id,
        target_data_path=args.target_data_path,
        summary_path=args.summary_path,
        output_root=args.output_root,
        its_values=_parse_its_values(args.its_values),
        best_its=args.best_its,
        best_num_state=args.best_num_state,
        save_models_dir=args.save_models_dir,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
