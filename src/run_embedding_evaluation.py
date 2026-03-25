"""
run_eval.py
-----------
Example entrypoint showing how to call ClusterEvaluator
with an embeddings DataFrame.

Usage:
    python run_eval.py --embeddings path/to/embeddings.csv
                       --occ        path/to/occ.csv
                       --templates  data/preprocessed/HDFS.log_templates.csv
"""

import argparse
import pandas as pd
from cluster_eval import ClusterEvaluator, BEST_PARAMS


def main():
    parser = argparse.ArgumentParser(description="Evaluate embedding clustering quality")
    parser.add_argument("--embeddings",  required=True, help="CSV with BlockId + feature columns")
    parser.add_argument("--occ",         required=True, help="CSV with BlockId, Label, Type columns")
    parser.add_argument("--templates",   required=True, help="Path to HDFS.log_templates.csv")
    parser.add_argument("--no-tree",     action="store_true", help="Skip tree hierarchy evaluation")
    parser.add_argument("--no-depth",    action="store_true", help="Skip depth-to-purity evaluation")
    parser.add_argument("--output",      default=None, help="Optional path to save flat metrics CSV")
    args = parser.parse_args()

    print(f"Loading embeddings from {args.embeddings}...")
    embeddings_df = pd.read_csv(args.embeddings)
    print(f"  Shape: {embeddings_df.shape}")
    feature_cols = [c for c in embeddings_df.columns if c != "BlockId"]
    print(f"  Features: {len(feature_cols)}")

    print(f"\nLoading occurrence data from {args.occ}...")
    occ_df = pd.read_csv(args.occ)
    print(f"  Shape: {occ_df.shape}")

    print(f"\nHDBSCAN params: {BEST_PARAMS}")

    evaluator = ClusterEvaluator(
        occ_df=occ_df,
        templates_path=args.templates,
    )

    results = evaluator.evaluate(
        embeddings_df=embeddings_df,
        run_tree_eval=not args.no_tree,
        run_depth_eval=not args.no_depth,
    )

    evaluator.print_report(results)

    if args.output:
        pd.DataFrame([results["flat_metrics"]]).to_csv(args.output, index=False)
        print(f"Flat metrics saved to {args.output}")


if __name__ == "__main__":
    main()