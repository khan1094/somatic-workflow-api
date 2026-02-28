import csv
import os

RESULTS_DIR = "/refdata/results"

def get_result_file_path(workflow_name: str):
    return os.path.join(RESULTS_DIR, f"{workflow_name}.tsv")


def summarize_results(workflow_name: str):

    file_path = get_result_file_path(workflow_name)

    if not os.path.exists(file_path):
        raise FileNotFoundError("Results file not found")

    summary = {}

    with open(file_path, newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")

        for row in reader:
            classification = row.get("classification")
            if classification:
                summary[classification] = summary.get(classification, 0) + 1

    return file_path, summary
