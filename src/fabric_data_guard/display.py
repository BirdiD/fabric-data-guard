import os
from datetime import datetime
from pathlib import Path

from IPython.display import HTML, display


def get_latest_html_file(root_folder):
    """
    Retrieve the path of the HTML file in the latest subfolder based on the subfolder name.

    Args:
    root_folder (str): The path to the root folder containing subfolders named with datetime values.

    Returns:
    str: The full path to the HTML file in the latest subfolder, or None if no HTML file is found.
    """
    latest_subfolder = None
    latest_date = datetime.min
    for subfolder_name in os.listdir(root_folder):
        subfolder_path = Path(root_folder) / subfolder_name
        if subfolder_path.is_dir() and subfolder_name.endswith("Z"):
            try:
                folder_date = datetime.strptime(subfolder_name, "%Y%m%dT%H%M%S.%fZ")
                if folder_date > latest_date:
                    latest_date = folder_date
                    latest_subfolder = subfolder_path
            except ValueError:
                continue
    if latest_subfolder:
        html_files = list(latest_subfolder.glob("*.html"))
        if html_files:
            return str(html_files[0])
    return None


def show_great_expectations_html(project_root_dir, suite_name):
    """
    Display the HTML content of the latest Great Expectations validation result.

    Args:
    project_root_dir (str): The root directory of the Great Expectations project.
    suite_name (str): The name of the expectation suite.
    """
    root_folder = f"{project_root_dir}/gx/uncommitted/data_docs/local_site/validations/{suite_name}/{suite_name}_run"
    file_path = get_latest_html_file(root_folder)
    if file_path:
        with open(file_path, "r") as f:
            html_content = f.read()
        display(HTML(html_content))
    else:
        print("No HTML file found for the latest validation.")
