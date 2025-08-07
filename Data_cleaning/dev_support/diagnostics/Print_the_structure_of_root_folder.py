# Print the folder and file structure of your project, excluding mankind_env
import os

def print_tree(startpath, prefix=""):
    for item in os.listdir(startpath):
        if item == "mankind_env":
            continue  # Skip the mankind_env folder
        path = os.path.join(startpath, item)
        if os.path.isdir(path):
            print(f"{prefix}{item}/")
            print_tree(path, prefix + "    ")
        else:
            print(f"{prefix}{item}")

# Change this to your project root if needed
project_root = r"E:\mankind_matrix_project"
print_tree(project_root)