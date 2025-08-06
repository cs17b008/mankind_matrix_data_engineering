# src/utils/config_loader.py

# import os
# from dotenv import load_dotenv

# def load_env_and_get(filename=".env", max_depth=6, raise_if_not_found=True):
#     """
#     Recursively walks up from current script location to find and load the .env file.
#     """
#     current_dir = os.path.abspath(os.path.dirname(__file__))

#     for _ in range(max_depth):
#         env_path = os.path.join(current_dir, filename)
#         if os.path.exists(env_path):
#             load_dotenv(env_path)
#             print(f"[SUCCESS] .env loaded from: {env_path}")
#             return True
#         current_dir = os.path.dirname(current_dir)

#     msg = f"[ERROR] .env file not found within {max_depth} levels"
#     if raise_if_not_found:
#         raise FileNotFoundError(msg)
#     else:
#         print(msg)
#         return False

import os
from dotenv import load_dotenv

def load_env_and_get(key: str = None, default=None, filename=".env", max_depth=6, raise_if_not_found=True):
    """
    Recursively walks up from current script location to find and load the .env file.
    If `key` is provided, returns the value of the env variable.
    """
    current_dir = os.path.abspath(os.path.dirname(__file__))

    for _ in range(max_depth):
        env_path = os.path.join(current_dir, filename)
        if os.path.exists(env_path):
            load_dotenv(env_path)
            print(f"[SUCCESS] .env loaded from: {env_path}")
            return os.getenv(key, default) if key else True
        current_dir = os.path.dirname(current_dir)

    msg = f"[ERROR] .env file not found within {max_depth} levels"
    if raise_if_not_found:
        raise FileNotFoundError(msg)
    else:
        print(msg)
        return os.getenv(key, default) if key else False















# # To test the function, you can uncomment the following lines:
# if __name__ == "__main__":
#     search_env_and_load()




























# # src/utils/config_loader.py

# import os
# from dotenv import load_dotenv

# def find_project_root(filename=".env", max_depth=6):
#     """
#     Recursively walks up the directory tree to find the directory containing the given filename.
#     Returns the path to the directory if found, else None.
#     """
    
#     current_dir = os.path.abspath(os.path.dirname(__file__))

#     for _ in range(max_depth):
#         env_file_path = os.path.join(current_dir, filename)
#         if os.path.exists(env_file_path):
#             return current_dir
#         current_dir = os.path.dirname(current_dir)

#     return None



# def load_project_env(filename=".env", max_depth=6, raise_if_not_found=True):
#     """
#     Loads a .env file from the project root by searching upward.
#     """
#     project_root = find_project_root(filename, max_depth)

#     if project_root:
#         env_path = os.path.join(project_root, filename)
#         load_dotenv(env_path)
#         print(f"[SUCCESS] .env loaded from: {env_path}")
#         return True

#     msg = f"[ERROR] .env file not found within {max_depth} levels up from {__file__}"
#     if raise_if_not_found:
#         raise FileNotFoundError(msg)
#     else:
#         print(f"WARNING: {msg}")
#         return False
