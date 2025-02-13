#!/usr/bin/env python3
import os
import datetime
import shutil
import sys

def get_contents(paths):
    """
    Collects file information from the given directories.
    Returns a dictionary with file paths as keys and file metadata as values.
    """
    return_dict = {}
    for path in paths:
        contents = _add_contents(path, path)
        for file_path, file_info in contents.items():
            if file_path in return_dict:
                existing_last_edited = return_dict[file_path]["last_edited"]
                if file_info["last_edited"] > existing_last_edited:
                    return_dict[file_path] = file_info
            else:
                return_dict[file_path] = file_info
    return return_dict

def _add_contents(path, base_path):
    """ Recursively adds file contents to the dictionary. """
    return_dict = {}
    try:
        for item in os.listdir(path):
            item_path = os.path.join(path, item)
            if os.path.isfile(item_path):
                file_info = _get_file_info(item_path, base_path)
                return_dict[file_info["new_path"]] = file_info
            elif os.path.isdir(item_path):
                return_dict.update(_add_contents(item_path, base_path))
    except PermissionError:
        print(f"Permission denied: {path}", file=sys.stderr)
    return return_dict

def _get_file_info(file_path, base_path):
    """ Returns a dictionary with metadata for the given file."""
    last_edited = os.path.getmtime(file_path)
    last_edited = datetime.datetime.fromtimestamp(last_edited).strftime('%Y-%m-%d, %H:%M:%S')
    return {
        "name": os.path.basename(file_path),
        "original_path": file_path,
        "last_edited": last_edited,
        "type": "local",
        "new_path": file_path.replace(base_path, "").lstrip(os.sep)
    }

def copy_files(file_dict, destination_folder):
    """ Copies files to the destination folder, maintaining directory structure. """
    for file_info in file_dict.values():
        dest_path = os.path.join(destination_folder, file_info["new_path"])
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        shutil.copy2(file_info["original_path"], dest_path)
    return f"COPYING LATEST VERSION OF FILES TO {destination_folder} COMPLETE"

def get_user_folders():
    """
    Prompts the user to input folders to process.
    Validates the folders and returns a list of valid paths.
    """
    folders = []
    print("Enter the folders to process (one at a time). Type 'done' when finished.")
    while True:
        folder = input("Folder path (or 'done'): ").strip()
        if folder.lower() == 'done':
            break
        folder = os.path.abspath(os.path.expanduser(folder))
        if not os.path.isdir(folder):
            print(f"Error: '{folder}' is not a valid directory.", file=sys.stderr)
            continue
        folders.append(folder)
    return folders

def main():
    folders = get_user_folders()
    if not folders:
        print("No folders provided. Exiting.")
        sys.exit(1)


    file_dict = get_contents(folders)

    destination_folder = input("Enter the destination folder: ").strip()
    destination_folder = os.path.abspath(os.path.expanduser(destination_folder))
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    print(copy_files(file_dict, destination_folder))

if __name__ == "__main__":
    main()