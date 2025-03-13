import os
import re
import pandas as pd
import shutil
from googletrans import Translator
import time
import httpcore
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.errors import HttpError
import subprocess
import logging

# Set up logging
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, "translation_errors.log")

# Remove the old log file if it exists
if os.path.exists(log_file):
    os.remove(log_file)

logging.basicConfig(
    filename=log_file,
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def format_lua_content(content, file_path):
    print(f"Start format output file")
    try:
        stylua_path = r"path_to_stylua.exe"

        if not os.path.exists(stylua_path):
            print(f"Warning: stylua not found at {stylua_path}")
            return content

        # Create a temporary file for the content
        temp_file = file_path + ".temp"
        with open(temp_file, "w", encoding="utf-8") as f:
            f.write(content)

        try:
            result = subprocess.run(
                [stylua_path, temp_file], check=True, capture_output=True, text=True
            )

            if result.returncode != 0:
                print(f"StyLua failed for {file_path}: {result.stderr}")
                return content

            # Read the formatted content
            with open(temp_file, "r", encoding="utf-8") as f:
                formatted_content = f.read()

            print(f"Format done")
            return formatted_content

        except subprocess.TimeoutExpired:
            print(f"StyLua timed out for {file_path}")
            return content
        finally:
            # Clean up temporary file
            if os.path.exists(temp_file):
                os.remove(temp_file)

    except Exception as e:
        print(f"Formatting failed: {e}")
        return content


setattr(httpcore, "SyncHTTPTransport", "AsyncHTTPProxy")

# Global cache for translations
translations_cache = {}

japanese_skip = []

# Path to your service account key file
SERVICE_ACCOUNT_FILE = "translate-mods.json"

# Name of your Google Sheet
SHEET_NAME = "Dictionary"


def get_gspread_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        SERVICE_ACCOUNT_FILE, scope
    )
    client = gspread.authorize(creds)
    return client


def load_translations_from_google_sheets(client):
    global translations_cache
    print("\nLoading Dictionary . . .")
    try:
        sheet = client.open(SHEET_NAME).sheet1
        data = sheet.get_all_records()
        print("Dictionary loaded")
        translations_cache = {
            row["Original Text"]: {
                "Translated_Text": row["Translated Text"],
                "Is_Comment": row["Is Comment"],
                "Is_Quotes": row["Is Quotes"],
                "Found_In": row.get("Found In", "").split("\n")
                if row.get("Found In")
                else [],
            }
            for row in data
        }
    except Exception as e:
        logging.error(f"Failed to load translations from Google Sheets: {str(e)}")
        print(f"Error loading dictionary: {e}")


def load_ignore_files_from_sheet2(client):
    print("\nLoading Ignore file list . . .")
    global ignore_files
    try:
        sheet = client.open(SHEET_NAME).get_worksheet(1)  # Access the second sheet

        # Get non-empty values from first column
        # Assuming the ignore files are listed in the first column
        ignore_files = [f for f in sheet.col_values(1) if f.strip()]

        print("Ignore file list loaded")
        ignore_files = ignore_files
    except Exception as e:
        logging.error(f"Failed to load ignore files list: {str(e)}")
        print(f"Error loading ignore files: {e}")


def load_data_from_google_sheets():
    client = get_gspread_client()
    load_translations_from_google_sheets(client)
    load_ignore_files_from_sheet2(client)


def save_translations_to_google_sheets():
    print("\nSaving Dictionary . . .")
    global translations_cache
    client = get_gspread_client()
    sheet = None
    retries = 3
    for attempt in range(retries):
        try:
            sheet = client.open(SHEET_NAME).sheet1
            break
        except HttpError as e:
            if e.resp.status in [500, 503]:
                print(
                    f"Server error encountered. Retrying... ({attempt + 1}/{retries})"
                )
                time.sleep(2**attempt)  # Exponential backoff
            else:
                logging.error(f"HTTP error while saving translations: {str(e)}")
                raise

    if sheet is None:
        logging.error("Failed to open sheet after several attempts")
        print(
            "Failed to open the sheet after several attempts. Saving to local backup file."
        )
        save_to_local_backup()
        return

    try:
        # Convert the translations cache to a DataFrame
        df = pd.DataFrame(
            [
                {
                    "Original Text": key,
                    "Translated Text": value["Translated_Text"],
                    "Is Comment": value.get("Is_Comment", ""),
                    "Is Quotes": value.get("Is_Quotes", ""),
                    "Found In": "\n".join(value.get("Found_In", [])),
                }
                for key, value in translations_cache.items()
            ]
        )
        # Clear the existing content
        sheet.clear()
        # Update the sheet with new data
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"Translations saved to {SHEET_NAME}")
    except Exception as e:
        logging.error(f"Failed to save translations to Google Sheets: {str(e)}")
        print(f"Error saving translations: {e}")
        save_to_local_backup()


def save_to_local_backup():
    import json

    global translations_cache
    try:
        with open("dictionary_backup.json", "w", encoding="utf-8") as file:
            json.dump(translations_cache, file, ensure_ascii=False, indent=4)
        print("Translations saved to local backup file 'dictionary_backup.json'")
    except Exception as e:
        logging.error(f"Failed to save local backup: {str(e)}")
        print(f"Error saving backup: {e}")


def escape_quotes(text):
    # Replace unescaped double quotes with escaped double quotes
    text = re.sub(r'(?<!\\)"', r"\"", text)
    # Replace unescaped single quotes with escaped single quotes
    text = re.sub(r"(?<!\\)'", r"\'", text)
    return text


def check_format_specifiers(original, translated):
    # Pattern to match format specifiers
    pattern = r"%(?:\d+)?(?:\.\d+)?[diouxXeEfFgGcrs%]"

    # Extract format specifiers from both strings
    original_specifiers = re.findall(pattern, original)
    translated_specifiers = re.findall(pattern, translated)

    # Compare the specifiers
    if len(original_specifiers) > 0 and len(original_specifiers) != len(
        translated_specifiers
    ):
        error_msg = f"Format specifiers count mismatch! \nOriginal: {original}\nTranslated: {translated}"
        logging.error(error_msg)
        raise SystemExit(error_msg)

    for i, (orig, trans) in enumerate(zip(original_specifiers, translated_specifiers)):
        if orig != trans:
            error_msg = f"Format specifier mismatch at position {i + 1}! \nOriginal: {orig}\nTranslated: {trans}"
            logging.error(error_msg)
            raise SystemExit(error_msg)


def get_path_start_from_mod_folder(full_path):
    # Regular expression to find the mod ID folder and everything after it
    pattern = r"(?:\\)([^\n\\]+(?:_translated_en)\\.*)"

    # Search for the pattern in the full path
    match = re.search(pattern, full_path)

    if match:
        # Extract the path and remove "_translated_en" if present
        path = match.group(1)
        path = path.replace("_translated_en\\", "\\")
        return path
    else:
        error_msg = "No mod folder found in the path"
        logging.error(error_msg)
        raise SystemExit(error_msg)


def translate_text(text, translator, isComment, file_path, retries=100):
    global translations_cache
    translated_entry = translations_cache.get(text) or {}

    if isComment:
        translated_entry["Is_Comment"] = "✔"
    else:
        translated_entry["Is_Quotes"] = "✔"

    # Initialize Found_In list if it doesn't exist
    if "Found_In" not in translated_entry:
        translated_entry["Found_In"] = []
    # Add file path if not already present
    relative_path = get_path_start_from_mod_folder(file_path)
    if relative_path not in translated_entry["Found_In"]:
        translated_entry["Found_In"].append(relative_path)
        translated_entry["Found_In"].sort()

    if translated_entry.get("Translated_Text"):
        print(f"Reuse translation: \n{text}\n{translated_entry['Translated_Text']}\n")
        check_format_specifiers(text, translated_entry["Translated_Text"])
        translations_cache[text] = translated_entry
        return translated_entry["Translated_Text"]

    # Skip Japanese text
    hiragana_katakana_pattern = r"[\u3040-\u30FF]+"
    if re.search(hiragana_katakana_pattern, text):
        print(f"Japanese - Skip: \n{text}\n")
        translated_entry["Translated_Text"] = text
        translations_cache[text] = translated_entry
        japanese_skip.append(text)
        return text

    # Start translate text
    for attempt in range(retries):
        try:
            translator.raise_Exception = True
            translated = translator.translate(text, src="zh-cn", dest="en")
            translated_text = translated.text.capitalize().replace("\\ n", "\\n")

            print(f"Translated:\n{text}\n{translated_text}\n")
            check_format_specifiers(text, translated_text)
            translated_entry["Translated_Text"] = translated_text
            translations_cache[text] = translated_entry
            return translated_text
        except Exception as e:
            error_msg = f"Error translating text:\n{text}\n{str(e)}"
            print(error_msg)
            logging.error(error_msg)
            if attempt < retries - 1:
                print(f"\nRetrying... ({attempt + 1}/{retries})\n")
                time.sleep(1)  # Wait a bit before retrying
            else:
                error_msg = f"Failed to translate '{text}' after {retries} attempts."
                logging.error(error_msg)
                print(error_msg)
                return text


def translate_file(file_path, translator):
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            original_content = file.read()
        translated_content = original_content
        # try:
        # decrypted_content = decrypt_lua(original_content)
        # except Exception as e:
        # error_msg = f"Error decrypt file \n{file_path}\n{str(e)}"
        # raise SystemExit

        # Regex to find comments
        comments = re.findall(r"--[^-\n]*(?:-[^-\n]+)*", translated_content)
        print(f"Found {len(comments)} comments in \n{file_path}\n")

        for comment in comments:
            japanese_mandarin_text = re.findall(
                r"[\u3040-\u30FF\u4E00-\u9FFF]+", comment
            )
            if (
                len(japanese_mandarin_text) > 0 and comment.strip()
            ):  # Skip strings with only spaces

                # Extract leading and trailing dashes, "[[", "]]", "=" and whitespace
                leading = comment[: len(comment) - len(comment.lstrip("-[= .\t"))]
                trailing = comment[len(comment.rstrip("]-= .\t")) :]

                # Get the trimmed version of the original comment
                trimmed_comment = comment.lstrip("-[= .\t").rstrip("]-= .\t")

                print("(comment)")
                translated_comment = translate_text(
                    trimmed_comment, translator, True, file_path
                )

                # Restore the comment with original leading and trailing
                translated_comment = leading + translated_comment + trailing

                translated_content = translated_content.replace(
                    comment, translated_comment
                )

        # Regex to find text in double quotation marks
        # May select double quotes string inside single quotes tring
        double_quoted_strings = re.findall(
            r'(?<!\\)"([^"\n\\]*(?:\\.[^"\n\\]*)*)"', translated_content
        )

        print(
            f"Found {len(double_quoted_strings)} double-quoted strings in \n{file_path}\n"
        )

        for string in double_quoted_strings:
            japanese_mandarin_text = re.findall(
                r"[\u3040-\u30FF\u4E00-\u9FFF]+", string
            )
            if (
                len(japanese_mandarin_text) > 0 and string.strip()
            ):  # Skip strings with only spaces

                # Extract leading and trailing whitespace
                leading = string[: len(string) - len(string.lstrip())]
                trailing = string[len(string.rstrip()) :]

                print("(double quoted)")
                translated_string = escape_quotes(
                    translate_text(string.strip(), translator, False, file_path)
                )

                # Restore the string with original leading and trailing
                translated_string = leading + translated_string + trailing

                translated_content = translated_content.replace(
                    f'"{string}"', f'"{translated_string}"'
                )

        # Regex to find text in single quotation marks
        single_quoted_strings = re.findall(
            r"(?<!\\)'([^'\n\\]*(?:\\.[^'\n\\]*)*)'", translated_content
        )
        print(
            f"Found {len(single_quoted_strings)} single-quoted strings in \n{file_path}\n"
        )

        for string in single_quoted_strings:
            japanese_mandarin_text = re.findall(
                r"[\u3040-\u30FF\u4E00-\u9FFF]+", string
            )
            if (
                len(japanese_mandarin_text) > 0 and string.strip()
            ):  # Skip strings with only spaces

                # Extract leading and trailing whitespace
                leading = string[: len(string) - len(string.lstrip())]
                trailing = string[len(string.rstrip()) :]

                print("(single quoted)")
                translated_string = escape_quotes(
                    translate_text(string.strip(), translator, False, file_path)
                )

                # Restore the string with original leading and trailing
                translated_string = leading + translated_string + trailing

                # Turn single_quoted string into a translated double-quoted string
                translated_content = translated_content.replace(
                    f"'{string}'", f'"{translated_string}"'
                )

        if translated_content != original_content:
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(format_lua_content(translated_content, file_path))
            print(f"Successfully translated file: \n{file_path}")
        else:
            print(f"No changes made to file: \n{file_path}")

    except Exception as e:
        error_msg = f"Error processing file \n{file_path}\n{str(e)}"
        logging.error(error_msg)
        print(error_msg)


def translate_folder(folder_path):
    translator = Translator()

    folder_name = os.path.basename(
        folder_path.replace(" - Copy", "")
        .replace(" - Source", "")
        .replace("_decrypted-Only-Merge", "")
        .replace("_decrypted", "")
        .replace("_beautified", "")
    )
    new_folder_path = os.path.join(
        "D:\\SteamLibrary\\steamapps\\common\\Don't Starve Together\\mods",
        folder_name + "_translated_en",
    )

    # Check if the duplicate folder exists
    if os.path.exists(new_folder_path):
        user_input = (
            input(
                f"\nThe folder '{new_folder_path}' already exists.\nDo you want to delete it and create a new one? (yes/no): "
            )
            .strip()
            .lower()
        )
        if user_input in ["yes", "y"]:
            try:
                shutil.rmtree(new_folder_path)
                print(f"\nDeleted existing folder: \n{new_folder_path}")
            except Exception as e:
                error_msg = f"Error deleting folder {new_folder_path}: {str(e)}"
                logging.error(error_msg)
                print(error_msg)
                return
        elif user_input in ["no", "n"]:
            print("Operation cancelled by user.")
            return
        else:
            print("Invalid input. Operation cancelled.")
            return

    # Duplicate the folder
    try:
        shutil.copytree(folder_path, new_folder_path)
        print(f"\nDuplicated folder to: \n{new_folder_path}")
    except Exception as e:
        error_msg = f"Error duplicating folder to {new_folder_path}: {str(e)}"
        logging.error(error_msg)
        print(error_msg)
        return

    total_files = 0
    lua_files = []
    for root, _, files in os.walk(new_folder_path):
        for file in files:
            if file.endswith(".lua"):
                total_files += 1
                lua_files.append(os.path.join(root, file))

    print(f"\nFound {total_files} .lua files to process")

    # Load translations once at the start
    load_data_from_google_sheets()

    global ignore_files

    # Process files with progress tracking
    processed_files = 0
    for file_path in lua_files:
        processed_files += 1
        print(f"\nProgress: {processed_files}/{total_files} files")
        # Check if file is ignored
        matching_ignore_files = [
            ignore_file
            for ignore_file in ignore_files
            if os.path.normpath(ignore_file) in file_path.replace("_translated_en", "")
        ]

        if matching_ignore_files:
            print(f"\nIgnore file: \n{file_path}")
            print("Matched ignore patterns:")
            for ignore_file in matching_ignore_files:
                print(f"- {ignore_file}")
            print()
        else:
            print(f"\nTranslating file: \n{file_path}\n")
            translate_file(file_path, translator)


def require_valid_folder_directory():
    while True:
        folder_path = input("\nPlease enter the path of your folder: ")

        if os.path.isdir(folder_path):
            print(f"\nValid directory")
            break
        else:
            error_msg = f"Error: '{folder_path}' is not a valid directory"
            logging.error(error_msg)
            print(f"\n{error_msg}. Please try again.")

    return folder_path.rstrip("\\")


if __name__ == "__main__":
    try:
        folder_path = require_valid_folder_directory()
        print(f"Starting translation for folder: {folder_path}")
        translate_folder(folder_path)

        if len(japanese_skip) > 0:
            print(f"\nSkipped {len(japanese_skip)} Japanese string(s):")
            for value in japanese_skip:
                print(value)

        save_translations_to_google_sheets()
        print("\nTranslation completed.")
    except Exception as e:
        error_msg = f"Fatal error in main execution: {str(e)}"
        logging.error(error_msg)
        print(f"\n{error_msg}")
