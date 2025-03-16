import json
import sys
import os
from typing import Any, Union

def reduce_json(data: Any, sample_size: int = 10, current_depth: int = 0, max_depth: int = 10) -> Union[list, dict, Any]:
    if current_depth > max_depth:
        return "MAX_DEPTH_REACHED"

    if isinstance(data, list):
        return [reduce_json(item, sample_size, current_depth + 1, max_depth) 
                for item in data[:sample_size]]
    elif isinstance(data, dict):
        return {key: reduce_json(value, sample_size, current_depth + 1, max_depth) 
                for key, value in list(data.items())[:sample_size]}
    else:
        return data

def main() -> None:
    if len(sys.argv) not in [2, 3]:
        print("Usage: python reducejson.py <file_name> [sample_size]")
        sys.exit(1)

    file_path = sys.argv[1]
    sample_size = 10

    if len(sys.argv) == 3:
        try:
            sample_size = int(sys.argv[2])
            if sample_size <= 0:
                raise ValueError("Sample size must be positive.")
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        reduced_data = reduce_json(data, sample_size=sample_size)

        base_name = os.path.splitext(os.path.basename(file_path))[0]
        output_file = f'{base_name}_reduced.json'

        with open(output_file, 'w', encoding='utf-8') as file:
            json.dump(reduced_data, file, indent=2, ensure_ascii=False)

        print(f"Reduced file created at '{output_file}' with sample size {sample_size}.")
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except json.JSONDecodeError:
        print(f"Error: The file '{file_path}' is not a valid JSON file.")
    except PermissionError:
        print(f"Error: Permission denied when trying to read or write the file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()