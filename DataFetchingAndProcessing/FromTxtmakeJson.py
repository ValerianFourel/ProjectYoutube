import json
import re
import sys
import os


def parse_entry(entry):
    pattern = r'\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*(\d+|\w+)\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*([\w-]+)\s*\|\s*(https?://\S+)\s*\|\s*(.*?)\s*\_/'
    match = re.match(pattern, entry, re.DOTALL)
    if not match:
        print(f"Failed to parse entry: {entry}...")  # Print first 100 chars of failed entry
        return None

    return {
        "upload_date": match.group(1),
        "duration": match.group(2),
        "view_count": match.group(3),
        "like_count": match.group(4),
        "title": match.group(5),
        "id": match.group(6),
        "webpage_url": match.group(7),
        "description": match.group(8).strip()
    }

def process_file(filename):
    data = {"videos": {}, "urls": {}}

    with open(filename, 'r', encoding='utf-8') as file:
        content = file.read()
        print(f"File content length: {len(content)} characters")
        entries = content.split('\_/')
        print(f"Number of entries after split: {len(entries)}")

        for i, entry in enumerate(entries):
            entry = entry.strip()
            if entry:
                parsed_entry = parse_entry(entry + ' \_/')  # Add back the separator for parsing
                if parsed_entry:
                    video_id = parsed_entry["id"]
                    webpage_url = parsed_entry["webpage_url"]

                    data["videos"][video_id] = parsed_entry
                    data["urls"][webpage_url] = video_id
            else:
                print(f"Empty entry at index {i}")

    print(f"Processed videos: {len(data['videos'])}")
    print(f"Processed URLs: {len(data['urls'])}")
    return data

def save_json(data, output_file):
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# Main function to handle command line input
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <input_file.txt>")
        sys.exit(1)

    input_file = sys.argv[1]
    
    # Get the file name without extension
    file_name_without_ext = os.path.splitext(os.path.basename(input_file))[0]
    result = file_name_without_ext[4:]

    # Define the processedJson folder in the current directory where the script is located
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ProcessedJson')
    
    # Ensure the processedJson folder exists (although it should already)
    if not os.path.exists(output_folder):
        print(f"Error: processedJson folder does not exist in the current directory.")
        sys.exit(1)
    
    # Define the output file path inside processedJson
    output_file = os.path.join(output_folder, f"{result}_VideosObject.json")
    
    # Process the file and save the output
    data = process_file(input_file)
    save_json(data, output_file)

    print(f"JSON file has been created in: {output_file}")