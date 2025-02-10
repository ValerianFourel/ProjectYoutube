import json
import os
import sys
from collections import defaultdict


def remove_folder_and_slashes(file_path):
    return file_path.split('/')[-1]

def remove_after_underscore(s):
    return s.split('_')[0]


def generate_unique_filename(base_name, extension, folder):
    """Generates a unique filename by appending _2, _3, etc., if a file already exists."""
    filename = f"{base_name}{extension}"
    full_path = os.path.join(folder, filename)
    counter = 2

    while os.path.exists(full_path):
        filename = f"{base_name}_{counter}{extension}"
        full_path = os.path.join(folder, filename)
        counter += 1

    return full_path

def process_json_files(input_file, search_terms):
    # Dictionary to store all matching videos, grouped by subject_name
    videos_by_subject = defaultdict(dict)
    # Extract subject name from the filename and replace leading "_" with "@"
    subject_name = input_file.replace('_VideosObject.json', '')
    subject_name = remove_folder_and_slashes(subject_name)

    if subject_name.startswith('_'):
        subject_name = '@' + subject_name[1:]
    
    subject_name = remove_after_underscore(subject_name)

    # Read the JSON file
    with open(input_file, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # Search through the videos
    for video_id, video_info in data['videos'].items():
        title = video_info['title']

        # If search terms are provided, filter by terms; otherwise, include all videos
        if search_terms:
            if any(term.lower() in title.lower() for term in search_terms):
                videos_by_subject[subject_name][video_id] = video_info
        else:
            videos_by_subject[subject_name][video_id] = video_info

    # Create a new dictionary with the matching videos
    reduced_videos = {
        "search_terms": search_terms if search_terms else "All videos processed",
        "videos": {}
    }
    
    for subject, videos in videos_by_subject.items():
        for video_id, video_info in videos.items():
            reduced_videos["videos"][video_id] = video_info
            reduced_videos["videos"][video_id]['subject_name'] = subject

    # Ensure the 'downloadingReadyJson' folder exists
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'DownloadingReadyJson')
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Create the base output file name using the input file's base name and the suffix "_matching_Download_ready.json"
    base_output_filename = os.path.splitext(os.path.basename(input_file))[0] + "_matching_Download_ready"
    output_filepath = generate_unique_filename(base_output_filename, ".json", output_folder)

    # Create a single JSON file with reduced matching videos
    with open(output_filepath, 'w', encoding='utf-8') as outfile:
        json.dump(reduced_videos, outfile, indent=2)

    print(f"\nTotal videos in reduced set: {len(reduced_videos['videos'])}")
    print(f"Created {output_filepath}")

if __name__ == "__main__":
    # Check if enough arguments are provided
    if len(sys.argv) < 2:
        print("Usage: python script.py <input_file.json> [search_terms]")
        sys.exit(1)

    # Get the input JSON file from the first argument
    input_file = sys.argv[1]

    # Check if a list of search terms is provided
    if len(sys.argv) > 2:
        # Convert the command line input into a list of search terms (terms are separated by spaces)
        search_terms = sys.argv[2].strip('[]').replace("'", "").split(',')
        search_terms = [term.strip() for term in search_terms]  # Clean up extra spaces
    else:
        search_terms = []

    # Run the processing function with the input file and search terms
    process_json_files(input_file, search_terms)
