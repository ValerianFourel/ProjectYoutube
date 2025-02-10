import json
import os
from collections import defaultdict
import sys 


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

def process_json_file(input_file, search_terms, output_folder):
    """Process a single JSON file."""
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

    # Create the base output file name using the input file's base name and the suffix "_matching_Download_ready.json"
    base_output_filename = os.path.splitext(os.path.basename(input_file))[0] + "_matching_Download_ready"
    output_filepath = generate_unique_filename(base_output_filename, ".json", output_folder)

    # Create a single JSON file with reduced matching videos
    with open(output_filepath, 'w', encoding='utf-8') as outfile:
        json.dump(reduced_videos, outfile, indent=2)

    print(f"\nTotal videos in reduced set: {len(reduced_videos['videos'])}")
    print(f"Created {output_filepath}")

def process_all_json_files_in_folder(search_terms):
    """Process all *_VideosObject.json files in the ProcessedJson folder."""
    # Define the folder paths
    input_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ProcessedJson')
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'downloadingReadyJsonAllDatasets')

    # Ensure the output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Get all *_VideosObject.json files in the ProcessedJson folder
    json_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith('_VideosObject.json')]

    if not json_files:
        print("No matching files found in the ProcessedJson folder.")
        return

    # Process each file
    for json_file in json_files:
        print(f"Processing {json_file}...")
        process_json_file(json_file, search_terms, output_folder)

if __name__ == "__main__":
    # Check if a list of search terms is provided
    if len(sys.argv) > 1:
        # Convert the command line input into a list of search terms (terms are separated by spaces)
        search_terms = sys.argv[1].strip('[]').replace("'", "").split(',')
        search_terms = [term.strip() for term in search_terms]  # Clean up extra spaces
    else:
        search_terms = []

    # Run the processing function to process all files in the folder
    process_all_json_files_in_folder(search_terms)
