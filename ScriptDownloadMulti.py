import os
import time
import logging
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
import torch
import json
import time
from paligemma import PaliGemma 
from datetime import datetime
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import argparse
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import json
import logging
from ImageUtils import capture_screenshot
from OCRutils import check_and_process_captcha , process_captcha
from DownloadUtils import getDownloadSmallVideosWithSound , getDownloadLargeNoSound , lowQualityDownload , PopUpLargeVideos



text_prompt = "Give me only the 4 characters"



def check_value_above_400(driver):
    """
    Function to check if the value in the specified span is above 400.

    Args:
    driver: Selenium WebDriver instance
    url: URL of the page to check

    Returns:
    bool: True if value is 400 or above, False otherwise
    """
    try:

        # Wait for the result div to be present in the DOM
        result_div = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "sf_result"))
        )

        # Find the span inside the div with class "def-btn-name"
        span_element = result_div.find_element(By.CSS_SELECTOR, ".def-btn-name span")

        # Get the text content of the span
        span_value = span_element.text.strip()

        # Try to convert the value to a number
        try:
            numeric_value = float(span_value)
            is_above_400 = numeric_value >= 400
            print(f"Value found: {numeric_value}, Is above or equal to 400: {is_above_400}")
            return is_above_400
        except ValueError:
            print(f"Unable to convert span value '{span_value}' to a number")
            return False

    except TimeoutException:
        print("Timed out waiting for element to be present")
        return False
    except NoSuchElementException:
        print("Could not find the specified element")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False





def get_latest_mp4_file(directory, expected_name, id):

    try:
        # Use scandir for better performance 【1】【2】
        files = [f for f in os.scandir(directory) 
                if f.is_file() and f.name.lower().endswith('.mp4')]

        if not files:
            return None, None

        # Get latest file based on modification time 【3】
        latest_file = max(files, key=lambda x: os.path.getmtime(x.path))
        mod_time = os.path.getmtime(latest_file.path)

        # Get the directory path and filename separately
        dir_path = os.path.dirname(latest_file.path)
        filename = os.path.basename(latest_file.path)

        # Check if filename matches expected name
        if filename != expected_name:
            # Create new filename with id
            new_filename = f"{id}.mp4"
            new_path = os.path.join(dir_path, new_filename)

            # Rename the file
            os.rename(latest_file.path, new_path)
            return new_path, datetime.fromtimestamp(mod_time)

        return latest_file.path, datetime.fromtimestamp(mod_time)

    except Exception as e:
        print(f"Error: {e}")
        return None, None



def setup_firefox_webdriver(download_dir="/home/vfourel/ProjectGym/DownloadedOutputHighQuality_v2", geckodriver_path="/usr/local/bin/geckodriver"):
    # Set up the Firefox profile
    firefox_profile = webdriver.FirefoxProfile()
    firefox_profile.set_preference("browser.download.folderList", 2)  # 2 means custom directory
    firefox_profile.set_preference("browser.download.dir", download_dir)
    firefox_profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "video/mp4")
    firefox_profile.set_preference("media.play-stand-alone", False)

    # Set up Firefox options
    firefox_options = FirefoxOptions()
    firefox_options.add_argument("--headless")  # Run in headless mode
    firefox_options.log.level = "trace"  # Enable verbose logging

    # Set up the Firefox service
    firefox_service = FirefoxService(executable_path=geckodriver_path)

    try:
        logging.info("Initializing WebDriver")
        print('Initializing WebDriver')
        driver = webdriver.Firefox(
            service=firefox_service,
            options=firefox_options,
            firefox_profile=firefox_profile
        )
        return driver
    except Exception as e:
        logging.error(f"Failed to initialize WebDriver: {str(e)}")
        print(f"Failed to initialize WebDriver: {str(e)}")
        return None


def get_existing_files(base_download_dir):
    existing_files = {}
    for subject_name in os.listdir(base_download_dir):
        subject_dir = os.path.join(base_download_dir, subject_name)
        high_quality_dir = os.path.join(subject_dir, 'HighQuality')

        if os.path.exists(high_quality_dir):
            for filename in os.listdir(high_quality_dir):
                if filename.endswith('.mp4'):
                    file_id = os.path.splitext(filename)[0]
                    existing_files[file_id] = subject_name

    return existing_files

def filter_data(data, existing_files):
    return {k: v for k, v in data.items() if v.get('id') not in existing_files}





def rename_mp4_file(subject_download_dir, id):
    original_file = os.path.join(subject_download_dir, "videoplayback.mp4")
    new_file = os.path.join(subject_download_dir, f"{id}.mp4")
    
    if os.path.exists(original_file):
        os.rename(original_file, new_file)
        print(f"Renamed '{original_file}' to '{new_file}'")
    else:
        print(f"File '{original_file}' not found.")

def check_and_update_download_status(driver, video_id, video_title, subject_download_dir):
    """
    Check if download link was found and update CSV file with the status.

    Args:
        driver: Selenium WebDriver instance
        video_id: ID of the video
        video_title: Title of the video
        subject_download_dir: Directory where the CSV should be saved
    """
    

    # Ensure the directory exists
    if not os.path.exists(subject_download_dir):
        os.makedirs(subject_download_dir)

    # Define CSV path inside subject_download_dir
    csv_path = os.path.join(subject_download_dir, 'download_status.csv')
    is_link_found = True

    # Check if the error message exists
    error_message = "The download link not found."
    try:
        error_element = driver.find_element(By.XPATH, f"//div[contains(@class, 'result-failure') and contains(text(), '{error_message}')]")
        is_link_found = False
    except:
        is_link_found = True

    # Prepare the new data
    new_data = {
        'video_id': [video_id],
        'video_title': [video_title],
        'is_link_found': [is_link_found]
    }

    # Convert to DataFrame
    new_df = pd.DataFrame(new_data)

    # If file exists, append to it, otherwise create new file
    if os.path.exists(csv_path):
        # Read existing CSV
        df = pd.read_csv(csv_path)

        # Check if this video_id already exists
        if video_id in df['video_id'].values:
            # Update existing entry
            df.loc[df['video_id'] == video_id, 'is_link_found'] = is_link_found
            df.loc[df['video_id'] == video_id, 'video_title'] = video_title
        else:
            # Append new entry
            df = pd.concat([df, new_df], ignore_index=True)
    else:
        df = new_df

    # Save to CSV
    df.to_csv(csv_path, index=False)

    return is_link_found

def get_download_status(subject_download_dir, video_id, video_title):
    """
    Get the download status (is_link_found) from CSV file for a specific video.

    Args:
        subject_download_dir: Directory where the CSV is saved
        video_id: ID of the video to look up
        video_title: Title of the video to look up

    Returns:
        bool or None: True if link was found, False if not found, None if entry doesn't exist
    """
    import pandas as pd
    import os

    csv_path = os.path.join(subject_download_dir, 'download_status.csv')

    # Check if file exists
    if not os.path.exists(csv_path):
        return True

    # Read the CSV file
    df = pd.read_csv(csv_path)

    # Look for matching entry
    matching_rows = df[
        (df['video_id'] == video_id) & 
        (df['video_title'] == video_title)
    ]

    # If we found a matching entry, return its status
    if not matching_rows.empty:
        return bool(matching_rows.iloc[0]['is_link_found'])

    return True

def main(model, device, file_path, data, dataSearchTerm, base_download_dir):
    # Get the start time of the loop
    start_time = time.time()
    print(f"Loop start time: {start_time}")

    # Dictionary to store WebDriver instances for each subject
    subject_drivers = {}

    totalDuration = 0
    totalDurationGoal = 0

    # Set to keep track of unique subject names
    unique_subjects = set()
    try:

    # Iterate through the dictionary
        for key, value in data.items():
            # Assuming the 'subject_name' is a field in your JSON data
            #subject_name = value.get('subject_name')
            if 'subject_name' in value:
                unique_subjects.add(value['subject_name'])

            # Calculate total duration goal
        try:
            totalDurationGoal += float(value.get('duration', 0))
            print("totalDurationGoal: ", totalDurationGoal)
        except ValueError as e:
            print(f"Error converting duration to float: {e}")
        except KeyError as e:
            print(f"Error accessing 'duration' key: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        # Get the first 5 unique subject names (or all if there are fewer than 5)
        first_5_subjects = list(unique_subjects)[:5]

        for subject_name in first_5_subjects:
            subject_download_dir = os.path.join(base_download_dir, subject_name)
            os.makedirs(subject_download_dir, exist_ok=True)
            
           
                # Before the main loop, add these lines:
        existing_files = get_existing_files(base_download_dir)
        filtered_data = filter_data(data, existing_files)
        loop_counter = 0  # Zähler für die Iterationen
        driver = setup_firefox_webdriver(subject_download_dir)  # Initialisierung des WebDrivers außerhalb der Schleife
        old_id = 0
        for key, value in filtered_data.items():
            is_downloaded = False
            is_link_found = True
            loop_counter += 1  # Increment counter

            if loop_counter % 8 == 0:
                # Close previous WebDriver if it exists
                if driver:
                    driver.quit()

                # Create new WebDriver
                subject_download_dir = os.path.join(base_download_dir, subject_name)
                driver = setup_firefox_webdriver(subject_download_dir)
                print(f"New driver initialized at iteration {loop_counter}")

            # Get values once
            video_id = value.get('id')
            video_title = value.get('title')
            subject_name = value.get('subject_name')
            subject_download_dir = os.path.join(base_download_dir, subject_name)

            # Check if video already exists
            existing_videos = [f for f in os.listdir(subject_download_dir) if f.endswith('.mp4')]
                    # Check download link status and update CSV in subject_download_dir
            is_link_found = get_download_status(subject_download_dir, video_id, video_title)
            print(is_link_found)
            if not is_link_found:
                print(f"Download link not found for video: {video_id or video_title}")
                continue
            # Check if video ID or title exists in any of the filenames
            should_skip = False
            print(video_title)

            for video_file in existing_videos:
                if (video_id in video_file) or ( video_title in video_file):
                    should_skip = True
                    break
            print("should_skip      ",should_skip)

            if should_skip:
                print(f"Skipping existing video: {video_id or video_title}")
                continue

            # Use current WebDriver for this iteration
            subject_drivers[subject_name] = driver

           

            subject_download_dir = os.path.join(base_download_dir, subject_name)


            # Create the nested folder if it doesn't exist
            os.makedirs(subject_download_dir, exist_ok=True)

            print(f"Folder created at: {subject_download_dir}")
            timestamp = time.time()  # Current time
            driver = subject_drivers[subject_name]
            logging.info("Navigating to webpage")
            print('Navigating to webpage')
            driver.get("https://en1.savefrom.net/2ol/")

            logging.info("Waiting for input area")
            print('Waiting for input area')
            input_area = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "sf_url"))
            )

            logging.info("Sending string to input area")
            print("Sending string to input area")
            string_to_send = value.get('webpage_url')# "https://www.youtube.com/watch?v=NqvooBJ8FY8" #  "https://www.youtube.com/watch?v=jEF-u4ntt2Q"
            input_area.send_keys(string_to_send)

            logging.info("Waiting for 2 seconds")
            time.sleep(2)

            logging.info("Locating and clicking the download button")
            print("Locating and clicking the download button")
            download_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "sf_submit"))
            )
            download_button.click()
            logging.info("Waiting for 3 seconds")
            time.sleep(3)

            # Take the screenshot
            print("1. ")
            capture_screenshot(driver)
            try:
                # check_and_process_captcha(driver, model)
                process_captcha(driver,model,text_prompt)
                print("Captcha processed")
            except Exception as e:
                print("No captcha found")

            time.sleep(3)
            download_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "sf_submit")))
            download_button.click()
            print('We click on the download button again')
            time.sleep(3)
            print("2. ")
            capture_screenshot(driver)
            
            # Wait for the section to be present
            lowQualityDownloaded = lowQualityDownload(driver, subject_download_dir) 
            is_downloaded = is_downloaded | lowQualityDownloaded
                    # Wait for the sf_result div to be present
                # getDownload(driver)
            #download_url = getDownloadLargeNoSound(driver)
            capture_screenshot(driver)
            
                            # Construct the full path to the video file
            print('2')
            downloadSmallVideos = getDownloadSmallVideosWithSound(driver)
            is_downloaded = is_downloaded | downloadSmallVideos

            print("Download link clicked successfully")
            rename_mp4_file(subject_download_dir, video_id)
            print("3. ")
            if check_value_above_400(driver):
                downloadSmallVideos = getDownloadSmallVideosWithSound(driver)
                is_downloaded = is_downloaded | downloadSmallVideos
                popupLarge = PopUpLargeVideos(driver)
                is_downloaded = is_downloaded | popupLarge
            if is_downloaded:
                get_latest_mp4_file(subject_download_dir, video_title, video_id)

            totalDuration += float(value.get('duration'))
            check_and_update_download_status(driver, video_id, video_title, subject_download_dir)
                # Wait for the sf_result div to be present

        # Find the download link within sf_result
            time.sleep(5)
            capture_screenshot(driver)


        logging.info("Waiting for 10 seconds after clicking the download button")
        time.sleep(300)


        logging.info("Script execution completed")
        print("Script execution completed")

    except WebDriverException as e:
        logging.error(f"WebDriver error occurred: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        capture_screenshot(driver)
        logging.info("Closing WebDriver")
        if 'driver' in locals():
            driver.quit()

    # Get the end time of the loop
    end_time = time.time()
    print(f"Loop end time: {end_time}")

    # Calculate and print the total execution time
    execution_time = end_time - start_time
    print(f"Total execution time: {execution_time} seconds")

if __name__ == "__main__":
    # Initialize the PaliGemma model
    model = PaliGemma()
    text_prompt = "Give me only the 4 characters"
    #: Ex:     file_path = '/home/vfourel/ProjectGym/SmallDataset/downloadingReadyJsonAllDatasets/_PamelaRf1_VideosSmallDataset_VideosObject_matching_Download_ready.json'
    #: Ex:     base_download_dir="/home/vfourel/ProjectGym/DownloadedOutputHighQuality"

    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_path", type=str, required=True, help="Path to the JSON file")
    parser.add_argument("--base_download_dir", type=str, required=True, help="Base directory for downloads")
    args = parser.parse_args()

    # Read the JSON file
    with open(args.file_path, 'r') as file:
        data = json.load(file)
        dataVideos = data['videos']
        dataSearchTerm = data['search_terms']
        data = dataVideos

    # Check if CUDA is available
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    # Set up logging
    logging.basicConfig(level=logging.INFO)

    main(model, device, args.file_path, data, dataSearchTerm, args.base_download_dir)