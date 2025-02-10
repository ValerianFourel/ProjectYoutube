import os
import time
import logging
import datetime
from PIL import Image
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
import torch
from torchvision import models, transforms
import cv2
from transformers import AutoTokenizer, AutoModelForCausalLM
import json
import time
from paligemma import PaliGemma , check_and_process_captcha
from datetime import datetime
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import urllib.request
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import requests
import argparse
from urllib.parse import urlparse

import json
import logging
import torch
from ImageUtils import process_screenshot , capture_screenshot
# This needs to run only once to load the model into memory

text_prompt = "Give me only the 4 characters"


def download_video_High_quality(url, id, high_quality_dir):
    download_dir = os.path.join(high_quality_dir, 'HighQuality')

    os.makedirs(high_quality_dir, exist_ok=True)
                

    print('1')
    # Create the download directory if it doesn't exist
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    # Construct the full path to the video file
    video_path = os.path.join(download_dir, id + '_highQuality_.mp4')

    # Set the chunk size to 1MB
    chunk_size = 1024 * 1024
    time.sleep(10)
    # Send a GET request to the URL with streaming enabled
    response = requests.get(url, stream=True)

    # Check if the request was successful
    if response.status_code == 200:
        # Open the video file in binary write mode
        with open(video_path, 'wb') as file:
            # Iterate over the chunks of the response
            for chunk in response.iter_content(chunk_size=chunk_size):
                # Write the chunk to the file
                file.write(chunk)
    else:
        print('response      ',response)
        print(f"Failed to download video. Status code: {response.status_code}")


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






def getDownloadSmallVideosWithSound(driver):
    """
    Function to get the first download link within the specified div.

    Args:
    driver: Selenium WebDriver instance
    url: URL of the page containing the download link

    Returns:
    str: URL of the download link if found, None otherwise
    """
    try:

        # Wait for the result div to be present in the DOM
        result_div = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "sf_result"))
        )
        capture_screenshot(driver)

        # Find the first 'a' link inside the div with class "def-btn-box"
        download_link = result_div.find_element(By.CSS_SELECTOR, ".def-btn-box a")
        download_link.click()

        return download_link

    except TimeoutException:
        print("Timed out waiting for element to be present")
        return None
    except NoSuchElementException:
        print("Could not find the specified element")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None



def getDownloadLargeNoSound(driver):
    """
    Function to get the first download link for a video without audio from the second link-group.

    Args:
    driver: Selenium WebDriver instance

    Returns:
    str: URL of the download link if found, None otherwise
    """
    try:
        print("getDownloadLargeNoSound")
        # Wait for the drop-down box to be clickable and click it
        dropdown = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "drop-down-box"))
        )
        dropdown.click()

        # Capture screenshot (assuming this function is defined elsewhere)
        capture_screenshot(driver)

        # Wait for the list div to be visible
        list_div = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.CLASS_NAME, "list"))
        )

        # Find the links div inside the list div
        links_div = list_div.find_element(By.CLASS_NAME, "links")

        # Find the main div inside the links div
        main_div = links_div.find_element(By.CLASS_NAME, "main")

        # Find all link-group divs inside the main div
        link_groups = main_div.find_elements(By.CLASS_NAME, "link-group")

        # Check if there are at least two link-groups
        if len(link_groups) < 2:
            print("Not enough link-groups found")
            return None
        # Store the current tab ID
        original_window = driver.current_window_handle
        # Get the second link-group
        second_link_group = link_groups[0]

        # Find the first link in the second link-group
        download_link = second_link_group.find_element(By.TAG_NAME, "a")

        # Get the href attribute before clicking
        download_url = download_link.get_attribute('href')

        # Click the link
        download_link.click()
            # Wait briefly to allow the new tab to open (optional)
        driver.implicitly_wait(3)

        # Switch to the new tab (the one that is not the original tab)
        for window_handle in driver.window_handles:
            if window_handle != original_window:
                driver.switch_to.window(window_handle)
                break

        # (Optional) Wait for the new page to load or perform actions
        driver.implicitly_wait(5)

        # Close the new tab
        driver.close()

        # Switch back to the original tab
        driver.switch_to.window(original_window)
        return download_url

    except TimeoutException:
        print("Timed out waiting for element to be present")
    except NoSuchElementException:
        print("Could not find the specified element")
    except Exception as e:
        print(f"An error occurred: {e}")

    return None



def PopUpLargeVideos(driver):
    """
    Function to interact with the video download popup.

    Args:
    driver: Selenium WebDriver instance

    Returns:
    bool: True if successful, False otherwise
    """
    print("PopUpLargeVideos")
    try:
        # Wait for the popup to be present in the DOM
        popup = WebDriverWait(driver, 100).until(
            EC.presence_of_element_located((By.ID, "c-ui-popup"))
        )
        print("Popup found")
        capture_screenshot(driver)

                # Once popup is present, wait for the download button to be present within the popup
        download_button = WebDriverWait(popup, 300).until(
            EC.presence_of_element_located((By.CLASS_NAME, "c-ui-download-button"))
        )
        print("Download button found")
            # Find the download button within the popup
        # download_button = popup.find_element(By.CLASS_NAME, "c-ui-download-button")

            # Click the download button
        download_button.click()
        capture_screenshot(driver)

        print("Download button clicked successfully!")

            # Wait for the close button to be clickable
        close_button = WebDriverWait(driver, 100).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "c-ui-popup-btn-close"))
            )

            # Click the close button
        close_button.click()
        print("Close button clicked successfully!")

    except TimeoutException:
        print("Timed out waiting for popup to be present")
        capture_screenshot(driver)

        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


import os


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




def lowQualityDownload(driver, subject_download_dir):
    """
    Downloads a video from a low-quality link on the current page.
    
    Args:
        driver (webdriver): Selenium WebDriver instance.
        subject_download_dir (str): Directory where the video should be saved.
    """
    try:
        print("Trying to locate the low-quality download section...")
        section = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "landingTz-main-screen-top"))
        )

        # Find and click the "with low quality" link
        low_quality_link = section.find_element(By.XPATH, ".//a[@class='landingTz-btn-close' and contains(text(), 'with low quality')]")
        print("Found the 'with low quality' link. Clicking it now...")
        low_quality_link.click()

        print("Successfully clicked the 'with low quality' link.")

        # Wait for the download link to be generated
        time.sleep(10)  # Adjust based on actual site behavior

        # Get the actual video download URL
        video_url = low_quality_link.get_attribute('href')
        print("Video URL:", video_url)

        # Extract the original filename from the URL
        parsed_url = urlparse(video_url)
        original_filename = os.path.basename(parsed_url.path)

        # Construct the full path to save the video
        video_path = os.path.join(subject_download_dir, original_filename)

        # Ensure the download directory exists
        os.makedirs(subject_download_dir, exist_ok=True)

        # Download the video in chunks
        chunk_size = 1024 * 1024  # 1MB
        response = requests.get(video_url, stream=True)

        if response.status_code == 200:
            with open(video_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    file.write(chunk)
            print(f"Download complete: {video_path}")
            return video_path
        else:
            print(f"Failed to download video. Status code: {response.status_code}")
            return None

    except Exception as e:
        print("Error:", str(e))
        return None

def rename_mp4_file(subject_download_dir: str, id: str):
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
    import pandas as pd
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import os

    # Ensure the directory exists
    if not os.path.exists(subject_download_dir):
        os.makedirs(subject_download_dir)

    # Define CSV path inside subject_download_dir
    csv_path = os.path.join(subject_download_dir, 'download_status.csv')

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
        driver = None  # Initialisierung des WebDrivers außerhalb der Schleife
        old_id = 0
        for key, value in filtered_data.items():
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
            is_link_found = check_and_update_download_status(driver, video_id, video_title, subject_download_dir)

            if not is_link_found:
                print(f"Download link not found for video: {video_id or video_title}")
                continue
            # Check if video ID or title exists in any of the filenames
            should_skip = False
            for video_file in existing_videos:
                if (video_id and video_id in video_file) or (video_title and video_title in video_file):
                    should_skip = True
                    break

            if should_skip:
                print(f"Skipping existing video: {video_id or video_title}")
                continue

            # Use current WebDriver for this iteration
            subject_drivers[subject_name] = driver

            loop_counter += 1  # Increment counter

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
                check_and_process_captcha(driver, model)
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
            lowQualityDownload(driver, subject_download_dir)
                    # Wait for the sf_result div to be present
                # getDownload(driver)
            #download_url = getDownloadLargeNoSound(driver)
            capture_screenshot(driver)
            
            # download_video_High_quality(download_url, id, subject_download_dir)
                            # Construct the full path to the video file
            print('2')
            getDownloadSmallVideosWithSound(driver)

            print("Download link clicked successfully")
            rename_mp4_file(subject_download_dir, id)
            print("3. ")
            if check_value_above_400(driver):
                getDownloadSmallVideosWithSound(driver)
                PopUpLargeVideos(driver)
            totalDuration += float(value.get('duration'))
            old_id = id
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