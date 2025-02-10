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
import json
import logging
import torch
from ImageUtils import process_screenshot , capture_screenshot
# This needs to run only once to load the model into memory

text_prompt = "Give me only the 4 characters"


def download_video_High_quality(url, id, download_dir):
    # Create the download directory if it doesn't exist
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    # Construct the full path to the video file
    video_path = os.path.join(download_dir, id + '.mp4')

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
        print('Download URL: ', download_url)

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
        popup = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "c-ui-popup"))
        )
        print("Popup found")
        capture_screenshot(driver)

                # Once popup is present, wait for the download button to be present within the popup
        download_button = WebDriverWait(popup, 30).until(
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


def check_and_rename_mp4_files(directory, timestamp,id):
    for filename in os.listdir(directory):
        if filename.lower().endswith('.mp4'):
            file_path = os.path.join(directory, filename)
            file_creation_time = os.path.getctime(file_path)
            
            if file_creation_time > timestamp:
                # File was created after the specified timestamp
                new_filename = f"{id}.mp4"
                new_file_path = os.path.join(directory, new_filename)
                
                os.rename(file_path, new_file_path)
                print(f"Renamed {filename} to {new_filename}")

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
            
            # Set up a WebDriver for this unique subject
            driver = setup_firefox_webdriver(subject_download_dir)
            print(driver,subject_name)
            if driver:
                subject_drivers[subject_name] = driver
        
                # Before the main loop, add these lines:
        existing_files = get_existing_files(base_download_dir)
        filtered_data = filter_data(data, existing_files)

        for key, value in filtered_data.items():
            subject_name = value.get('subject_name')
            id = value.get('id')
            subject_download_dir = os.path.join(base_download_dir, subject_name)
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
            try:
            # Wait for the section to be present
                print("try for low quality")
                section = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "landingTz-main-screen-top"))
            )

            # Find the "with low quality" link and click it
                low_quality_link = section.find_element(By.XPATH, ".//a[@class='landingTz-btn-close' and contains(text(), 'with low quality')]")

                print("Found the 'with low quality' link. Clicking it now...")
                low_quality_link.click()
                print("Successfully clicked the 'with low quality' link.")
            except Exception as e:
                print("No low quality link found")
                    # Wait for the sf_result div to be present
                # getDownload(driver)
                download_url = getDownloadLargeNoSound(driver)
                capture_screenshot(driver)
                high_quality_dir = os.path.join(subject_download_dir, 'HighQuality')

                os.makedirs(high_quality_dir, exist_ok=True)
                if download_url == None:
                    continue
                capture_screenshot(driver)
                download_video_High_quality(download_url, id, high_quality_dir)
                            # Construct the full path to the video file
                capture_screenshot(driver)
                getDownloadSmallVideosWithSound(driver)
                capture_screenshot(driver)
                print("Download link clicked successfully")
                print("3. ")
                if check_value_above_400(driver):
                    getDownloadSmallVideosWithSound(driver)
                    PopUpLargeVideos(driver)
            check_and_rename_mp4_files(subject_download_dir, timestamp,id+"_test_360")
            totalDuration += float(value.get('duration'))
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