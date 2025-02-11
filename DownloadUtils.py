import os
import time
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
import time
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import requests
from urllib.parse import urlparse

from ImageUtils import capture_screenshot


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

        return True

    except TimeoutException:
        print("Timed out waiting for element to be present")
        return False
    except NoSuchElementException:
        print("Could not find the specified element")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False



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
        # Download the video in chunks
        chunk_size = 1024 * 1024  # 1MB
        response = requests.get(video_url, stream=True)
        print("RESPONSEEEE")

        if response.status_code == 200:
            with open(video_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    file.write(chunk)
            print(f"Download complete: {video_path}")
            return False
        else:
            print(f"Failed to download video. Status code: {response.status_code}")
            return False

    except Exception as e:
        print("Error:", str(e))
        return False


def PopUpLargeVideos(driver):
    """
    Function to interact with the video download popup.

    Args:
    driver: Selenium WebDriver instance

    Returns:
    bool: True if successful, False otherwise
    """
    try:
        # Wait for the popup to be present in the DOM
        popup = WebDriverWait(driver, 100).until(
            EC.presence_of_element_located((By.ID, "c-ui-popup"))
        )
        print("Popup found")

    # Wait for either the download button or text containing "Unable to download"
                # Wait for either the download button or popup title containing "Unable to download file"
        element = WebDriverWait(popup, 300).until(
            lambda x: x.find_element(By.CLASS_NAME, "c-ui-download-button") or 
                    x.find_element(By.CLASS_NAME, "c-ui-popup-title").text == "Unable to download file"
        )
        capture_screenshot(driver)


        # Check if the found element contains "Unable to download"
        if "Unable to download" in element.text:
            print("Unable to download message found")
            return False
            # download_button = popup.find_element(By.CLASS_NAME, "c-ui-download-button")

            # Click the download button
        element.click()

        print("Download button clicked successfully!")

            # Wait for the close button to be clickable
        close_button = WebDriverWait(driver, 100).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "c-ui-popup-btn-close"))
            )

            # Click the close button
        close_button.click()
        print("Close button clicked successfully!")
        return True

    except TimeoutException:
        print("Timed out waiting for popup to be present")
        capture_screenshot(driver)

        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False