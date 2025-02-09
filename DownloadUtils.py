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

def capture_screenshot(driver):
    # Create timestamp
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    # Define screenshot path
    screenshot_dir = 'tmpImages'
    screenshot_filename = f'tmp-{timestamp}.png'
    screenshot_path = os.path.join(screenshot_dir, screenshot_filename)

    # Ensure the directory exists
    os.makedirs(screenshot_dir, exist_ok=True)

    # Capture and save the screenshot
    driver.save_screenshot(screenshot_path)

    print(f"Screenshot saved: {screenshot_path}")

    return screenshot_path




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

        # Get the second link-group
        second_link_group = link_groups[1]

        # Find the first link in the second link-group
        download_link = second_link_group.find_element(By.TAG_NAME, "a")

        # Get the href attribute before clicking
        download_url = download_link.get_attribute('href')
        print('Download URL: ', download_url)

        # Click the link
        download_link.click()

        return download_url

    except TimeoutException:
        print("Timed out waiting for element to be present")
    except NoSuchElementException:
        print("Could not find the specified element")
    except Exception as e:
        print(f"An error occurred: {e}")

    return None



def getDownloadLargeNoSound_(driver):
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

        # Wait for the list to be visible
        list_div = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.CLASS_NAME, "list"))
        )

        # Find all divs with class "link-group"
        link_groups = list_div.find_elements(By.CLASS_NAME, "link-group")

        # Check if there are at least two link-groups
        if len(link_groups) < 2:
            print("Not enough link-groups found")
            return None

        # Get the second link-group
        second_link_group = link_groups[1]

        # Find the first link in the second link-group
        download_link = second_link_group.find_element(By.TAG_NAME, "a")

        # Get the href attribute before clicking
        download_url = download_link.get_attribute('href')
        print('Download URL: ', download_url)

        # Click the link
        download_link.click()

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
        close_button = WebDriverWait(driver, 10).until(
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
