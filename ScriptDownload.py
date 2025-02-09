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

# Check if CUDA is available
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")

from paligemma import PaliGemma

# Initialize the PaliGemma model
model = PaliGemma()
text_prompt = "Give me only the 4 characters"
# Set up logging
logging.basicConfig(level=logging.INFO)

# This needs to run only once to load the model into memory



def capture_screenshot(driver):
    # Create timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

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



def process_screenshot(driver):
    # Take the screenshot
    screenshot_path = capture_screenshot(driver)

    # Open the screenshot for editing
    img = Image.open(screenshot_path)

    # Get the width and height of the image
    width, height = img.size

    # Calculate the coordinates of the middle rectangle in a 3 by 3 grid
    left = width / 3
    top = height / 3
    right = 2 * (width / 3)
    bottom = height / 2  # Adjust to get the upper half of the middle rectangle

    # Crop the image
    img_cropped = img.crop((left, top, right, bottom))

    # Save the cropped image as a JPG file
    if img_cropped.mode == 'RGBA':
        img_cropped = img_cropped.convert('RGB')
    img_cropped.save(screenshot_path)
    
    # Load the image
    img = cv2.imread(screenshot_path)

    # Convert the image to grayscale
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # Apply a binary threshold to the grayscale image
    _, binary = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY)

    # Find contours in the binary image
    contours, _ = cv2.findContours(binary, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    # Get the bounding box of the largest contour
    x, y, w, h = cv2.boundingRect(max(contours, key = cv2.contourArea))

    # Crop the original image using the coordinates of the bounding box
    crop_img = img[y:y+h, x:x+w]
    
    # Save the cropped image as a JPG file
    cv2.imwrite(screenshot_path, crop_img)

    return screenshot_path

# Set up the Firefox profile
firefox_profile = webdriver.FirefoxProfile()
# Set download directory path
firefox_profile.set_preference("browser.download.folderList", 2)  # 2 means custom directory
firefox_profile.set_preference("browser.download.dir", 
"/home/vfourel/ProjectGym/FireFox")
# Add .mp4 MIME type to download preferences
firefox_profile.set_preference("browser.helperApps.neverAsk.saveToDisk", 
"video/mp4")
firefox_profile.set_preference("media.play-stand-alone", False)

# Set up the GeckoDriver path
geckodriver_path = "/usr/local/bin/geckodriver"  # Replace with the actual path to geckodriver

# Set up Firefox options
firefox_options = FirefoxOptions()
firefox_options.add_argument("--headless")  # Run in headless mode
firefox_options.log.level = "trace"  # Enable verbose logging

# Set up the Firefox service
firefox_service = FirefoxService(executable_path=geckodriver_path)

try:
    logging.info("Initializing WebDriver")
    print('Initializing WebDriver')
    driver = webdriver.Firefox(service=firefox_service, options=firefox_options, firefox_profile = firefox_profile)

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
    string_to_send ="https://www.youtube.com/watch?v=NqvooBJ8FY8" #  "https://www.youtube.com/watch?v=jEF-u4ntt2Q"
    input_area.send_keys(string_to_send)

    logging.info("Waiting for 2 seconds")
    time.sleep(2)

    logging.info("Locating and clicking the download button")
    print("Locating and clicking the download button")
    download_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "sf_submit"))
    )
    download_button.click()
    logging.info("Waiting for 5 seconds")

    time.sleep(5)

    # Take the screenshot
    screenshot_path = process_screenshot(driver)

    text = model.run(text_prompt,screenshot_path)


    print("We get: ",text)
        # Wait for the main div to be present
    main_div = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "output-captcha-dialog"))
    )
        # Get all elements inside main_div
    all_elements = main_div.find_elements(By.XPATH, ".//*")

    print(f"Total elements found inside main_div: {len(all_elements)}")

    # Get all direct child div elements of main_div
    child_divs = main_div.find_elements(By.XPATH, "./div")

    print(f"Number of direct child div elements: {len(child_divs)}")

    # Print information about each child div
    for index, div in enumerate(child_divs, start=1):
        div_class = div.get_attribute("class")
        div_text = div.text.strip() if div.text else "No text"

        print(f"\nChild Div {index}:")
        print(f"  Class: {div_class or 'No class'}")
        print(f"  Text: {div_text}")

        # Get the second child div
    second_child_div = main_div.find_elements(By.XPATH, "./div")[1]

    print("Second Child Div:")
    print(f"  Class: {second_child_div.get_attribute('class')}")

    # Find the form within the second child div
    form = second_child_div.find_element(By.TAG_NAME, "form")

     # Find the input field
    input_field = form.find_element(By.CSS_SELECTOR, "div.captcha-dialog__input__ctr > input[type='text'][name='val']")

    # Input the text (replace 'Your Text Here' with the actual text you want to input)
    input_field.send_keys(text)

    # Find and click the submit button
    submit_button = form.find_element(By.CSS_SELECTOR,"button.captcha-dialog__button[type='submit']")
    submit_button.click()
    time.sleep(1)
    download_button = WebDriverWait(driver, 10).until(
     EC.element_to_be_clickable((By.ID, "sf_submit")))
    download_button.click()
    print('We click on the download button again')
    time.sleep(1)
    capture_screenshot(driver)
    try:
    # Wait for the section to be present
        section = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "landingTz-main-screen-top"))
    )

    # Find the "with low quality" link and click it
        low_quality_link = section.find_element(By.XPATH, ".//a[@class='landingTz-btn-close' and contains(text(), 'with low quality')]")

        print("Found the 'with low quality' link. Clicking it now...")
        low_quality_link.click()
        print("Successfully clicked the 'with low quality' link.")
    except Exception as e:
            # Wait for the sf_result div to be present
        sf_result = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "sf_result")))

    # Find the download link within sf_result
        download_link = sf_result.find_element(By.CSS_SELECTOR, "a.link.link-download.subname.ga_track_events.download-icon")

    # Click the download link
        download_link.click()

        print("Download link clicked successfully")
        # Wait for the sf_result div to be present

    # Find the download link within sf_result
# capture_screenshot(driver)
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
    logging.info("Closing WebDriver")
    if 'driver' in locals():
        driver.quit()
