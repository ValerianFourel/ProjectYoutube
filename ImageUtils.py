import os
import datetime
import cv2
from PIL import Image


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

