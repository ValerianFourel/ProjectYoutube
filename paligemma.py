import torch
from transformers import PaliGemmaForConditionalGeneration, AutoProcessor
from PIL import Image
import requests
from ImageUtils import process_screenshot
text_prompt = "Give me only the 4 characters"
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from selenium.common.exceptions import TimeoutException, NoSuchElementException


class PaliGemma:
    def __init__(self, model_id="google/paligemma-3b-mix-224", max_new_tokens=1000):
        self.model = PaliGemmaForConditionalGeneration.from_pretrained(model_id)
        self.processor = AutoProcessor.from_pretrained(model_id)
        self.max_new_tokens = max_new_tokens

    def run(self, task, image_path):
        image = Image.open(image_path)
        # image = Image.open(requests.get(image_url, stream=True).raw)
        inputs = self.processor(task, image, return_tensors="pt")
        output = self.model.generate(**inputs, max_new_tokens=self.max_new_tokens)
        return self.processor.decode(output[0], skip_special_tokens=True)[len(task):]

def check_and_process_captcha(driver, model):
    try:
        # Wait for up to 10 seconds for the element to be present
        main_div = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "output-captcha-dialog"))
        )

        # If the element is found, process the captcha
        process_captcha(driver, model)

    except TimeoutException:
        # If the element is not found within 10 seconds, this block will execute
        print("Captcha dialog not found. Continuing without processing captcha.")


def process_captcha(driver, model, text_prompt= text_prompt):
    # Process screenshot and run model
    screenshot_path = process_screenshot(driver)
    max_attempts = 5

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

    # Get the second child div
    second_child_div = main_div.find_elements(By.XPATH, "./div")[1]

    # Find the form within the second child div
    form = second_child_div.find_element(By.TAG_NAME, "form")

    # Find the input field
    input_field = form.find_element(By.CSS_SELECTOR, "div.captcha-dialog__input__ctr > input[type='text'][name='val']")

    # Input the text
    attempt = 0
    text = model.run(text_prompt, screenshot_path).replace(" ", "")
    print("We get: ", text)
    input_field.send_keys(text)

    # Find and click the submit button
    submit_button = form.find_element(By.CSS_SELECTOR, "button.captcha-dialog__button[type='submit']")
    
    submit_button.click()
    time.sleep(3)
            # Check if the CAPTCHA dialog is still present
    while attempt < max_attempts:
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "output-captcha-dialog"))
            )
            # If we reach here, the CAPTCHA dialog is still present, so we need to retry
            print("CAPTCHA not solved. Retrying...")
            text_prompt_revised = f"{text} is not correct. {text_prompt}"
            attempt += 1
            text = model.run(text_prompt_revised, screenshot_path).replace(" ", "")
            print("We get: ", text)
            input_field.send_keys(text)

            # Find and click the submit button
            submit_button = form.find_element(By.CSS_SELECTOR, "button.captcha-dialog__button[type='submit']")
            
            submit_button.click()
        except TimeoutException:
            # If we reach here, the CAPTCHA dialog is no longer present, indicating success
            print("CAPTCHA solved successfully!")
            return True

