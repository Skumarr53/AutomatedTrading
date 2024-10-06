# Optimizing the auth/fyers_auth.py script

# Improvements:
# 1. Enhanced error handling for Selenium operations.
# 2. Secure handling of sensitive data like TOTP secrets.
# 3. Improved logging for better clarity.
# 4. Modularized code for improved readability and maintenance.
# 5. Ensured proper management of WebDriver.
import time
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium import webdriver
from pyotp import TOTP
from fyers_apiv3 import fyersModel  # accessToken
import os
from src.config.config import config, setup_logging
from src.utils import utils
import pyperclip
import webbrowser
from urllib.parse import parse_qs,urlparse


setup_logging()

class AuthCodeGenerator:
    def __init__(self):
        self.session = fyersModel.SessionModel(
            client_id=config.environment.app_settings.client_id,
            secret_key=config.environment.app_settings.secret_key,
            redirect_uri=config.trading_config.redirect_url,
            response_type=config.trading_config.response_type,
            grant_type=config.trading_config.response_type
        )
        self.username = config.environment.app_settings.user_name
        self.totp = config.environment.app_settings.totp_secret
        ## TODO: add user_pin to config
        self.pin = config.USER_PIN

    def initialize_fyers_model(self):
        try:
            auth_code = self.gen_auth_code()
            self.session.set_token(auth_code)
            response = self.session.generate_token()
            access_token = response["access_token"]
            fyers = fyersModel.FyersModel(
                client_id=config.environment.app_settings.client_id, is_async=False, token=access_token, log_path=os.getcwd())
            logging.info(fyers.get_profile())
            return fyers
        except Exception as e:
            logging.exception(f"Error in initializing Fyers Model: {e}")
            raise

    def gen_auth_code(self):
        driver = None
        try:
            # driver = webdriver.Chrome(options=utils.get_chrome_options())
            # driver.get(self.session.generate_authcode())
            url = self.session.generate_authcode()

            webbrowser.open_new(url)

            old_clipboard_contents = pyperclip.paste()
            new_clipboard_contents = pyperclip.paste()
            while old_clipboard_contents == new_clipboard_contents:
                new_clipboard_contents = pyperclip.paste()
                time.sleep(1)
            
            url = new_clipboard_contents
            parsed = urlparse(url)
            auth_code = parse_qs(parsed.query)["auth_code"][0]
            # EDIT
            # WebDriverWait(driver, 10).until(EC.presence_of_element_located(
            #     (By.XPATH, '//*[@id="fy_client_id"]')))
            # self._login(driver)
            # self._handle_human_validation(driver)
            # self._enter_otp(driver)
            # self._enter_pin(driver)
            # auth_code = self._extract_auth_code(driver)
            return auth_code
        except Exception as e:
            logging.exception(f"Error in generating auth code: {e}")
            raise
        finally:
            if driver:
                driver.quit()

    def _handle_human_validation(self, driver):
        try:
            # Wait for the human validation checkbox to be clickable
            human_validation_checkbox = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '#qquD4 > div > label > input[type=checkbox]')))
            
            # Click the checkbox
            human_validation_checkbox.click()
            
            # Optionally wait for a second to ensure the validation is processed
            time.sleep(1)
        except Exception as e:
            logging.error(f"Error during human validation: {e}")
            raise

    def _login(self, driver):
        try:
            client_id_field = driver.find_element(
                By.XPATH, '//*[@id="fy_client_id"]')
            client_id_field.send_keys(self.username)
        except Exception as e:
            logging.info(
                f"Standard login field not found, attempting alternative login. Error: {e}")
            self._alternative_login(driver)

    def _alternative_login(self, driver):
        login_alternate_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="login_client_id"]')))
        login_alternate_button.click()
        client_id_field = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="fy_client_id"]')))
        client_id_field.send_keys(self.username)
        time.sleep(2)
        self._handle_human_validation(driver)
        submit_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="clientIdSubmit"]')))
        submit_button.click()

    def _enter_otp(self, driver):
        otp = TOTP(self.totp).now()
        logging.info(f'OTP: {otp}')
        otp_field_ids = ["first", "second",
                         "third", "fourth", "fifth", "sixth"]
        for i, otp_digit in enumerate(otp):
            otp_field = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, f'//*[@id="{otp_field_ids[i]}"]')))
            otp_field.send_keys(otp_digit)
        driver.find_element(By.XPATH, '//*[@id="confirmOtpSubmit"]').click()

    def _enter_pin(self, driver):
        pin_page = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "verify-pin-page")))
        pin_field_ids = ["first", "second", "third", "fourth"]
        for i, pin_digit in enumerate(self.pin):
            pin_field = WebDriverWait(pin_page, 5).until(
                EC.element_to_be_clickable((By.ID, pin_field_ids[i])))
            pin_field.send_keys(pin_digit)
        driver.find_element(By.XPATH, '//*[@id="verifyPinSubmit"]').click()

    def _extract_auth_code(self, driver):
        WebDriverWait(driver, 10).until(EC.url_contains("auth_code="))
        new_url = driver.current_url
        return new_url[new_url.index('auth_code=') + 10:new_url.index('&state')]

# Commenting out the execution part to prevent execution in the PCI
# if __name__=="__main__":
#     logging
