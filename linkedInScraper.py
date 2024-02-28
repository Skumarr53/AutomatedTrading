from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from time import sleep
from utils import utils

# ChromeDriver path
chrome_driver_path = "/path/to/your/chromedriver"

# Setting up ChromeDriver

driver = webdriver.Chrome(options=utils.get_chrome_options())

# Function to scrape a LinkedIn profile
def scrape_linkedin_profile(url):
    driver.get(url)
    sleep(5)  # Adjust sleep time as necessary to ensure the page loads completely

    # Initialize Markdown content
    markdown_content = "# LinkedIn Profile\n\n"

    # Scrape the Profile Overview
    try:
        profile_overview = driver.find_element(By.CLASS_NAME, "pv-top-card")
        markdown_content += "## Profile Overview\n" + profile_overview.text.replace('\n', ', ') + "\n\n"
    except Exception as e:
        print("Error in scraping Profile Overview:", e)

    # Scrape the About Section
    try:
        about_section = driver.find_element(By.ID, "about-section")
        markdown_content += "## About\n" + about_section.text + "\n\n"
        print("About section succuful")
    except Exception as e:
        print("Error in scraping About section:", e)

    # Scrape the Experience Section
    try:
        experience_section = driver.find_element(By.ID, "experience-section")
        exp_items = experience_section.find_elements(By.CLASS_NAME, "pv-profile-section__list-item")
        markdown_content += "## Experience\n"
        for item in exp_items:
            markdown_content += item.text + "\n\n"
    except Exception as e:
        print("Error in scraping Experience section:", e)

    # Scrape the Education Section
    try:
        education_section = driver.find_element(By.ID, "education-section")
        edu_items = education_section.find_elements(By.CLASS_NAME, "pv-profile-section__list-item")
        markdown_content += "## Education\n"
        for item in edu_items:
            markdown_content += item.text + "\n\n"
    except Exception as e:
        print("Error in scraping Education section:", e)

    # Additional sections (e.g., Licenses & Certifications, Skills) can be added similarly...

    # Save to Markdown file
    with open("/home/skumar/OneTime/linkedin_profile.md", "w", encoding="utf-8") as file:
        file.write(markdown_content)

    # Close the driver
    driver.quit()

# URL of the LinkedIn profile
profile_url = "https://www.linkedin.com/in/santhosh-kumar-choori/"
scrape_linkedin_profile(profile_url)
