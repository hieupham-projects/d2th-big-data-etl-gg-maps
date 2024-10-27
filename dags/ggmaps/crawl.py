from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

locations = ["Quận 1, TP. Hồ Chí Minh"]
categories = ["nhà hàng", "phở"] 

def scrape_google_maps(location, category, results_limit=10):
    places_data = []
    try:
        firefox_options = Options()
        firefox_options.add_argument("--headless")
        driver = webdriver.Remote(
            command_executor='http://remote_firefoxdriver:4444/wd/hub',
            options=firefox_options
        )
        keyword = f"{category} in {location}"
        driver.get(f"https://www.google.com/maps/search/{keyword}")
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "bfdHYd")))
        
        wait.until(EC.presence_of_all_elements_located((By.XPATH, "//a[starts-with(@href, 'https://www.google.com/maps/place/')]")))
        
        
        place_cards = driver.find_elements(By.XPATH, "//a[starts-with(@href, 'https://www.google.com/maps/place/')]")[:results_limit]
        
        for place in place_cards:
            try:
                place_url = place.get_attribute("href")
                name = place.get_attribute("aria-label")
                places_data.append([category, location, keyword, name, place_url])       
            except Exception as e:
                print(f"Error scraping place: {e}")
    except Exception as e:
        print(f"Error scraping Google Maps: {e}")
    finally:
        driver.quit()
    return places_data

def get_url(ti) -> None:    
    all_data = []
    for location in locations:
        for category in categories:
            places_data = scrape_google_maps(location, category)
            print(f"Found {len(places_data)} places")
            all_data.extend(places_data)
    print(all_data)

def crawl_all(ti) -> None:
    print("Crawling all")

def crawl_tasks():
    with TaskGroup(
            group_id="crawling",
            tooltip="Crawling GGMaps"
    ) as group:

        get_url_task = PythonOperator(
            task_id='get_url',
            python_callable=get_url
        )

        crawl_all_task = PythonOperator(
            task_id='crawl_all',
            python_callable=crawl_all
        )

        get_url_task >> crawl_all_task

        return group