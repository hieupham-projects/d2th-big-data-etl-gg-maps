from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

options = webdriver.ChromeOptions()
options.add_argument('headless')

driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

def scrape_google_maps(keyword, location, category, results_limit=20):
    base_url = "https://www.google.com/maps/"
    driver.get(base_url)
    time.sleep(5)
    
    # Locate the search bar, input the keyword, and press enter
    search_bar = driver.find_element(By.ID, "searchboxinput")
    search_bar.clear()
    search_bar.send_keys(keyword)
    time.sleep(1)
    search_bar.send_keys(Keys.RETURN)
    time.sleep(15)  # Wait for the page to load
    
    places_data = []
    place_cards = driver.find_elements(By.XPATH, "//a[starts-with(@href, 'https://www.google.com/maps/place/')]")[:results_limit]
    
    for place in place_cards:
        try:
            # Extract place name and URL            
            place_url = place.get_attribute("href")
            
            # Name is aria-label of place element
            name = place.get_attribute("aria-label")
            
            # Append data (keyword, place URL, and address)
            places_data.append([category, location, keyword, name, place_url])
            
        except Exception as e:
            print(f"Error scraping place: {e}")
    
    return places_data

def get_url(ti) -> None:
    locations = ["Quận 1, TP. Hồ Chí Minh"]
    categories = ["nhà hàng", "phở"] 

    all_data = []
    for location in locations:
        for category in categories:
            keyword = f"{category} ở {location}"
            print(f"Scraping data for: {keyword}")
            places_data = scrape_google_maps(keyword, location, category)
            print(f"Found {len(places_data)} places")
            all_data.extend(places_data)

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