import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



def scrape_data(dates, executable_path="drivers/chromedriver.exe", show_output=False):
    service = Service(executable_path=executable_path)
    driver = webdriver.Chrome(service=service)
    all_data = []

    for match_date in dates:
        formatted_date = pd.to_datetime(match_date).strftime('%Y-%b-%d')
        football_url = f'https://www.twtd.co.uk/league-tables/competition:premier-league/daterange/fromdate:2023-Aug-01/todate:{formatted_date}/type:home-and-away/'
        driver.get(football_url)
        wait = WebDriverWait(driver, 10)
        table_container = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "leaguetable")))
        rows = table_container.find_elements(By.TAG_NAME, "tr")

        for idx, row in enumerate(rows[1:], start=1):
            cols = row.find_elements(By.TAG_NAME, "td")
            row_data = [col.text.strip() for col in cols if col.text.strip() != '']
            print(f"Row data {idx}: {row_data}") if show_output else None
            row_data.append(formatted_date) 
            all_data.append(row_data)

        if show_output:
            print(f"Premier League Table Standings (as of {formatted_date}):")
            print('-'*60)
            for row_data in all_data:
                print(' '.join(row_data))
            print('\n' + '-'*60)

        driver.implicitly_wait(2)
    
    driver.quit()
    

    # columns = ["Pos", "Team", "P", "W1", "D1", "L1", "GF1", "GA1", "W2", "D2", "L2", "GF2", "GA2", "GD", "Pts", "Date"].lower()
    columns = ["pos", "team", "p", "w1", "d1", "l1", "gf1", "ga1", "w2", "d2", "l2", "gf2", "ga2", "gd", "pts", "date"]
    df = pd.DataFrame(all_data, columns=columns)
    return df
