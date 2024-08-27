import os
from supabase import create_client, Client
from playwright.sync_api import sync_playwright
from homeharvest import scrape_property
from datetime import datetime
import pandas as pd
from pandas.tseries.offsets import DateOffset
import requests
from bs4 import BeautifulSoup
import json
import numpy as np

args=['--disable-gpu',
      '--single-process',
        "--no-sandbox",
        "--disable-infobars",
        "--start-maximized",
        "--window-position=-10,0",
    ]

ignore_default_args = ["--enable-automation"]

proxies = {
        "http": "http://hizxybhc:7etyqbb24fqo@207.228.7.25:7207",
    }

# Initialize Supabase client
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY')
SERVER = os.getenv('SERVER')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')

supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

def calculate_crime_score(county: str, city: str, listing_id: str):
    try:
        county = county.replace(" ", "_").lower()
        city = city.replace(" ", "_").lower()
    except Exception as e:
        print(f"Error processing county or city name: {e}")
        update_flags(listing_id, "Error processing county or city name.")
        return {"message": "Error processing county or city name."}

    try:
        # Example query to fetch data from a table
        response = supabase.table('crime_data_ca').select(
            'all_violent_crime_trend, agency_name, crime_location, victim_age, victim_ethnicity, victim_race, victim_age, id'
        ).or_(
            f"agency_name.ilike.%{city}%,agency_name.ilike.%{county}%"
        ).execute()
    except Exception as e:
        print(f"Error fetching crime_score data from Supabase: {e}")
        update_flags(listing_id, "Error fetching crime_score data from Supabase.")
        return {"message": "Error fetching data from database."}

    try:
        # Check if there's any city-level data
        city_data = [item for item in response.data if city.lower() in item['agency_name'].lower()]
        county_data = [item for item in response.data if county.lower() in item['agency_name'].lower() and city.lower() not in item['agency_name'].lower()]

        # Combine city_data and county_data
        combined_data = city_data + county_data

        crime_data_ids = []
        # Append crime_data_ids from combined_data
        for item in combined_data:
            crime_data_ids.append(item['id'])
    except Exception as e:
        print(f"Error processing fetched data: {e}")
        update_flags(listing_id, "Error processing fetched data.")
        return {"message": "Error processing fetched data."}

    # Determine which data to process
    try:
        if city_data:
            data_to_process = city_data
            print(f"Processing city-level data for {city}")
        else:
            data_to_process = county_data
            print(f"Processing county-level data for {county}")
    except Exception as e:
        print(f"Error determining data to process: {e}")
        update_flags(listing_id, "Error determining data to process.")
        return {"message": "Error determining data to process."}

    scores = []
    try:
        # Calculate the scores
        for res in range(len(data_to_process)):
            print(f"Processing item {res + 1} of {len(data_to_process)}")

            # Print the response data for the current item
            print(data_to_process[res])

            averages = []
            for i in range(2012, 2023):
                if i == 2021:
                    continue
                try:
                    divisor = data_to_process[res]['all_violent_crime_trend'][0][f'{i}']
                    if divisor != 0:
                        result = data_to_process[res]['all_violent_crime_trend'][1][f'{i}'] / divisor
                        averages.append(result)
                        print(f"Year: {i}, Result: {result}")
                except KeyError:
                    print(f"Year: {i} data is missing or incomplete.")
                    continue

            # Calculate the average percentage
            if averages:
                avg_pct = sum(averages) / len(averages)
                score_10 = avg_pct * 10
            else:
                score_10 = 0  # Handle cases with no valid data

            # Store the score
            scores.append(score_10)
            print(f"Score for item {res + 1}: {score_10}")
    except Exception as e:
        print(f"Error calculating scores: {e}")
        update_flags(listing_id, "Error calculating scores.")
        return {"message": "Error calculating scores."}

    try:
        # If there are multiple scores (e.g., city has both police and sheriff's departments), calculate the average score
        if len(scores) > 1:
            crime_score = sum(scores) / len(scores)
        else:
            crime_score = scores[0] if scores else 0
    except Exception as e:
        print(f"Error calculating crime score: {e}")
        update_flags(listing_id, "Error calculating crime score.")
        return {"message": "Error calculating crime score."}

    try:
        # Update the reports table with crime_data_ids and crime_score
        update_data = {
            'crime_data_ids': crime_data_ids,
            'crime_score': crime_score
        }

        response = supabase.table('reports').update(update_data).eq('listing_id', listing_id).execute()
        print(f"Updated report {listing_id} with crime_data_ids: {crime_data_ids}")
    except Exception as e:
        print(f"Error updating report {listing_id}: {str(e)}")
        update_flags(listing_id, "Error updating report.")
        return {"message": f"Error updating report {listing_id}."}

    return crime_score, data_to_process

def scrape_home_details(address, listing_id):
    print("scraping home details")
    url = "https://www.homes.com/routes/res/consumer/property/autocomplete"
    print(f"url {url}")
    headers = {
        "accept": "application/json",
        "content-type": "application/json-patch+json",
        "referer": "https://www.homes.com/",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    }
    print(f"headers {headers}")
    body = {
        "term": address,
        "transactionType": 1,
        "limitResult": False,
        "includeAgent": True,
        "includeSchools": True,
        "placeOnlySearch": False
    }
    print(f"body {body}")
    try:
        response = requests.post(url, headers=headers, json=body, timeout=10, proxies=proxies)
        print("getting home url")
        response.raise_for_status()
        print(f"response {response}")
        data = response.json()
        print(f"data {data}")
        home_url = 'https://www.homes.com' + data['suggestions']['places'][0]['u']
    except Exception as e:
        print(f"Error fetching home URL: {e}")
        update_flags(listing_id, "Error fetching home URL.")
        return

    get_headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
        "priority": "u=1, i",
        "cache-control": "max-age=0, no-cache, no-store",
        "sec-ch-ua": '"Google Chrome";v="93", "Chromium";v="93", "Not;A Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "upgrade-insecure-requests": "1",
        "user-agent": 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) EdgiOS/117.0.2045.48 Version/17.0 Mobile/15E148 Safari/604.1',
        "content-type": "text/html; charset=utf-8",
    }

    try:
        print(f"home_url {home_url}")
        get_response = requests.get(home_url, headers=get_headers, proxies=proxies, timeout=10)
        print(f"get_response {get_response}")
        get_response.raise_for_status()
        print(f"get_response.text {get_response.text}")
    except Exception as e:
        print(f"Error fetching home details: {e}")
        update_flags(listing_id, "Error fetching home details.")
        return

    soup = BeautifulSoup(get_response.text, 'html.parser')
    print(f"soupifying")
    def safe_get_text(selector, default=""):
        element = soup.select_one(selector)
        return element.get_text(strip=True) if element else default

    home_details = {
        "price": safe_get_text("#price"),
        "views": safe_get_text(".total-views"),
        "highlights": [
            highlight.select_one(".highlight-value").get_text(strip=True)
            for highlight in soup.select("#highlights-section .highlight")
        ] if soup.select("#highlights-section .highlight") else []
    }
    print("Finished extracting highlights")

    home_details.update({
        "home_details": [
            {
                "label": subcategory.select_one(".amenity-name").get_text(strip=True),
                "details": [detail.get_text(strip=True) for detail in subcategory.select(".amenities-detail")]
            }
            for subcategory in soup.select("#amenities-container .subcategory")
        ] if soup.select("#amenities-container .subcategory") else []
    })
    print("Finished extracting home details")

    home_details.update({
        "neighborhood_kpis": [
            {
                "title": kpi.select_one(".neighborhood-kpi-card-title").get_text(strip=True),
                "text": kpi.select_one(".neighborhood-kpi-card-text").get_text(strip=True)
            }
            for kpi in soup.select(".neighborhood-kpi-card")
        ] if soup.select(".neighborhood-kpi-card") else []
    })
    print("Finished extracting neighborhood KPIs")

    home_details.update({
        "tax_history": [
            {
                "year": row.select_one(".tax-year").get_text(strip=True),
                "tax_paid": row.select_one(".tax-amount").get_text(strip=True),
                "tax_assessment": row.select_one(".tax-assessment").get_text(strip=True),
                "land": row.select_one(".tax-land").get_text(strip=True),
                "improvement": row.select_one(".tax-improvement").get_text(strip=True)
            }
            for row in soup.select("#tax-history-container .tax-table .tax-table-body .tax-table-body-row")
        ] if soup.select("#tax-history-container .tax-table .tax-table-body .tax-table-body-row") else []
    })
    print("Finished extracting tax history")

    home_details.update({
        "price_history": [
            {
                "date": row.select_one(".price-year .long-date").get_text(strip=True) if row.select_one(".price-year .long-date") else "",
                "event": row.select_one(".price-event").get_text(strip=True) if row.select_one(".price-event") else "",
                "price": row.select_one(".price-price").get_text(strip=True) if row.select_one(".price-price") else "",
                "change": row.select_one(".price-change").get_text(strip=True) if row.select_one(".price-change") else "",
                "sq_ft_price": row.select_one(".price-sq-ft").get_text(strip=True) if row.select_one(".price-sq-ft") else ""
            }
            for row in soup.select("#price-history-container .price-table .table-body-row")
        ] if soup.select("#price-history-container .price-table .table-body-row") else []
    })
    print("Finished extracting price history")

    home_details.update({
        "deed_history": [
            {
                "date": row.select_one(".deed-date .shorter-date").get_text(strip=True) if row.select_one(".deed-date .shorter-date") else "",
                "type": row.select_one(".deed-type").get_text(strip=True) if row.select_one(".deed-type") else "",
                "sale_price": row.select_one(".deed-sale-price").get_text(strip=True) if row.select_one(".deed-sale-price") else "",
                "title_company": row.select_one(".deed-title-company").get_text(strip=True) if row.select_one(".deed-title-company") else ""
            }
            for row in soup.select("#deed-history-container .deed-table .deed-table-body-row")
        ] if soup.select("#deed-history-container .deed-table .deed-table-body-row") else []
    })
    print("Finished extracting deed history")

    home_details.update({
        "mortgage_history": [
            {
                "date": row.select_one(".mortgage-date .shorter-date").get_text(strip=True) if row.select_one(".mortgage-date .shorter-date") else "",
                "status": row.select_one(".mortgage-status").get_text(strip=True) if row.select_one(".mortgage-status") else "",
                "loan_amount": row.select_one(".mortgage-amount").get_text(strip=True) if row.select_one(".mortgage-amount") else "",
                "loan_type": row.select_one(".mortgage-type").get_text(strip=True) if row.select_one(".mortgage-type") else ""
            }
            for row in soup.select("#mortgage-history-container .mortgage-table .table-body-row")
        ] if soup.select("#mortgage-history-container .mortgage-table .table-body-row") else []
    })
    print("Finished extracting mortgage history")

    home_details.update({
        "transportation": [
            {
                "type": item.select_one(".transportation-type").get_text(strip=True) if item.select_one(".transportation-type") else "",
                "name": item.select_one(".transportation-name").get_text(strip=True) if item.select_one(".transportation-name") else "",
                "distance": item.select_one(".transportation-distance").get_text(strip=True) if item.select_one(".transportation-distance") else ""
            }
            for item in soup.select("#transportation-container .transportation-item")
        ] if soup.select("#transportation-container .transportation-item") else []
    })
    print("Finished extracting transportation details")

    home_details.update({
        "bike_score": {
            "tagline": safe_get_text("#score-card-container .bike-score .score-card-tagline"),
            "score": safe_get_text("#score-card-container .bike-score .score-scoretext")
        },
        "walk_score": {
            "tagline": safe_get_text("#score-card-container .walk-score .score-card-tagline"),
            "score": safe_get_text("#score-card-container .walk-score .score-scoretext")
        }
    })
    print("Finished extracting bike and walk scores")

    

    
    try:
        supabase.table('reports').update({
            'home_details': json.dumps(home_details)
        }).eq('listing_id', listing_id).execute()
        print("Home details successfully uploaded.")
    except Exception as e:
        print(f"Error uploading home details to Supabase: {e}")
        update_flags(listing_id, "Error uploading home details to Supabase.")

def scrape_schooldigger(street_line, city, state, zipcode, lat, long, listing_id):
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    url = f"https://www.schooldigger.com/go/CA/search.aspx?searchtype=11&address={street_line.replace(' ', '+')}&city={city.replace(' ', '+')}&state={state}&zip={zipcode}&lat={lat}&long={long}"

    with sync_playwright() as p:
        print("Launching browser...")
        
        browser = p.chromium.launch(
            headless=True,
            args=args,
            timeout=120000,
        )
        print("Browser launched successfully.")

        page = browser.new_page(user_agent=ua)
        # page.set_extra_http_headers({
        #     "sec-ch-ua": '"Chromium";v="125", "Not.A/Brand";v="24"'
        # }) 
        # UNCOMMENT AND TRY IF IT DONT WORK

        try:
            print(f"Navigating to URL: {url}")  # Debugging statement
            page.goto(url, timeout=120000)  # Increase timeout to 120 seconds
            page.wait_for_load_state("domcontentloaded", timeout=120000)  # Wait for DOM content to load
            print("Page loaded.")  # Debugging statement

            # Wait for the table tab to be clickable and click it
            page.wait_for_selector("xpath=/html/body/form/div[5]/div[5]/ul/li[4]/a", timeout=120000)
            page.click("xpath=/html/body/form/div[5]/div[5]/ul/li[4]", timeout=120000)
            print("Clicked on the table tab.")  # Debugging statement

            # Wait for the all button to be clickable and click it
            page.wait_for_selector("xpath=/html/body/form/div[5]/div[6]/div[3]/div[1]/a[8]", timeout=120000)
            page.click("xpath=/html/body/form/div[5]/div[6]/div[3]/div[1]/a[8]", timeout=120000)
            print("Clicked on the 'All' button.")  # Debugging statement
            print("Waiting for the page to update...")  # Debugging statement
            page.wait_for_timeout(2000)

            # Scrape the table data
            print("Scraping table data...")  # Debugging statement
            table = page.query_selector("table.table.table-hover.table-condensed.table-striped.table-bordered.gSurvey.dataTable.no-footer")
            rows = page.query_selector_all('//*[@id="tabSchooList"]/tbody/tr')  # Get all rows from the specified XPath

            school_data = []
            headers = [header.inner_text() for header in page.query_selector_all('//*[@id="tabSchooList_wrapper"]/div/div[2]/div/div[1]/div/table/thead/tr[2]/th')]  # Extract headers from specified XPath
            for row in rows:  # Iterate through all rows
                cols = row.query_selector_all("td")  # Get all columns in the current row
                row_data = {headers[i]: col.inner_text() for i, col in enumerate(cols)}  # Map headers to data
                school_data.append(row_data)  # Append the row data as a dictionary
            print("Data scraped successfully.")  # Debugging statement
            browser.close()

            calculate_school_data(school_data, listing_id)
            print("Done calculating school data")
            return

        except Exception as e:
            browser.close()
            print(f"Error in scrape_schooldigger: {str(e)}")
            update_flags(listing_id, "Error in scrape_schooldigger.")
            raise

def calculate_school_data(school_data, listing_id):

    # TODO: REFINE
    def calculate_school_score(school):
        max_score = 100 
        score = 0

        if school["State Percentile (2023)"]:
            score += float(school["State Percentile (2023)"].strip('%')) / 10
        if school["Average Standard Score (2023)"]:
            score += float(school["Average Standard Score (2023)"])
        if school["Distance"]:
            score -= float(school["Distance"].strip('mi')) * 2  
        if school["Student/\nTeacher Ratio"]:
            score += 100 / float(school["Student/\nTeacher Ratio"])
        
        score = max(0, min(score, max_score))
        return score

    # Calculate scores for all schools
    for school in school_data:
        school["Score"] = calculate_school_score(school)


    # Sort schools by distance
    school_data.sort(key=lambda x: float(x["Distance"].strip('mi')))

    def grade_in_range(grade_range, target_grade):
        start, end = grade_range.split('-')
        start = 0 if start == 'K' else int(start)
        end = int(end)
        return start <= target_grade <= end

    # Separate the closest 2 elementary, middle, and high schools based on grades
    closest_schools = {
        "elementary": [school for school in school_data if grade_in_range(school["Grades"], 3)][:2],
        "middle": [school for school in school_data if grade_in_range(school["Grades"], 7)][:2],
        "high": [school for school in school_data if grade_in_range(school["Grades"], 10)][:2]
    }
    
    # calc the average
    # Sort schools by score in descending order
    top_schools = sorted(school_data, key=lambda x: x["Score"], reverse=True)

    # Calculate the average score of the top 3 schools
    average_top_score = sum(school["Score"] for school in top_schools) / len(top_schools)

    # Update Supabase row in table 'reports' with id = listing_id with 'school_score' and 'top_schools'
    supabase.table('reports').update({
        'school_score': average_top_score,
        'top_schools': json.dumps(closest_schools)
    }).eq('listing_id', listing_id).execute()

def scrape_address_data(address, listing_id):
    past_days = 5*365  # 5 years worth of days
    radius = 0.5  # 0.5 mile radius

    all_properties = pd.DataFrame()

    # Fetch properties for sale, sold, and pending
    try:
        for listing_type in ['for_sale', 'sold', 'pending']:
            properties = scrape_property(
                location=address,
                listing_type=listing_type,
                radius=radius,
                past_days=past_days,
                extra_property_data=True,
            )
            all_properties = pd.concat([all_properties, properties])
    except Exception as e:
        print(f"Failed to scrape properties data: {e}")
        update_flags(listing_id, "Failed to scrape properties data.")



    try:
       
        pending_properties = scrape_property(
                location=address,
                listing_type="pending",
                radius=1.5,
                past_days=90,
                extra_property_data=True,
            )
        
    except Exception as e:
        print(f"Failed to scrape pending properties data: {e}")
        update_flags(listing_id, "Failed to scrape properties data.")



    current_year = datetime.now().year
    metrics = {}
  



    # Calculate property metrics
    try:
        metrics = {
            'average_year_built': current_year - (current_year - all_properties['year_built']).mean(),
            'median_year_built': current_year - (current_year - all_properties['year_built']).median(),
            'range_year_built': all_properties['year_built'].max() - all_properties['year_built'].min(),
            'average_lot_size': all_properties['lot_sqft'].mean(),
            'median_lot_size': all_properties['lot_sqft'].median(),
            'range_lot_size': all_properties['lot_sqft'].max() - all_properties['lot_sqft'].min(),
            'average_price_per_sqft': all_properties['price_per_sqft'].mean(),
            'median_price_per_sqft': all_properties['price_per_sqft'].median(),
            'range_price_per_sqft': all_properties['price_per_sqft'].max() - all_properties['price_per_sqft'].min(),
            'average_estimated_price': all_properties['list_price'].mean(),
            'median_estimated_price': all_properties['list_price'].median(),
            'range_estimated_price': all_properties['list_price'].max() - all_properties['list_price'].min(),
            'average_house_size': all_properties['sqft'].mean(),
            'median_house_size': all_properties['sqft'].median(),
            'range_house_size': all_properties['sqft'].max() - all_properties['sqft'].min(),
            'average_days_on_market': all_properties['days_on_mls'].mean(),
            'median_days_on_market': all_properties['days_on_mls'].median(),
            'range_days_on_market': all_properties['days_on_mls'].max() - all_properties['days_on_mls'].min(),
            'comparable_homes': pending_properties.to_dict()
        }
    except Exception as e:
        print(f"Failed to calculate property metrics: {e}")
        update_flags(listing_id, "Failed to calculate property metrics.")

    # Calculate sales volume
    # YOY
    try:
        current_date = datetime.now()
        all_properties['last_sold_date'] = pd.to_datetime(all_properties['last_sold_date'], errors='coerce')

        past_5_years = current_date - DateOffset(years=5)
        past_year = current_date - DateOffset(years=1)
        past_month = current_date - DateOffset(months=1)

        total_properties = len(all_properties)
        sold_past_5_years = len(all_properties[(all_properties['last_sold_date'] >= past_5_years)])
        sold_past_year = len(all_properties[(all_properties['last_sold_date'] >= past_year)])
        sold_past_month = len(all_properties[(all_properties['last_sold_date'] >= past_month)])

        metrics.update({
            'total_properties': total_properties,
            'sold_past_5_years': sold_past_5_years,
            'sold_past_year': sold_past_year,
            'sold_past_month': sold_past_month,
        })
    except Exception as e:
        print(f"Failed to calculate sales volume: {e}")
        update_flags(listing_id, "Failed to calculate sales volume.")

    # Calculate median and average house prices for each of the past 5 years
    try:
        for i in range(5):
            start_date = current_date - DateOffset(years=i + 1)
            end_date = current_date - DateOffset(years=i)
            yearly_properties = all_properties[
                (all_properties['last_sold_date'] >= start_date) & (all_properties['last_sold_date'] < end_date)
            ]

            metrics.update({
                f'average_price_{end_date.year}': yearly_properties['list_price'].mean(),
                f'median_price_{end_date.year}': yearly_properties['list_price'].median(),
            })
    except Exception as e:
        print(f"Failed to calculate yearly house prices: {e}")
        update_flags(listing_id, "Failed to calculate yearly house prices.")

    # Get recent sold properties
    try:
        sold_properties = all_properties.dropna(subset=['last_sold_date']).sort_values(by='last_sold_date')
        recent_sold_properties = sold_properties.tail(10)

        recent_sold_properties_list = []
        for _, property in recent_sold_properties.iterrows():
            property_dict = property.to_dict()
            # Convert Timestamp objects to string in ISO 8601 format
            if isinstance(property_dict.get('last_sold_date'), pd.Timestamp):
                property_dict['last_sold_date'] = property_dict['last_sold_date'].isoformat()
            recent_sold_properties_list.append(property_dict)

        # Add recent sold properties to the metrics dictionary
        metrics['recent_sold_properties'] = recent_sold_properties_list
    except Exception as e:
        print(f"Failed to process recent sold properties: {e}")
        update_flags(listing_id, "Failed to process recent sold properties.")
    
    for key, value in metrics.item():
        if isinstance(value,np.int64):
            metrics[key] = int(value)
        elif isinstance(value,np.float64):
            metrics[key] = float(value)

    # Update Supabase with metrics
    try:
        supabase.table('reports').update({
            'market_trends': json.dumps(metrics)
        }).eq('listing_id', listing_id).execute()
    except Exception as e:
        print(f"Failed to update Supabase Market Trends: {e}")
        update_flags(listing_id, "Failed to update Supabase Market Trends.")

    return metrics

def get_rent_insights(address, sqft, listing_id, estimated_value,listing_type="for_rent", past_days=300):
    """
    Get insights on the best rent for a particular property based on the rent to square footage ratio.

    Args:
        address (str): The address of the property (city, state, zip).
        listing_id (int): The listing ID to update in the database.
        listing_type (str): The type of listings to search (for_sale, for_rent, pending).
        past_days (int): How many past days to include in the search.
        type (int): Indicator to use lot_sqft or sqft for rent calculation.

    Returns:
        dict: Dictionary with property details and rent per square foot.
    """

    # Initialize radius and settings for fetching properties
    radius = 0.5
    max_radius = 10
    radius_step = 0.5
    comparable_properties_list = []
    minimum_comps = 3
    
    while radius <= max_radius:
        print(f"Fetching properties within {radius} miles...")
        try:
            properties = scrape_property(
                location=address,
                radius=radius,
                listing_type=listing_type,
                past_days=past_days
            )

            # Check if the response is valid and not empty
            if properties is None or properties.empty:
                print(f"No valid properties found in the response at radius {radius}.")
                radius += radius_step
                continue

        except Exception as e:
            print(f"Error fetching properties: {e}")
            update_flags(listing_id, "Error fetching properties.")
            return {"message": "Error fetching properties."}

        # Check if 'list_price' exists in the DataFrame
        if 'list_price' not in properties.columns:
            print("Warning: 'list_price' column not found in the dataset.")
            return {"message": "Error: 'list_price' column not found in the dataset."}

        # Step 2: Filter properties with valid rent (list_price), sqft, assessed/estimated price
        try:
            print("Started to filtered")
            filtered_properties = properties[
                (properties['list_price'].notna()) &
                (properties['sqft'].notna()) &
                ((properties['assessed_value'].notna()) | (properties['estimated_value'].notna()))
            ].copy()
            print("filtered successfully")
            # Filter properties within 5% of the target square footage and price
            print("starting calculations")
            sqft_lower_bound = sqft * 0.95
            sqft_upper_bound = sqft * 1.05
            price_lower_bound = estimated_value * 0.95
            price_upper_bound = estimated_value * 1.05
            print("Calculations successful")
            
            print("starting filtering")
            filtered_properties = filtered_properties[
                (filtered_properties['sqft'] >= sqft_lower_bound) & 
                (filtered_properties['sqft'] <= sqft_upper_bound) & 
                ((filtered_properties['assessed_value'].between(price_lower_bound, price_upper_bound)) |
                 (filtered_properties['estimated_value'].between(price_lower_bound, price_upper_bound)))
            ]
            print("finished filtering")

            # Check if we have enough comparable properties for CMA
            if len(filtered_properties) >= minimum_comps:
                break  # Exit the loop when enough comps are found
            else:
                radius += radius_step
            

        except Exception as e:
            print(f"Error filtering properties: {e}")
            update_flags(listing_id, "Error filtering properties.")
            return {"message": "Error filtering properties."}

    # Step 4: If no comparable properties are found within the max radius, loosen the style and sqft restrictions
    if len(filtered_properties) < minimum_comps and radius > max_radius:
        print("Expanding search to include all property styles and relaxing square footage and price restrictions.")
        radius = 0.5
        sqft_lower_bound = sqft * 0.90
        sqft_upper_bound = sqft * 1.10
        price_lower_bound = estimated_value * 0.90
        price_upper_bound = estimated_value * 1.10

        while radius <= max_radius:
            print(f"Fetching properties within {radius} miles with relaxed criteria...")
            try:
                properties = scrape_property(
                    location=address,
                    radius=radius,
                    listing_type=listing_type,
                    past_days=past_days
                )

                if properties is None or properties.empty:
                    radius += radius_step
                    continue

                # Filter properties within relaxed sqft and price criteria
                filtered_properties = properties[
                    (properties['list_price'].notna()) &
                    (properties['sqft'].notna()) &
                    ((properties['assessed_value'].notna()) | (properties['estimated_value'].notna()))
                ].copy()

                filtered_properties = filtered_properties[
                    (filtered_properties['sqft'] >= sqft_lower_bound) & 
                    (filtered_properties['sqft'] <= sqft_upper_bound) & 
                    ((filtered_properties['assessed_value'].between(price_lower_bound, price_upper_bound)) |
                     (filtered_properties['estimated_value'].between(price_lower_bound, price_upper_bound)))
                ]

                if len(filtered_properties) >= minimum_comps:
                    break
                else:
                    radius += radius_step

            except Exception as e:
                print(f"Error fetching properties with relaxed criteria: {e}")
                return {"message": "Error fetching properties with relaxed criteria."}

    # Step 5: Calculate rent per square foot and remove outliers using IQR
    try:
        filtered_properties['rent_per_sqft'] = filtered_properties['list_price'] / filtered_properties['sqft']
        q1 = np.percentile(filtered_properties['rent_per_sqft'], 25)
        q3 = np.percentile(filtered_properties['rent_per_sqft'], 75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        filtered_properties = filtered_properties[
            (filtered_properties['rent_per_sqft'] >= lower_bound) & 
            (filtered_properties['rent_per_sqft'] <= upper_bound)
        ]
    except Exception as e:
        print(f"Error calculating rent insights: {e}")
        update_flags(listing_id, "Error calculating rent insights.")
        return {"message": "Error calculating rent insights."}

    try:
        # Calculate average rent per square foot and estimated rent based on input_sqft
        avg_rent_per_sqft = filtered_properties['rent_per_sqft'].mean()
        estimated_rent_cma = avg_rent_per_sqft * sqft

        # Calculate rent range using Property Value Percentage approach
        rent_low_percent = estimated_value * 0.008
        rent_high_percent = estimated_value * 0.011

        # Calculate GRM for comparable properties
        grm_list = []
        comparable_properties_list = []
        
        for _, row in filtered_properties.iterrows():
            house_price = row['assessed_value'] if pd.notna(row['assessed_value']) else row['estimated_value']
            annual_rent = row['list_price'] * 12
            grm = house_price / annual_rent
            grm_list.append(grm)
            comparable_properties_list.append({
                'sqft': row['sqft'],
                'list_price': row['list_price'],
                'house_price': house_price,
                'annual_rent': annual_rent
            })
        estimated_grm = np.mean(grm_list)
        estimated_annual_rent_grm = estimated_value / estimated_grm
        estimated_monthly_rent_grm = estimated_annual_rent_grm / 12

        # Prepare rent_cash_flow dictionary
        rent_cash_flow = {
            
            'rent_per_sqft': avg_rent_per_sqft,
            'CMA_approach': {
                'estimated_rent': estimated_rent_cma,
                'comparable_properties': comparable_properties_list
            },
            'value_percentage_approach': {
                'rent_low': rent_low_percent,
                'rent_high': rent_high_percent
            },
            'grm_approach': {
                'estimated_grm': estimated_grm,
                'estimated_monthly_rent': estimated_monthly_rent_grm
            }
        }

    except Exception as e:
        print(f"Error creating rent cash flow dictionary: {e}")
        update_flags(listing_id, "Error creating rent cash flow dictionary.")
        return {"message": "Error creating rent cash flow dictionary."}

    try:
        # Update rent_cash_flow in Supabase
        supabase.table('reports').update({
            'rent_cash_flow': json.dumps(rent_cash_flow)
        }).eq('listing_id', listing_id).execute()
        print("Rent cash flow successfully uploaded.")
    except Exception as e:
        print(f"Error uploading rent cash flow to Supabase: {e}")
        update_flags(listing_id, "Error uploading rent cash flow to database.")
        return {"message": "Error uploading rent cash flow to database."}

    return rent_cash_flow


def fetch_city_census_data(city_name, listing_id):
    table_info = [
        {"Table ID":"B25001","Title":"Housing Units","Description":"This table provides the total number of housing units in the area."},
        {"Table ID":"B25002","Title":"Occupancy Status","Description":"This table shows whether the housing units are occupied or vacant."},
        {"Table ID":"B25003","Title":"Tenure","Description":"This table indicates whether the occupied housing units are owner-occupied or renter-occupied."},
        {"Table ID":"B19001","Title":"Household Income in the Past 12 Months (In 2022 Inflation-adjusted Dollars)","Description":"This table provides a distribution of households by income range."},
        {"Table ID":"B19013","Title":"Median Household Income in the Past 12 Months (In 2022 Inflation-adjusted Dollars)","Description":"This table shows the median household income for the area."},
        {"Table ID":"B23025","Title":"Employment Status for the Population 16 Years and Over","Description":"This table provides data on the employment status of the population aged 16 years and over."},
        {"Table ID":"B01001","Title":"Sex by Age","Description":"This table provides a breakdown of the population by sex and various age groups."},
        {"Table ID":"B02001","Title":"Race","Description":"This table provides the population distribution by race, including categories like White, Black or African American, Asian, Native American, etc."},
        {"Table ID":"B03002","Title":"Hispanic or Latino Origin by Race","Description":"This table provides data on the Hispanic or Latino population and breaks it down by race."}
    ]

    # Convert table_info list to a dictionary for easy lookup
    table_info_dict = {item['Table ID']: {'Title': item['Title'], 'Description': item['Description']} for item in table_info}

    def fetch_geo_data(city_name):
        city_name = f'{city_name}, ca'
        response = requests.get("https://api.censusreporter.org/1.0/geo/show/latest?geo_ids=160%7C04000US06")
        if response.status_code == 200:
            data = response.json()
            geo_info = []
            first_object = data[0] if isinstance(data, list) and data else data

            for feat in first_object['features']:
                if feat['properties']['name'].lower() == city_name.lower():
                    geo_info.append({
                        'name': feat['properties']['name'],
                        'geoid': feat['properties']['geoid']
                    })
            if not geo_info:
                return f"City '{city_name}' not found in the data."
            return geo_info
        else:
            raise Exception(f"Error fetching geo data: {response.status_code} - {response.text}")
    
    def fetch_census_data(table_ids, geo_id):
        url = f"https://api.censusreporter.org/1.0/data/show/latest?table_ids={table_ids}&geo_ids={geo_id}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error fetching census data: {response.status_code} - {response.text}")

    # Fetch geographic data for the given city
    geo_data = fetch_geo_data(city_name)

    # If no geographic data found, return the message
    if isinstance(geo_data, str):
        update_flags(listing_id, f"City '{city_name}' not found in the data.")
        return geo_data

    geo_entry = geo_data[0]
    geo_id = geo_entry['geoid']

    table_ids = "B25001,B25002,B25003,B19001,B19013,B23025,B01001,B02001,B03002"
    census_data = fetch_census_data(table_ids, geo_id)

    # Dictionary to hold the structured data
    structured_data = {}

    # Iterate over the tables and data
    for table_id, table_content in census_data['data'][geo_id].items():
        table_columns = census_data['tables'][table_id]['columns']
        # Retrieve the metadata for this table
        table_info = table_info_dict.get(table_id, {})
        table_title = table_info.get('Title', 'Unknown Title')
        table_description = table_info.get('Description', 'No description available')

        # Initialize the structure for this table if not already initialized
        if table_id not in structured_data:
            structured_data[table_id] = {
                "Table Title": table_title,
                "Table Description": table_description,
                "Columns": []
            }

        # Append the column details to the "Columns" list
        for column_id, estimate in table_content['estimate'].items():
            description = table_columns[column_id]['name']
            error = table_content['error'].get(column_id, None)
            structured_data[table_id]["Columns"].append({
                "Column ID": column_id,
                "Description": description,
                "Estimate": estimate,
                "Error": error,
                "Geoid": geo_id,
                "Name": census_data['geography'][geo_id]['name']
            })

    print(f"Appended census data: {geo_entry['name']}")

    supabase.table('reports').update({
        'census_data': json.dumps(structured_data)
    }).eq('listing_id', listing_id).execute()
    # Return the structured data
    return structured_data

def update_status(listing_id, status, client_id):
    supabase.table('reports').update({
        'status': status
        }).eq('listing_id', listing_id).execute()
    
    url = "https://housesnapai.vercel.app/api/report/event"
    payload = {
        "clientId": client_id,
        "message": status
    }
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)
    return response.status_code, response.text




def update_flags(listing_id, flag):
    try:
        response = supabase.table('reports').select('flags').eq('listing_id', listing_id).single().execute()
        flags = response.data['flags'] if response.data else []
        if flags != None:
            flags = json.loads(flags)
            flags.append(flag)
        else:
            flags = [flag]
        supabase.table('reports').update({
            'flags': json.dumps(flags)
        }).eq('listing_id', listing_id).execute()
    except Exception as e:
        print(f"Error updating flags: {e}")

# ... rest of the code ...

def handler(event, context):
    for record in event['Records']:
        print("RUNNING CODE!!")
        body = json.loads(record.get('body', ''))
        
        print(body)
            
            
       
        client_id = body['client_id']
        listing = body['listing']
        listing_id = listing['id']
        
        try:
           
            county = listing['county']
            city = listing['city']
            street_line = listing['street']
            state = listing['state']
            zipcode = listing['zip_code']
            lat = listing['latitude']
            long = listing['longitude']
            sqft = float(listing['sqft'])
            lot_sqft = listing['lot_sqft']
            value_estimate = float(listing['assessed_value'])
            address = f'{street_line},{city},{state} {zipcode}'

            try:
                update_status(listing_id, "started", client_id)
            except Exception as e:
                print(f"Error updating status to 'started': {e}")
                update_flags(listing_id, "Error updating status to 'started'.")
            
           
              
           

            # CRIME SCORE
            try:
                crime_score, data_to_process = calculate_crime_score(county, city, listing_id)
                print(f"Crime score: {crime_score}")
                print(f"Data to process: {data_to_process}")
                update_status(listing_id, "crime_done", client_id)
            except Exception as e:
                print(f"Error calculating crime score or updating status: {e}")
                update_flags(listing_id, "Error calculating crime score or updating status.")

            # TRENDS DATA
            try:
                trends = scrape_address_data(address, listing_id)
                if trends:
                    print(f"Successfully uploaded trend data for {city}. Here is the data:\n{trends}")
                else:
                    print(f"Failed to upload trend data for {city}")
                update_status(listing_id, "trends_done", client_id)
            except Exception as e:
                print(f"Error scraping trends data or updating status: {e}")
                update_flags(listing_id, "Error scraping trends data or updating status.")

            # SCHOOL SCORE
            try:
                scrape_schooldigger(street_line, city, state, zipcode, lat, long, listing_id)
                update_status(listing_id, "scraping_done", client_id)
                print("scraping school data and updating done")
            except Exception as e:
                print(f"Error scraping school data or updating status: {e}")
                update_flags(listing_id, "Error scraping school data or updating status.")
            
            try:
                print("scraping home details")
                scrape_home_details(f'{street_line},{city}', listing_id)
                update_status(listing_id, "home_details_done", client_id)
            except Exception as e:
                print(f"Error in scrape_home_details: {str(e)}")
                update_flags(listing_id, "Error in scrape_home_details.")
                raise

            # CENSUS DATA
            try:
                census_data = fetch_city_census_data(city, listing_id)
                if census_data:
                    print(f"Successfully uploaded census data for {city}. Here is the data: {census_data}")
                else:
                    print(f"Failed to upload census data for {city}")
                update_status(listing_id, "census_done", client_id)
            except Exception as e:
                print(f"Error fetching census data or updating status: {e}")
                update_flags(listing_id, "Error fetching census data or updating status.")

            # RENT CASH FLOW
            try:
                
                rent_cash_flow = get_rent_insights(address, sqft,listing_id,value_estimate, listing_type="for_rent", past_days=300)
                
                if rent_cash_flow:
                    print(f"Successfully uploaded rent cash flow data for {city}. Here is the data: {rent_cash_flow}")
                else:
                    print(f"Failed to upload rent cash flow data for {city}")
                update_status(listing_id, "cash_flow_done", client_id)
            except Exception as e:
                print(f"Error fetching rent cash flow data or updating status: {e}")
                update_flags(listing_id, "Error fetching rent cash flow data or updating status.")

            # Mark as complete
            try:
                update_status(listing_id, "complete", client_id)
            except Exception as e:
                print(f"Error updating status to 'complete': {e}")
                update_flags(listing_id, "Error updating status to 'complete'.")

        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            update_flags(listing_id, "JSON decode error.")
        except Exception as e:
            print(f"General error processing record: {e}")
            update_flags(listing_id, f"General error processing record: ")
        
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }