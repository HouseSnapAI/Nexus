import json
import os
import subprocess
import sys
import time
from supabase import create_client, Client
from playwright.sync_api import sync_playwright
from homeharvest import scrape_property
from datetime import datetime
import pandas as pd
from pandas.tseries.offsets import DateOffset
from datetime import datetime
import requests


args=['--no-sandbox', '--disable-setuid-sandbox','--disable-gpu','--single-process']


# Initialize Supabase client
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

args=['--no-sandbox', '--disable-setuid-sandbox','--single-process','--disable-gpu']



def install_dependencies():
    subprocess.check_call(["playwright", "install"])
    

def calculate_crime_score(county: str, city: str, report_id: str):
    try:
        county = county.replace(" ", "_").lower()
        city = city.replace(" ", "_").lower()
    except Exception as e:
        print(f"Error processing county or city name: {e}")
        update_flags(report_id, "Error processing county or city name.")
        return {"message": "Error processing county or city name."}

    try:
        # Example query to fetch data from a table
        response = supabase.table('crime_data_ca').select(
            'all_violent_crime_trend, agency_name, crime_location, victim_age, victim_ethnicity, victim_race, victim_age, id'
        ).or_(
            f"agency_name.ilike.%{city}%,"
            f"agency_name.ilike.%{county}%,"
        ).execute()
    except Exception as e:
        print(f"Error fetching crime_score data from Supabase: {e}")
        update_flags(report_id, "Error fetching crime_score data from Supabase.")
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
        update_flags(report_id, "Error processing fetched data.")
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
        update_flags(report_id, "Error determining data to process.")
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
                    result = data_to_process[res]['all_violent_crime_trend'][1][f'{i}'] / data_to_process[res]['all_violent_crime_trend'][0][f'{i}']
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
        update_flags(report_id, "Error calculating scores.")
        return {"message": "Error calculating scores."}

    try:
        # If there are multiple scores (e.g., city has both police and sheriff's departments), calculate the average score
        if len(scores) > 1:
            crime_score = sum(scores) / len(scores)
        else:
            crime_score = scores[0] if scores else 0
    except Exception as e:
        print(f"Error calculating crime score: {e}")
        update_flags(report_id, "Error calculating crime score.")
        return {"message": "Error calculating crime score."}

    try:
        # Update the reports table with crime_data_ids and crime_score
        update_data = {
            'crime_data_ids': crime_data_ids,
            'crime_score': crime_score
        }

        response = supabase.table('reports').update(update_data).eq('id', report_id).execute()
        print(f"Updated report {report_id} with crime_data_ids: {crime_data_ids}")
    except Exception as e:
        print(f"Error updating report {report_id}: {str(e)}")
        update_flags(report_id, "Error updating report.")
        return {"message": f"Error updating report {report_id}."}

    return crime_score, data_to_process

def scrape_home_details(page, address, report_id):
    page.goto("https://www.homes.com/")
    xpath_search_box = "//input[contains(@class, 'multiselect-search')]"

    page.locator(xpath_search_box).click()
    page.locator(xpath_search_box).type(address, delay=200)
    page.wait_for_load_state("domcontentloaded")
    page.locator(xpath_search_box).press("Enter")
    page.wait_for_load_state("domcontentloaded")

    time.sleep(2)

    home_details = {
        "price": page.query_selector("#price").inner_text(),
        "views": page.query_selector(".total-views").inner_text(),
        "highlights": [
            highlight.query_selector(".highlight-value").inner_text().strip()
            for highlight in page.query_selector_all("#highlights-section .highlight")
        ],
        "home_details": [
            {
                "label": subcategory.query_selector(".amenity-name").inner_text().strip(),
                "details": [detail.inner_text().strip() for detail in subcategory.query_selector_all(".amenities-detail")]
            }
            for subcategory in page.query_selector_all("#amenities-container .subcategory")
        ],
        "neighborhood_kpis": [
            {
                "title": kpi.query_selector(".neighborhood-kpi-card-title").inner_text(),
                "text": kpi.query_selector(".neighborhood-kpi-card-text").inner_text()
            }
            for kpi in page.query_selector_all(".neighborhood-kpi-card")
        ],
        "tax_history": [
            {
                "year": row.query_selector(".tax-year").inner_text().strip(),
                "tax_paid": row.query_selector(".tax-amount").inner_text().strip(),
                "tax_assessment": row.query_selector(".tax-assessment").inner_text().strip(),
                "land": row.query_selector(".tax-land").inner_text().strip(),
                "improvement": row.query_selector(".tax-improvement").inner_text().strip()
            }
            for row in page.query_selector_all("#tax-history-container .tax-table .tax-table-body .tax-table-body-row")
        ],
        "price_history": [
            {
                "date": row.query_selector(".price-year .long-date").inner_text().strip(),
                "event": row.query_selector(".price-event").inner_text().strip(),
                "price": row.query_selector(".price-price").inner_text().strip(),
                "change": row.query_selector(".price-change").inner_text().strip(),
                "sq_ft_price": row.query_selector(".price-sq-ft").inner_text().strip()
            }
            for row in page.query_selector_all("#price-history-container .price-table .table-body-row")
        ],
        "deed_history": [
            {
                "date": row.query_selector(".deed-date .shorter-date").inner_text().strip(),
                "type": row.query_selector(".deed-type").inner_text().strip(),
                "sale_price": row.query_selector(".deed-sale-price").inner_text().strip(),
                "title_company": row.query_selector(".deed-title-company").inner_text().strip()
            }
            for row in page.query_selector_all("#deed-history-container .deed-table .deed-table-body-row")
        ],
        "mortgage_history": [
            {
                "date": row.query_selector(".mortgage-date .shorter-date").inner_text().strip(),
                "status": row.query_selector(".mortgage-status").inner_text().strip(),
                "loan_amount": row.query_selector(".mortgage-amount").inner_text().strip(),
                "loan_type": row.query_selector(".mortgage-type").inner_text().strip()
            }
            for row in page.query_selector_all("#mortgage-history-container .mortgage-table .table-body-row")
        ],
        "transportation": [
            {
                "type": item.query_selector(".transportation-type").inner_text().strip(),
                "name": item.query_selector(".transportation-name").inner_text().strip(),
                "distance": item.query_selector(".transportation-distance").inner_text().strip()
            }
            for item in page.query_selector_all("#transportation-container .transportation-item")
        ],
        "bike_score": {
            "tagline": page.query_selector("#score-card-container .bike-score .score-card-tagline").inner_text().strip(),
            "score": page.query_selector("#score-card-container .bike-score .score-scoretext").inner_text().strip()
        },
        "walk_score": {
            "tagline": page.query_selector("#score-card-container .walk-score .score-card-tagline").inner_text().strip(),
            "score": page.query_selector("#score-card-container .walk-score .score-scoretext").inner_text().strip()
        },

    }
    
    try:
        supabase.table('reports').update({
            'home_details': json.dumps(home_details)
        }).eq('id', report_id).execute()
    except Exception as e:
        print(f"Failed to update Supabase: {e}")
        update_flags(report_id, "Failed to update Supabase.")
    



def scrape_schooldigger(street_line, city, state, zipcode, lat, long, report_id):
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    url = f"https://www.schooldigger.com/go/CA/search.aspx?searchtype=11&address={street_line.replace(' ', '+')}&city={city.replace(' ', '+')}&state={state}&zip={zipcode}&lat={lat}&long={long}"

    with sync_playwright() as p:

        print("Launching browser...")
        browser = p.chromium.launch(headless=True, args=args, timeout=120000)  # Increase timeout to 60 seconds
        print("Browser launched successfully.")
           
        page = browser.new_page(user_agent=ua)
        page.set_extra_http_headers({
            "sec-ch-ua": '"Chromium";v="125", "Not.A/Brand";v="24"'
        })
        
        try:
            
            print(f"Navigating to URL: {url}")  # Debugging statement
            page.goto(url, timeout=120000)  # Increase timeout to 120 seconds
            page.wait_for_load_state("domcontentloaded")  # Wait for DOM content to load
            print("Page loaded.")  # Debugging statement

            # Wait for the table tab to be clickable and click it
            page.wait_for_selector("xpath=/html/body/form/div[5]/div[5]/ul/li[4]/a")
            page.click("xpath=/html/body/form/div[5]/div[5]/ul/li[4]")
            print("Clicked on the table tab.")  # Debugging statement

            # Wait for the all button to be clickable and click it
            page.wait_for_selector("xpath=/html/body/form/div[5]/div[6]/div[3]/div[1]/a[8]")
            page.click("xpath=/html/body/form/div[5]/div[6]/div[3]/div[1]/a[8]")
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

            calculate_school_data(school_data,report_id)

           
        except Exception as e:
            print(f"Error in scrape_schooldigger: {str(e)}")
            update_flags(report_id, "Error in scrape_schooldigger.")
            raise
        
        
        try:
            scrape_home_details(f'{street_line},{city},{state}',report_id)
        except Exception as e:
            print(f"Error in scrape_home_details: {str(e)}")
            update_flags(report_id, "Error in scrape_home_details.")
            raise
            
            

def calculate_school_data(school_data, report_id):

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

    # Sort schools by score in descending order
    top_schools = sorted(school_data, key=lambda x: x["Score"], reverse=True)[:3]

    # Calculate the average score of the top 3 schools
    average_top_3_score = sum(school["Score"] for school in top_schools) / 3

    # Update Supabase row in table 'reports' with id = report_id with 'school_score' and 'top_schools'
    supabase.table('reports').update({
        'school_score': average_top_3_score,
        'top_schools': json.dumps(closest_schools)
    }).eq('id', report_id).execute()

    


def scrape_address_data(address, report_id):
    past_days = 5 * 365  # 5 years worth of days
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
        update_flags(report_id, "Failed to scrape properties data.")

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
        }
    except Exception as e:
        print(f"Failed to calculate property metrics: {e}")
        update_flags(report_id, "Failed to calculate property metrics.")

    # Calculate sales volume
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
        update_flags(report_id, "Failed to calculate sales volume.")

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
        update_flags(report_id, "Failed to calculate yearly house prices.")

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
        update_flags(report_id, "Failed to process recent sold properties.")

    # Update Supabase with metrics
    try:
        supabase.table('reports').update({
            'market_trends': json.dumps(metrics)
        }).eq('id', report_id).execute()
    except Exception as e:
        print(f"Failed to update Supabase: {e}")
        update_flags(report_id, "Failed to update Supabase.")

    return metrics

def get_rent_insights(address, sqft, report_id ,listing_type="for_rent", past_days=300, type = 1):
    """
    Get insights on the best rent for a particular property based on the rent to square footage ratio.
    
    Args:
        address (str): The address of the property (city, state, zip).
        listing_type (str): The type of listings to search (for_sale, for_rent, pending).
        past_days (int): How many past days to include in the search.
    
    Returns:
        dict: Dictionary with property details and rent per square foot.
    """
    try:
        # Fetch properties based on the address
        properties = scrape_property(
            location=address,
            radius=10,
            listing_type=listing_type,
            past_days=past_days,
        )
    except Exception as e:
        print(f"Error fetching properties: {e}")
        update_flags(report_id, "Error fetching properties.")
        return {"message": "Error fetching properties."}

    if properties.empty:
        return {"message": "No properties found for the given address."}

    try:
        # Filter out properties with missing 'list_price' or 'sqft'
        filtered_properties = properties[(properties['list_price'].notna()) & (properties['sqft'].notna()) & (properties['lot_sqft'].notna())]
        print(f"Number of filtered properties: {len(filtered_properties)}")
    except Exception as e:
        print(f"Error filtering properties: {e}")
        update_flags(report_id, "Error filtering properties.")
        return {"message": "Error filtering properties."}

    try:
        # Calculate average rent and rent per sqft
        properties_rent = filtered_properties['list_price'].mean()
        properties_sqft_rent = filtered_properties['list_price'] / filtered_properties['sqft']
        properties_sqft_rent_lot = filtered_properties['lot_sqft'] / filtered_properties['sqft']
    except Exception as e:
        print(f"Error calculating rent insights: {e}")
        update_flags(report_id, "Error calculating rent insights.")
        return {"message": "Error calculating rent insights."}

    try:
        if type == 1:
            estimated_rent = sqft * properties_sqft_rent.mean()
        else:
            estimated_rent = sqft * properties_sqft_rent_lot.mean()
    except Exception as e:
        print(f"Error calculating estimated rent: {e}")
        update_flags(report_id, "Error calculating estimated rent.")
        return {"message": "Error calculating estimated rent."}

    try:
        rent_cash_flow = {
            'estimated_rent': estimated_rent,
            'rent_per_sqft': properties_sqft_rent.mean(),
            'rent_per_lot_sqft': properties_sqft_rent_lot.mean(),
            'basis_number': len(filtered_properties)
        }
    except Exception as e:
        print(f"Error creating rent cash flow dictionary: {e}")
        update_flags(report_id, "Error creating rent cash flow dictionary.")
        return {"message": "Error creating rent cash flow dictionary."}

    try:
        # Update rent_cash_flow in Supabase
        supabase.table('reports').update({
            'rent_cash_flow': json.dumps(rent_cash_flow)
        }).eq('id', report_id).execute()
        print("Rent cash flow successfully uploaded.")
    except Exception as e:
        print(f"Error uploading rent cash flow to Supabase: {e}")
        update_flags(report_id, "Error uploading rent cash flow to database.")
        return {"message": "Error uploading rent cash flow to database."}

    return rent_cash_flow





def fetch_city_census_data(city_name, report_id):
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
        update_flags(report_id, f"City '{city_name}' not found in the data.")
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
    }).eq('id', report_id).execute()
    # Return the structured data
    return structured_data

def update_status(report_id, status, client_id):
    supabase.table('reports').update({
        'status': status
        }).eq('id', report_id).execute()
    
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




def update_flags(report_id, flag):
    try:
        response = supabase.table('reports').select('flags').eq('id', report_id).single().execute()
        flags = response.data['flags'] if response.data else []
        flags.append(flag)
        supabase.table('reports').update({
            'flags': json.dumps(flags)
        }).eq('id', report_id).execute()
    except Exception as e:
        print(f"Error updating flags: {e}")

# ... rest of the code ...

def handler(event, context):
    for record in event['Records']:
        try:
            print("RUNNING CODE!!")
            body = json.loads(record.get('body', ''))
            print(body)
            
            
            report_id = body['report_id']
            client_id = body['client_id']
            listing = body['listing']
            
            county = listing['county']
            city = listing['city']
            street_line = listing['street']
            state = listing['state']
            zipcode = listing['zip_code']
            lat = listing['latitude']
            long = listing['longitude']
            sqft = listing['sqft']
            lot_sqft = listing['lot_sqft']
            address = f'{street_line},{city},{state} {zipcode}'
            
            try:
                update_status(report_id, "started", client_id)
            except Exception as e:
                print(f"Error updating status to 'started': {e}")
                update_flags(report_id, "Error updating status to 'started'.")
            
           
              
           

            # CRIME SCORE
            try:
                crime_score, data_to_process = calculate_crime_score(county, city, report_id)
                print(f"Crime score: {crime_score}")
                print(f"Data to process: {data_to_process}")
                update_status(report_id, "crime_done", client_id)
            except Exception as e:
                print(f"Error calculating crime score or updating status: {e}")
                update_flags(report_id, "Error calculating crime score or updating status.")

            # TRENDS DATA
            try:
                trends = scrape_address_data(address, report_id)
                if trends:
                    print(f"Successfully uploaded trend data for {city}. Here is the data:\n{trends}")
                else:
                    print(f"Failed to upload trend data for {city}")
                update_status(report_id, "trends_done", client_id)
            except Exception as e:
                print(f"Error scraping trends data or updating status: {e}")
                update_flags(report_id, "Error scraping trends data or updating status.")

            # SCHOOL SCORE
            try:
                scrape_schooldigger(street_line, city, state, zipcode, lat, long, report_id)
                update_status(report_id, "scraping_done", client_id)
            except Exception as e:
                print(f"Error scraping school data or updating status: {e}")
                update_flags(report_id, "Error scraping school data or updating status.")

            # CENSUS DATA
            try:
                census_data = fetch_city_census_data(city, report_id)
                if census_data:
                    print(f"Successfully uploaded census data for {city}. Here is the data: {census_data}")
                else:
                    print(f"Failed to upload census data for {city}")
                update_status(report_id, "census_done", client_id)
            except Exception as e:
                print(f"Error fetching census data or updating status: {e}")
                update_flags(report_id, "Error fetching census data or updating status.")

            # RENT CASH FLOW
            try:
                if sqft == -1:
                    rent_cash_flow = get_rent_insights(address, lot_sqft, report_id, listing_type="for_rent", past_days=300, type=2)
                else:
                    rent_cash_flow = get_rent_insights(address, sqft, report_id, listing_type="for_rent", past_days=300, type=1)
                
                if rent_cash_flow:
                    print(f"Successfully uploaded rent cash flow data for {city}. Here is the data: {rent_cash_flow}")
                else:
                    print(f"Failed to upload rent cash flow data for {city}")
                update_status(report_id, "cash_flow_done", client_id)
            except Exception as e:
                print(f"Error fetching rent cash flow data or updating status: {e}")
                update_flags(report_id, "Error fetching rent cash flow data or updating status.")

            # Mark as complete
            try:
                update_status(report_id, "complete", client_id)
            except Exception as e:
                print(f"Error updating status to 'complete': {e}")
                update_flags(report_id, "Error updating status to 'complete'.")

        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            update_flags(report_id, "JSON decode error.")
        except Exception as e:
            print(f"General error processing record: {e}")
            update_flags(report_id, f"General error processing record: ")
        
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }



"""def get_latest_report():
    # Fetch the latest report by created time or id, depending on your table's schema
    try:
        response = supabase.table('reports').select('*').order('created_at', desc=True).limit(1).execute()
        if response.data:
            latest_report = response.data[0]
            print(f"Latest report retrieved: {latest_report}")
            return latest_report
        else:
            print("No reports found in the table.")
            return None
    except Exception as e:
        print(f"Failed to retrieve the latest report: {e}")
        return None"""

"""def test_handler():
    event = {
        {
        "Records": [
            {
            "body": {
                "report_id": "77e52f27-1e11-49c6-ad6d-ec248238ff31",
                "listing": {
                "id": "bc483c42-24b7-4138-853e-7329671fb1b8",
                "mls": "MRCA",
                "mls_id": "PW24130567",
                "status": "PENDING",
                "property_type": "TOWNHOMES",
                "full_street_line": "436 Orion Way",
                "street": "436 Orion Way",
                "city": "Newport Beach",
                "state": "CA",
                "unit": "-1",
                "zip_code": "92663",
                "list_price": 1175000,
                "beds": 3,
                "days_on_mls": 37,
                "full_baths": 2,
                "half_baths": 1,
                "sqft": 1440,
                "year_built": 1963,
                "list_date": "2024-06-26 00:00:00+00",
                "sold_price": 765000.0,
                "last_sold_date": "2018-11-29 00:00:00+00",
                "assessed_value": 804150.0,
                "estimated_value": 1150900.0,
                "lot_sqft": 1433,
                "price_per_sqft": 816.0,
                "latitude": 33.628274,
                "longitude": -117.930199,
                "county": "Orange",
                "fips_code": "6059",
                "stories": "2",
                "hoa_fee": 362.0,
                "parking_garage": "2.0"
                }
            }
            }
        ]
        }
        
        # need to put some test data into here
    }
    


    
    context = {}
    response = handler(event,context)
    print(f"Lambda handler response: {response}")
    get_latest_report()
"""