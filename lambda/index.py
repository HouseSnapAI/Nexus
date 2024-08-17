import json
import os
import subprocess
import sys
from supabase import create_client, Client
from playwright.sync_api import sync_playwright
from homeharvest import scrape_property
from datetime import datetime
import pandas as pd
from pandas.tseries.offsets import DateOffset
import datetime
import requests

# THESE NEED TO BE THESE VERSIONS
# realtime=1.0.6
# supabase=2.6.0

# Initialize Supabase client
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)


def install_dependencies():
    subprocess.check_call(["pip", "install", "playwright"])
    subprocess.check_call(["playwright", "install"])
    

def calculate_crime_score(county: str, city: str, report_id: str):

    # Example query to fetch data from a table
    response = supabase.table('crime_data_ca').select(
        'all_violent_crime_trend, agency_name, crime_location, victim_age, victim_ethnicity, victim_race, victim_age, id'
    ).or_(
        f"agency_name.ilike.%{county}_county_sheriff%,"
        f"agency_name.ilike.%{city}_police%,"
        f"agency_name.ilike.%{county}_police%,"
        f"agency_name.ilike.%{city}_sheriff%"
    ).execute()

    # Check if there's any city-level data
    city_data = [item for item in response.data if city.lower() in item['agency_name'].lower()]
    county_data = [item for item in response.data if county.lower() in item['agency_name'].lower() and city.lower() not in item['agency_name'].lower()]

    # Combine city_data and county_data
    combined_data = city_data + county_data

    crime_data_ids = []
    # Append crime_data_ids from combined_data
    for item in combined_data:
        crime_data_ids.append(item['id'])
    
    # Process city data if available, otherwise process county data
    if city_data:
        data_to_process = city_data
        print(f"Processing city-level data for {city}")
    else:
        data_to_process = county_data
        print(f"Processing county-level data for {county}")

    # Calculate the scores
    scores = []
    for res in range(len(data_to_process)):
        print(f"Processing item {res + 1} of {len(data_to_process)}")
        
        # Print the response data for the current item
        print(data_to_process[res])
        
        averages = []
        for i in range(2012, 2023):
            if i == 2021:
                continue
            # Calculate the result for the current year
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

    # If there are multiple scores (e.g., city has both police and sheriff's departments), calculate the average score
    if len(scores) > 1:
        crime_score = sum(scores) / len(scores)
    else:
        crime_score = scores[0] if scores else 0

    # Update the reports table with crime_data_ids and crime_score
    update_data = {
        'crime_data_ids': crime_data_ids,
        'crime_score': crime_score
    }

    try:
        response = supabase.table('reports').update(update_data).eq('id', report_id).execute()
        print(f"Updated report {report_id} with crime_data_ids: {crime_data_ids}")
    except Exception as e:
        print(f"Error updating report {report_id}: {str(e)}")

    return crime_score, data_to_process

def scrape_schooldigger(street_line, city, state, zipcode, lat, long, report_id):
    url = f"https://www.schooldigger.com/go/CA/search.aspx?searchtype=11&address={street_line.replace(' ', '+')}&city={city.replace(' ', '+')}&state={state}&zip={zipcode}&lat={lat}&long={long}"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # Set headless=True for headless mode
        page = browser.new_page()
        print(f"Navigating to URL: {url}")  # Debugging statement
        page.goto(url, timeout=120000)  # Increase timeout to 60 seconds
        page.wait_for_load_state("domcontentloaded")  # Wait for DOM content to load
        # Alternatively, wait for a specific element
        # page.wait_for_selector("selector_of_an_element_on_the_page")
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


        return calculate_school_data(school_data, report_id)

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
        'top_schools': closest_schools
    }).eq('id', report_id).execute()

    return average_top_3_score

def scrape_address_data(address, report_id):
    past_days = 5 * 365  # 5 years worth of days
    radius = 0.5  # 0.5 mile radius

    all_properties = pd.DataFrame()

    # Fetch properties for sale, sold, and pending
    for listing_type in ['for_sale', 'sold', 'pending']:
        properties = scrape_property(
            location=address,
            listing_type=listing_type,
            radius=radius,
            past_days=past_days,
            extra_property_data=True,
        )
        all_properties = pd.concat([all_properties, properties])

    

    current_year = datetime.datetime.now().year


    metrics = {
        'average_year_built': current_year-(current_year - all_properties['year_built']).mean(),
        'median_year_built': current_year-(current_year - all_properties['year_built']).median(),
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

    # Calculate sales volume
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

    # Calculate median and average house prices for each of the past 5 years
    for i in range(5):
        start_date = current_date - DateOffset(years=i+1)
        end_date = current_date - DateOffset(years=i)
        yearly_properties = all_properties[(all_properties['last_sold_date'] >= start_date) & (all_properties['last_sold_date'] < end_date)]
        
        metrics.update({
            f'average_price_{end_date.year}': yearly_properties['list_price'].mean(),
            f'median_price_{end_date.year}': yearly_properties['list_price'].median(),
        })

    # Sort properties by last_sold_date and get the 10 most recently sold
    sold_properties = all_properties.dropna(subset=['last_sold_date']).sort_values(by='last_sold_date')
    recent_sold_properties = sold_properties.tail(10)

    # Print details of the 10 most recently sold properties
    print("Details of the 10 most recently sold properties:")
    for index, property in recent_sold_properties.iterrows():
        print(property.to_dict())

    supabase.table('reports').update({
        'market_trends': json.dumps(metrics)
    }).eq('id', report_id).execute()

    return metrics

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
        return geo_data

    geo_entry = geo_data[0]
    geo_id = geo_entry['geoid']

    table_ids = "B25001,B25002,B25003,B19001,B19013,B23025,B01001,B02001,B03002"
    census_data = fetch_census_data(table_ids, geo_id)

    # Dictionary to hold the structured data
    census_data = {}

    # Iterate over the tables and data
    for table_id, table_content in census_data['data'][geo_id].items():
        table_columns = census_data['tables'][table_id]['columns']
        # Retrieve the metadata for this table
        table_info = table_info_dict.get(table_id, {})
        table_title = table_info.get('Title', 'Unknown Title')
        table_description = table_info.get('Description', 'No description available')

        # Initialize the structure for this table if not already initialized
        if table_id not in census_data:
            census_data[table_id] = {
                "Table Title": table_title,
                "Table Description": table_description,
                "Columns": []
            }

        # Append the column details to the "Columns" list
        for column_id, estimate in table_content['estimate'].items():
            description = table_columns[column_id]['name']
            error = table_content['error'].get(column_id, None)
            census_data[table_id]["Columns"].append({
                "Column ID": column_id,
                "Description": description,
                "Estimate": estimate,
                "Error": error,
                "Geoid": geo_id,
                "Name": census_data['geography'][geo_id]['name']
            })

    print(f"Appended census data: {geo_entry['name']}")

    supabase.table('reports').update({
        'census_data': json.dumps(census_data)
    }).eq('id', report_id).execute()
    # Return the structured data
    return census_data

def handler(event, context):
    for record in event['Records']:
        print("RUNNIN CODE!!")
        body = record.get('body', '')
        print(body)
        report_id = body['report_id']
        listing = body['listing']

        county = listing['county']
        city = listing['city']
        street_line = listing['street']
        state = listing['state']
        zipcode = listing['zip_code']
        lat = listing['latitude']
        long = listing['longitude']
        address = f'{street_line},{city},{state} {zipcode}'

        # install_dependencies()

        # CRIME SCORE
        crime_score, data_to_process = calculate_crime_score(county, city, report_id)
        print(f"Crime score: {crime_score}")
        print(f"Data to process: {data_to_process}")

        # SCHOOL SCORE
        school_score = scrape_schooldigger(street_line, city, state, zipcode, lat, long, report_id)
        print(f"School score: {school_score}")


        census_data = fetch_city_census_data(city,report_id)
        if census_data:
            print(f"Successfully uploaded census data for {city}. Here is the data:{census_data}")
        else:
            print(f"Failed to upload census data for {city}")

        trends = scrape_address_data(address,report_id)

        if trends:
            print(f"Successfully uploaded trend data for {city}. Here is the data:\n{trends}")
        else:
            print(f"Failed to upload trend data for {city}")
        # Process the body payload
        print(f"Processing message: {body}")
        
        # Example of using Supabase client
        data = supabase.table('your_table').select('*').execute()
        print(f"Supabase data: {data}")

    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }
