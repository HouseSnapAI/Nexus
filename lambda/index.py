import json
import os
import subprocess
import sys
from supabase import create_client, Client
from playwright.sync_api import sync_playwright


# Initialize Supabase client
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)


# Function to install dependencies
def install_dependencies():
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

    crime_data_ids = []
    for item in data_to_process:
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
        browser = p.chromium.launch(headless=False)  # Set headless=True for headless mode
        page = browser.new_page()
        print(f"Navigating to URL: {url}")  # Debugging statement
        page.goto(url, timeout=60000)  # Increase timeout to 60 seconds
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

def handler(event, context):
    for record in event['Records']:
        body = record.get('body', '')
        report_id = body['report_id']
        county = body['county']
        city = body['city']
        street_line = body['street_line']
        state = body['state']
        zipcode = body['zipcode']
        lat = body['latitude']
        long = body['longitude']

        install_dependencies()


        # CRIME SCORE
        crime_score, data_to_process = calculate_crime_score(county, city, report_id)
        school_score = scrape_schooldigger(street_line, city, state, zipcode, lat, long, report_id)


        # Process the body payload
        print(f"Processing message: {body}")
        
        # Example of using Supabase client
        data = supabase.table('your_table').select('*').execute()
        print(f"Supabase data: {data}")

    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }