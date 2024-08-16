import json
import os
from supabase import create_client, Client

# Initialize Supabase client
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)


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


def handler(event, context):
    for record in event['Records']:
        body = record.get('body', '')
        report_id = body['report_id']
        county = body['county']
        city = body['city']

        # CRIME SCORE
        crime_score, data_to_process = calculate_crime_score(county, city, report_id)


        # Process the body payload
        print(f"Processing message: {body}")
        
        # Example of using Supabase client
        data = supabase.table('your_table').select('*').execute()
        print(f"Supabase data: {data}")

    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }