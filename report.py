import boto3
from datetime import datetime, timedelta
import csv
import time
from collections import defaultdict

# Function to fetch daily cost data for a specific account and date range
def fetch_cost_for_account(aws_access_key, aws_secret_key, aws_session_token, region_name, start_date, end_date):
    session = boto3.Session(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        aws_session_token=aws_session_token,
        region_name=region_name
    )
    ce_client = session.client('ce')

    response = ce_client.get_cost_and_usage(
        TimePeriod={
            'Start': start_date,
            'End': end_date
        },
        Granularity='DAILY',
        Metrics=['UnblendedCost'],
        GroupBy=[
            {
                'Type': 'DIMENSION',
                'Key': 'SERVICE'
            }
        ]
    )
    
    return response['ResultsByTime']

# Function to fetch total cost per service for a specified month
def fetch_cost_for_month(aws_access_key, aws_secret_key, aws_session_token, region_name, start_date, end_date):
    session = boto3.Session(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        aws_session_token=aws_session_token,
        region_name=region_name
    )
    ce_client = session.client('ce')

    response = ce_client.get_cost_and_usage(
        TimePeriod={
            'Start': start_date,
            'End': end_date
        },
        Granularity='MONTHLY',
        Metrics=['UnblendedCost'],
        GroupBy=[
            {
                'Type': 'DIMENSION',
                'Key': 'SERVICE'
            }
        ]
    )

    monthly_costs = {}
    for group in response['ResultsByTime'][0]['Groups']:
        service = group['Keys'][0]
        amount = float(group['Metrics']['UnblendedCost']['Amount'])
        monthly_costs[service] = round(amount, 2)

    return monthly_costs

# Function to format amount in dollars
def format_dollar(amount):
    return f"${amount:.2f}"

# Function to write the cost data to a CSV file
def write_to_csv(account_name, previous_month_costs, current_month_costs, specified_date_costs, specified_date_total, specified_date_days):
    previous_month = (datetime.now() - timedelta(days=31)).strftime("%B")

    services = set(previous_month_costs.keys()) | set(current_month_costs.keys()) | set(specified_date_costs.keys())

    timestamp = time.strftime("%Y%m%d-%H%M%S")
    filename = f'{account_name}_cost_report_{timestamp}.csv'

    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        header = ['Service', f'Total Cost for {previous_month}', 'Total Cost Current Month', 'Total Cost for Specified Date'] + specified_date_days
        writer.writerow(header)

        for service in services:
            row = [service]
            previous_month_total = previous_month_costs.get(service, 0.00)
            current_month_total = current_month_costs.get(service, 0.00)
            specified_date_total_cost = sum(specified_date_costs[service].values()) if service in specified_date_costs else 0.00

            row.append(format_dollar(previous_month_total))
            row.append(format_dollar(current_month_total))
            row.append(format_dollar(specified_date_total_cost))

            for day in specified_date_days:
                daily_cost = specified_date_costs.get(service, {}).get(day, 0.00)
                row.append(format_dollar(daily_cost))

            writer.writerow(row)

        total_row = ['Total']
        total_previous_month = sum(previous_month_costs.values())
        total_current_month = sum(current_month_costs.values())
        # total_specified_date = specified_date_total
        total_row.append(format_dollar(total_previous_month))
        total_row.append(format_dollar(total_current_month))
        total_row.append(format_dollar(specified_date_total))

        for day in specified_date_days:
            daily_total = sum(specified_date_costs.get(service, {}).get(day, 0.00) for service in services)
            total_row.append(format_dollar(daily_total))

        writer.writerow(total_row)

    print(f"Combined cost report for {account_name} written to {filename}")

# Get dynamic date input for previous month and specified date range
def get_manual_date_range():
    while True:
        # Manual input for the previous month's start and end date
        print("Enter the date range for the previous month (e.g., 2023-10-01 to 2023-10-31):")
        previous_start_date = input("Previous Month Start Date (YYYY-MM-DD): ")
        previous_end_date = input("Previous Month End Date (YYYY-MM-DD): ")

        if not previous_start_date or not previous_end_date:
            print("Both start and end dates are required.")
            continue

        try:
            datetime.strptime(previous_start_date, '%Y-%m-%d')
            datetime.strptime(previous_end_date, '%Y-%m-%d')
        except ValueError:
            print("Incorrect date format. Please use YYYY-MM-DD.")
            continue

        # Manual input for specified date range
        print("\nEnter the specified date range (e.g., 2023-11-01 to 2023-11-10):")
        specified_start_date = input("Specified Start Date (YYYY-MM-DD): ")
        specified_end_date = input("Specified End Date (YYYY-MM-DD): ")

        if not specified_start_date or not specified_end_date:
            print("Both start and end dates are required.")
            continue

        try:
            datetime.strptime(specified_start_date, '%Y-%m-%d')
            datetime.strptime(specified_end_date, '%Y-%m-%d')
        except ValueError:
            print("Incorrect date format. Please use YYYY-MM-DD.")
            continue

        return previous_start_date, previous_end_date, specified_start_date, specified_end_date

# AWS credentials and regions for each account
accounts = [
    {
        "account_name": "Safe",
        "aws_access_key": "",
        "aws_secret_key": "",
        "aws_session_token": None,
        "region_name": "us-east-1"
    },
    {
        "account_name": "RedInk",
        "aws_access_key": "",
        "aws_secret_key": "",
        "aws_session_token": None,
        "region_name": "us-east-1"
    },
    {
        "account_name": "Antony",
        "aws_access_key": "",
        "aws_secret_key": "",
        "aws_session_token": None,
        "region_name": "us-east-1"
    }
    # Add more accounts if needed
]

# Fetching manual date range input from user
previous_start_date, previous_end_date, specified_start_date, specified_end_date = get_manual_date_range()

current_date = datetime.now()
# Get the current month start date
current_month_start = current_date.replace(day=1).strftime('%Y-%m-%d')
current_month_end = current_date.strftime('%Y-%m-%d')

# Calculate the specified date days range
specified_date_days = []
date_iterator = datetime.strptime(specified_start_date, '%Y-%m-%d')
end_date = datetime.strptime(specified_end_date, '%Y-%m-%d')

while date_iterator <= end_date:
    specified_date_days.append(date_iterator.strftime('%Y-%m-%d'))
    date_iterator += timedelta(days=1)

# Iterate over each account and fetch the daily cost data for the specified date range and previous month
for account in accounts:
    account_name = account['account_name']
    print(f"Fetching cost data for {account_name}...")

    # Fetch previous month cost data (ensuring the full range including 31st if available)
    previous_month_costs = fetch_cost_for_month(
        aws_access_key=account['aws_access_key'],
        aws_secret_key=account['aws_secret_key'],
        aws_session_token=account['aws_session_token'],
        region_name=account['region_name'],
        start_date=previous_start_date,
        end_date=previous_end_date
    )

    # Fetch current month cost data
    current_month_costs = fetch_cost_for_month(
        aws_access_key=account['aws_access_key'],
        aws_secret_key=account['aws_secret_key'],
        aws_session_token=account['aws_session_token'],
        region_name=account['region_name'],
        start_date=current_month_start,
        end_date=current_month_end
    )

    # Fetch specified date range cost data
    specified_date_cost_data = fetch_cost_for_account(
        aws_access_key=account['aws_access_key'],
        aws_secret_key=account['aws_secret_key'],
        aws_session_token=account['aws_session_token'],
        region_name=account['region_name'],
        start_date=specified_start_date,
        end_date=specified_end_date
    )

    specified_date_costs = defaultdict(lambda: defaultdict(float))
    for day in specified_date_cost_data:
        for group in day['Groups']:
            service = group['Keys'][0]
            amount = float(group['Metrics']['UnblendedCost']['Amount'])
            specified_date_costs[service][day['TimePeriod']['Start']] += round(amount, 2)

    specified_date_total = sum(sum(costs.values()) for costs in specified_date_costs.values())

    # Write results to CSV
    write_to_csv(account_name, previous_month_costs, current_month_costs, specified_date_costs, specified_date_total, specified_date_days)
