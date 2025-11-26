import requests
from datetime import datetime, timedelta
import json
import time

def regenerate_aid_report(start_date, end_date, template_id, max_retries=3, retry_delay=30):
    url = "https://reporting-api.astrapay.com/report-service/regenerate"
    # url = "http://localhost:8001/report-service/regenerate"

    headers = {
        'Content-Type': 'application/json'
    }

    # Determine SFTP destination based on template ID
    if template_id == 19:  # Master User AID
    # if template_id == 12:  # Master User AID
        sftp_destination = "/astrapayapp/new_pipeline_ap/master_user_astrapay/"
    elif template_id == 26:  # Transaction Astrapay AID
    # elif template_id == 13:  # Transaction Astrapay AID
        sftp_destination = "/astrapayapp/new_pipeline_ap/transaction_astrapay/"
    else:
        raise ValueError("Invalid template ID. Must be 12 or 13.")

    payload = {
        "templateId": template_id,
        "groupType": "SINGLE",
        "fileType": "TXT",
        "startDate": start_date,
        "endDate": end_date,
        "delimiter": "CARET",
        ### FOR PRODUCTION PURPOSES
        "sftp": {
            "ipAddress": "cdp-sftp.astrafinancial.co.id",
            "port": 2225,
            "username": "astrapayapp",
            "password": "",
            "destination": sftp_destination,
            "privateKey": "/home/report-service-prd/.ssh/id_rsa"
        },
        ### FOR TESTING PURPOSES ONLY
        # "sftp": {
        #     "ipAddress": "10.42.41.41",
        #     "port": "7272",
        #     "username": "astrapay",
        #     "password": "Partner#AstraP4y",
        #     "destination": "/report-service"
        # },
        "user": {
            "id": 32729867,
            "name": "Report AstraPay to AID"
        },
        "type": "DATA_TRANSFER_AID"
    }
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Making request to: {url} (Attempt {attempt}/{max_retries})")
            print(f"Template ID: {template_id}")
            if attempt == 1:  # Only show payload on first attempt to reduce clutter
                print(f"Payload: {json.dumps(payload, indent=2)}")
            
            response = requests.post(url, headers=headers, json=payload, timeout=300)  # 5 minute timeout
            response.raise_for_status()
            
            print(f"Status Code: {response.status_code}")
            print(f"Response Body: {response.text}")
            return  # Success, exit the function
            
        except requests.Timeout:
            print(f"Request timed out (Attempt {attempt}/{max_retries})")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Max retries reached. Request failed due to timeout.")
                raise
                
        except requests.ConnectionError as e:
            print(f"🔌 Connection error (Attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Max retries reached. Request failed due to connection error.")
                raise
                
        except requests.HTTPError as e:
            status_code = e.response.status_code if e.response else 'Unknown'
            print(f"HTTP error {status_code} (Attempt {attempt}/{max_retries}): {e}")
            
            # Don't retry for client errors (4xx), but retry for server errors (5xx)
            if e.response and 400 <= e.response.status_code < 500:
                print("Client error (4xx) - not retrying.")
                raise
            elif attempt < max_retries:
                print(f"Server error (5xx) - retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Max retries reached. Request failed due to server error.")
                raise
                
        except requests.RequestException as e:
            print(f"Request error (Attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Max retries reached. Request failed.")
                raise

def get_date_input(prompt):
    while True:
        date_str = input(prompt)
        try:
            # Try to parse the date in the expected format
            datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            return date_str
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD HH:MM:SS format (e.g., 2025-11-16 00:00:00)")

def generate_date_ranges_for_months():
    """Generate daily date ranges for September and October 2025"""
    import calendar
    date_ranges = []
    
    # September 2025 (1-30)
    for day in range(1, 31):  # September has 30 days
        start_date = datetime(2025, 9, day)
        
        # Check if this is the last day of September
        if day == 30:  # Last day of September
            end_date = datetime(2025, 10, 1)  # First day of October
        else:
            end_date = start_date + timedelta(days=1)
        
        date_ranges.append((
            start_date.strftime("%Y-%m-%d 00:00:00"),
            end_date.strftime("%Y-%m-%d 00:00:00")
        ))
    
    # October 2025 (1-31)
    for day in range(1, 32):  # October has 31 days
        start_date = datetime(2025, 10, day)
        
        # Check if this is the last day of October
        if day == 31:  # Last day of October
            end_date = datetime(2025, 11, 1)  # First day of November
        else:
            end_date = start_date + timedelta(days=1)
        
        date_ranges.append((
            start_date.strftime("%Y-%m-%d 00:00:00"),
            end_date.strftime("%Y-%m-%d 00:00:00")
        ))
    
    return date_ranges

def run_batch_requests(start_from_request=1):
    """Run regenerate requests for all days in September and October 2025"""
    date_ranges = generate_date_ranges_for_months()
    template_ids = [19, 26]  # Two template IDs to process -> 19 (Master User AID), 26 (Transaction Astrapay AID)
    # template_ids = [12, 13]  # Two template IDs to process -> 12 (Master User AID), 13 (Transaction Astrapay AID)
    total_days = len(date_ranges)
    total_requests = total_days * len(template_ids)  # 2 requests per day
    
    if start_from_request > 1:
        print(f"RESUMING BATCH PROCESSING FROM REQUEST #{start_from_request}")
        print("=" * 60)
    
    print(f"Will process {total_days} days (September + October 2025)")
    print(f"Each day will have {len(template_ids)} requests (Template IDs: {', '.join(map(str, template_ids))})")
    print(f"Total requests: {total_requests}")
    if start_from_request > 1:
        remaining_requests = total_requests - start_from_request + 1
        print(f"Remaining requests: {remaining_requests} (from {start_from_request} to {total_requests})")
    print("=" * 60)
    
    confirm = input("Do you want to proceed with batch processing? (y/n): ").lower().strip()
    if confirm not in ['y', 'yes']:
        print("Operation cancelled.")
        return
    
    successful_requests = 0
    failed_requests = 0
    request_count = 0
    
    for day_num, (start_date, end_date) in enumerate(date_ranges, 1):
        for template_id in template_ids:
            request_count += 1
            
            # Skip requests until we reach the starting point
            if request_count < start_from_request:
                continue
                
            print(f"\nDAY {day_num}/{total_days}: {start_date} to {end_date}")
            print("=" * 50)
            print(f"[{request_count}/{total_requests}] Template ID: {template_id}")
            print("-" * 30)
            
            try:
                regenerate_aid_report(start_date, end_date, template_id)
                successful_requests += 1
                print("✅ Request completed successfully")
            except Exception as e:
                failed_requests += 1
                print(f"❌ Request failed: {e}")
            
            # Add a delay between requests to avoid overwhelming the server
            if request_count < total_requests:
                print(f"⏱Waiting 2 minutes before next request...")
                time.sleep(60)  # 1 minutes delay between requests
    
    print("\n" + "=" * 60)
    print("BATCH PROCESSING SUMMARY")
    print("=" * 60)
    print(f"Total days processed: {total_days}")
    print(f"Total requests: {total_requests}")
    if start_from_request > 1:
        print(f"Started from request: {start_from_request}")
    print(f"Successful: {successful_requests}")
    print(f"Failed: {failed_requests}")
    if successful_requests + failed_requests > 0:
        print(f"Success rate: {(successful_requests/(successful_requests + failed_requests))*100:.1f}%")
    print(f"Template IDs used: {', '.join(map(str, template_ids))}")

if __name__ == "__main__":
    print("AstraPay AID Report Generator")
    print("=" * 40)
    
    print("Available options:")
    print("1. Batch process September + October 2025 (daily intervals)")
    print("2. Single date range (custom)")
    print("3. Yesterday's date range")
    print("4. Continue batch from specific request number")
    
    choice = input("\nSelect option (1/2/3/4): ").strip()
    
    if choice == "1":
        run_batch_requests()
    elif choice == "2":
        template_id = int(input("Enter template ID (19/26): "))
        # template_id = int(input("Enter template ID (12/13): "))
        start_date = get_date_input("Enter start date (YYYY-MM-DD HH:MM:SS): ")
        end_date = get_date_input("Enter end date (YYYY-MM-DD HH:MM:SS): ")
        print(f"\nGenerating report for date range: {start_date} to {end_date}")
        regenerate_aid_report(start_date, end_date, template_id)
    elif choice == "3":
        template_id = int(input("Enter template ID (19/26): "))
        # template_id = int(input("Enter template ID (12/13): "))
        yesterday = datetime.now() - timedelta(days=1)
        start_date = yesterday.strftime("%Y-%m-%d 00:00:00")
        end_date = yesterday.strftime("%Y-%m-%d 23:59:59")
        print(f"\nGenerating report for date range: {start_date} to {end_date}")
        regenerate_aid_report(start_date, end_date, template_id)
    elif choice == "4":
        while True:
            try:
                last_success = int(input("Enter last successful batch number (e.g., 80): "))
                if last_success <= 0 or last_success >= 122:
                    print("Please enter a valid number between 1 and 121.")
                    continue
                break
            except ValueError:
                print("Please enter a valid number.")
        
        start_from = last_success + 1
        print(f"\nWill continue from request #{start_from}/122")
        run_batch_requests(start_from_request=start_from)
    else:
        print("Invalid choice. Exiting.")