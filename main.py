from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession
from google.auth.transport import Response
from datetime import datetime, timezone, timedelta
from pathlib import Path
from argparse import ArgumentParser
import json


class ActivityFetcher:
    def __init__(self, credentials_file: str, subject: str, verbose: bool = False):
        self.verbose = verbose
        print(f"Using credentials file: {credentials_file}, subject: {subject}")
        self.credentials = Credentials.from_service_account_file(
            credentials_file,
            scopes=["https://www.googleapis.com/auth/admin.reports.audit.readonly"],
            subject=subject,
        )
        self.authed_session = AuthorizedSession(self.credentials)
        print("Successfully created AuthorizedSession")
        self.url = "https://admin.googleapis.com/admin/reports/v1/activity/users/all/applications/TOKEN"

    def fetch_activities(self, params: dict) -> Response:
        if self.verbose:
            print(f"Fetching activities with params: {params}")
        response = self.authed_session.get(self.url, params=params)
        if not response.ok:
            raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
        return response

    def get_activities(self, params: dict) -> list:
        activities = []
        response_json = self.fetch_activities(params).json()
        if "items" in response_json:
            if self.verbose:
                print(f"Last item timestamp: {response_json['items'][-1].get('id', {}).get('time')}")
            activities.extend(response_json["items"])
        # Recursively fetch next pages
        if response_json.get("nextPageToken"):
            if self.verbose:
                print(f"Found nextPageToken: {response_json.get('nextPageToken')}")
            params["pageToken"] = response_json.get("nextPageToken")
            activities.extend(self.get_activities(params))
        return activities

    def get_timestamped_activities(self, start: datetime, end: datetime) -> list:
        results = []
        params = {
            "startTime": start.isoformat(),
            "endTime": end.isoformat()
        }
        activities = self.get_activities(params)
        results.extend(activities)
        if self.verbose:
            print(f"Fetched {len(activities)} activities from {start} to {end}")
        return results

    def write_latest_timestamp(self, activities: list) -> None:
        with open("latest_timestamp.txt", "w") as f:
            # 0th index instead of -1 because activities are reverse chronological
            last_time = activities[0].get('id', {}).get('time')
            f.write(last_time)
            if self.verbose:
                print(f"Wrote latest timestamp: {last_time}")

    def read_latest_timestamp(self) -> datetime:
        if Path("latest_timestamp.txt").exists():
            with open("latest_timestamp.txt", "r") as f:
                timestamp_str = f.read()
                if self.verbose:
                    print(f"Latest timestamp from latest_timestamp.txt: {timestamp_str}")
                return datetime.fromisoformat(timestamp_str)

        elif Path("*48_hour_events.json").exists():
            # Probably an edge case for latest_timestamp.txt to be missing AND 48_hour_events.json to exist
            latest_event_file = next(Path(".").glob("*48_hour_events.json"), None)
            if latest_event_file is not None:
                with open(latest_event_file, "r") as f:
                    last_event_time = json.load(f)[0].get('id', {}).get('time')
                    if self.verbose:
                        print(f"Latest timestamp from {latest_event_file}: {last_event_time}")
                    return datetime.fromisoformat(last_event_time)

        # Default to 48 hours ago if neither file exists, likely first run
        if self.verbose:
            print("No timestamp file found, defaulting to 48 hours ago")
        return datetime.now(timezone.utc) - timedelta(seconds=172800)
    
    def write_activities_to_file(self, activities: list, start: datetime, end: datetime) -> None:
        filename = start.isoformat()
        if round((end - start).total_seconds()) == timedelta(hours=48).total_seconds():
            filename += "_48_hour_events.json"
        else:
            filename += "_events.json"
            # Exclude the last event to avoid duplication due to our use of the previous fetch's timestamp as a starting point
            activities = activities[:-1]
        with open(filename, "w") as f:
            # Newline delimited JSON
            for activity in activities:
                f.write(json.dumps(activity) + "\n")
        if self.verbose:
            print(f"Sample activities: {activities[:5]}")
        print(f"Wrote {len(activities)} activities to {filename}")
        self.write_latest_timestamp(activities)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    fetcher = ActivityFetcher("creds.json", "nancy.admin@hyenacapital.net", verbose=args.verbose)
    start = fetcher.read_latest_timestamp()
    end = datetime.now(timezone.utc)

    activities = fetcher.get_timestamped_activities(start, end)
    fetcher.write_activities_to_file(activities, start, end)

#TODO
"""
Part two:
- Identify the user with the most number of events.

- Which api method (see `method_name` parameter) has returned the most number of bytes to applications?

"""
