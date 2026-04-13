"""
Fetches live data from UK Government sources
- TfL (Transport for London) Traffic Data
- UK Gov API data
"""

import requests
import pandas as pd
from datetime import datetime
from typing import Dict, List
import os
import time

class UKGovDataFetcher:
    """Fetch real-time data from UK government sources"""
    
    def __init__(self):
        self.tfl_api_base = "https://api.tfl.gov.uk"
        # TfL API doesn't require authentication for basic queries
        
    def fetch_tfl_traffic_data(self) -> pd.DataFrame:
        """
        Fetch traffic data from Transport for London
        Returns DataFrame with traffic incidents
        """
        try:
            # Try known traffic endpoints. TfL's old /Incidents route now returns 404.
            endpoints = [
                f"{self.tfl_api_base}/Road/all/Disruption",
                f"{self.tfl_api_base}/Road/All/Disruption",
                f"{self.tfl_api_base}/Road/all/Incidents",
            ]

            for url in endpoints:
                response = requests.get(url, timeout=10)
                if response.status_code != 200:
                    continue

                data = response.json()
                incidents = []
                for item in data:
                    incidents.append({
                        'disruption_id': item.get('id', ''),
                        'category': item.get('category', ''),
                        'severity': item.get('severity', ''),
                        'location': item.get('location', ''),
                        'comments': item.get('comments', ''),
                        'last_modified': item.get('lastModifiedTime', ''),
                        'timestamp': datetime.now().isoformat(),
                        'data_source': 'TfL_RoadDisruption',
                        'endpoint_used': url,
                    })

                return pd.DataFrame(incidents)

            print("Error fetching TfL data: all known road disruption endpoints failed")
            return pd.DataFrame()
                
        except Exception as e:
            print(f"Error fetching TfL traffic data: {e}")
            return pd.DataFrame()

    def check_tfl_realtime_health(self, polls: int = 2, interval_seconds: int = 15) -> pd.DataFrame:
        """
        Poll TfL endpoints multiple times and report whether fresh data is being fetched.
        Useful for notebook verification of real-time behavior.
        """
        rows = []

        if polls < 1:
            polls = 1

        for i in range(polls):
            traffic_df = self.fetch_tfl_traffic_data()
            line_df = self.fetch_tfl_line_status()

            traffic_signature = "|".join(
                sorted(traffic_df.get('disruption_id', pd.Series([], dtype=str)).astype(str).head(20).tolist())
            ) if not traffic_df.empty else ""

            line_signature = "|".join(
                sorted(
                    (line_df.get('line_name', pd.Series([], dtype=str)).astype(str)
                     + ":"
                     + line_df.get('status', pd.Series([], dtype=str)).astype(str)).head(50).tolist()
                )
            ) if not line_df.empty else ""

            rows.append({
                'poll': i + 1,
                'poll_time': datetime.now().isoformat(),
                'traffic_rows': int(len(traffic_df)),
                'line_rows': int(len(line_df)),
                'traffic_signature': traffic_signature,
                'line_signature': line_signature,
            })

            if i < polls - 1:
                time.sleep(interval_seconds)

        result = pd.DataFrame(rows)
        result['traffic_changed_since_first'] = result['traffic_signature'] != result['traffic_signature'].iloc[0]
        result['line_status_changed_since_first'] = result['line_signature'] != result['line_signature'].iloc[0]
        return result
    
    def fetch_tfl_line_status(self) -> pd.DataFrame:
        """
        Fetch London Underground line status
        """
        try:
            url = f"{self.tfl_api_base}/Line/Mode/tube/Status"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                lines_data = []
                for line in data:
                    for status in line.get('lineStatuses', []):
                        lines_data.append({
                            'line_id': line.get('id', ''),
                            'line_name': line.get('name', ''),
                            'status': status.get('statusSeverityDescription', ''),
                            'timestamp': datetime.now().isoformat(),
                            'data_source': 'TfL_LineStatus'
                        })
                
                return pd.DataFrame(lines_data)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Error fetching TfL line status: {e}")
            return pd.DataFrame()

    def fetch_tfl_bikepoint_status(self) -> pd.DataFrame:
        """
        Fetch live bike point status across London.
        Returns station-level bike and dock availability.
        """
        try:
            url = f"{self.tfl_api_base}/BikePoint"
            response = requests.get(url, timeout=15)

            if response.status_code != 200:
                print(f"Error fetching TfL BikePoint data: {response.status_code}")
                return pd.DataFrame()

            data = response.json()
            rows = []
            fetched_at = datetime.now().isoformat()

            for item in data:
                props = {p.get('key', ''): p.get('value', '') for p in item.get('additionalProperties', [])}
                rows.append({
                    'bikepoint_id': item.get('id', ''),
                    'name': item.get('commonName', ''),
                    'lat': item.get('lat', None),
                    'lon': item.get('lon', None),
                    'bikes_available': props.get('NbBikes', ''),
                    'empty_docks': props.get('NbEmptyDocks', ''),
                    'docks_total': props.get('NbDocks', ''),
                    'install_date': props.get('InstallDate', ''),
                    'locked': props.get('Locked', ''),
                    'temporary': props.get('Temporary', ''),
                    'timestamp': fetched_at,
                    'data_source': 'TfL_BikePoint',
                })

            return pd.DataFrame(rows)

        except Exception as e:
            print(f"Error fetching TfL BikePoint status: {e}")
            return pd.DataFrame()

    def fetch_uk_gov_data(self) -> pd.DataFrame:
        """
        Fetch data from UK.Parliament or other UK Gov endpoints
        """
        try:
            # Example: Fetch from UK Parliament data APIs (no auth required)
            # This is a demonstration - you can add more UK gov sources
            return pd.DataFrame({
                'source': ['UK Parliament'],
                'timestamp': [datetime.now().isoformat()],
                'note': ['Add UK gov data endpoints as needed']
            })
        except Exception as e:
            print(f"Error fetching UK Gov data: {e}")
            return pd.DataFrame()

def generate_sample_gps_data(num_records: int = 1000) -> pd.DataFrame:
    """
    Generate sample GPS-like data for testing
    In real scenario, this would be replaced with actual GPS data streams
    """
    import random
    from datetime import timedelta
    
    base_time = datetime.now()
    
    data = {
        'timestamp': [base_time + timedelta(seconds=i) for i in range(num_records)],
        'latitude': [random.uniform(51.3, 51.7) for _ in range(num_records)],  # London area
        'longitude': [random.uniform(-0.4, 0.2) for _ in range(num_records)],
        'speed_kmh': [random.uniform(0, 80) for _ in range(num_records)],
        'vehicle_id': [f"VEH_{random.randint(1000, 9999)}" for _ in range(num_records)],
        'location': [random.choice(['London', 'Manchester', 'Birmingham', 'Leeds']) for _ in range(num_records)]
    }
    
    return pd.DataFrame(data)

if __name__ == "__main__":
    # Test data fetcher
    fetcher = UKGovDataFetcher()
    
    print("Fetching TfL Traffic Data...")
    traffic_df = fetcher.fetch_tfl_traffic_data()
    print(f"Traffic data shape: {traffic_df.shape}")
    print(traffic_df.head())
    
    print("\nFetching TfL Line Status...")
    line_df = fetcher.fetch_tfl_line_status()
    print(f"Line status shape: {line_df.shape}")
    print(line_df.head())
    
    print("\nGenerating Sample GPS Data...")
    gps_df = generate_sample_gps_data(100)
    print(f"GPS data shape: {gps_df.shape}")
    print(gps_df.head())
