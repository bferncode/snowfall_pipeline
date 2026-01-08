import requests
import pandas as pd
import io
import re
from datetime import timedelta
from config.settings import USER_AGENT, DEFAULT_STATE, SNOTEL_NETWORKS

class SnotelClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': USER_AGENT})
        self.base_url_sites = "https://wcc.sc.egov.usda.gov/nwcc/yearcount?network={}&counttype=statelist&state={}"
        self.base_url_report = "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customSingleStationReport/hourly/start_of_period/"

    def get_site_inventory(self, state=DEFAULT_STATE):
        all_sites = []
        for ntwk in SNOTEL_NETWORKS:
            url = self.base_url_sites.format(ntwk, state)
            dfs = pd.read_html(url)
            for df in dfs:
                if 'site_name' in df.columns:
                    df['ntwk'] = ntwk
                    all_sites.append(df)
                    break
        if not all_sites: return pd.DataFrame()
        df = pd.concat(all_sites, ignore_index=True)
        df['site_id'] = df['site_name'].str.extract(r'\((.*?)\)')
        return df

    def fetch_historical_data(self, site_id, ntwk, start_date, end_date):
        ntwk_upper = ntwk.upper()
        station_param = f"{site_id}:{DEFAULT_STATE}:{ntwk_upper}%257Cid=%2522%2522%257Cname"
        url = f"{self.base_url_report}{station_param}/{start_date},{end_date}/stationId,name,SNWD::value,WTEQ::value?fitToScreen=false"
        try:
            res = self.session.get(url, timeout=20)
            res.raise_for_status()
            lines = [l for l in res.text.splitlines() if not l.startswith('#')]
            df = pd.read_csv(io.StringIO("\n".join(lines)))
            df['ntwk'] = ntwk
            return df
        except Exception as e:
            print(f"Error SNOTEL {site_id}: {e}")
            return None

class NWSClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': USER_AGENT})
        self.points_cache = {}

    def _get_metadata(self, lat, lon):
        key = f"{lat},{lon}"
        if key not in self.points_cache:
            res = self.session.get(f"https://api.weather.gov/points/{lat},{lon}", timeout=10)
            res.raise_for_status()
            self.points_cache[key] = res.json()['properties']
        return self.points_cache[key]

    def get_hourly_forecast(self, lat, lon):
        meta = self._get_metadata(lat, lon)
        res = self.session.get(meta['forecastHourly'], timeout=10)
        res.raise_for_status()
        df = pd.DataFrame(res.json()['properties']['periods'])
        df['startTime'] = pd.to_datetime(df['startTime']).dt.tz_localize(None)
        return df

    def get_snow_grid_data(self, lat, lon):
        meta = self._get_metadata(lat, lon)
        res = self.session.get(meta['forecastGridData'], timeout=10)
        res.raise_for_status()
        vals = res.json()['properties'].get('snowfallAmount', {}).get('values', [])
        records = []
        for entry in vals:
            time_part, duration_part = entry['validTime'].split('/')
            start_dt = pd.to_datetime(time_part).tz_localize(None)
            hrs = int(re.findall(r'\d+', duration_part)[0])
            records.append({
                'snow_start': start_dt,
                'snow_end': start_dt + timedelta(hours=hrs),
                'total_snow_in': round(entry['value'] * 0.0393701, 3)
            })
        return pd.DataFrame(records)
