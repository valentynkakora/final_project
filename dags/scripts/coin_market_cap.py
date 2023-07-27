import requests
import os
from dotenv import load_dotenv
import time

load_dotenv()

CMC_API_KEY = os.environ.get('CMC_API_KEY')


class CoinMarketCap:
    @staticmethod
    def get_exchanges_map(exchange_map_url: str, api_key: str):
        """The current link exchange_map_url is: https://pro-api.coinmarketcap.com/v1/exchange/map"""
        headers = {
            "X-CMC_PRO_API_KEY": api_key
        }
        exchanges_map = list()
        response = requests.get(exchange_map_url, headers=headers)
        current_time = time.time()
        if response.status_code == 200:
            data = response.json()["data"]
            for element in data:
                id = element["id"]
                slug = element["slug"]
                name = element["name"]
                first_historical_data = element["first_historical_data"]
                data = {
                    "id": id,
                    "slug": slug,
                    "name": name,
                    "first_historical_data": first_historical_data,
                    "time": current_time
                }
                exchanges_map.append(data)
            return exchanges_map
        else:
            return f"Request failed. Status code: {response.status_code}"

    @staticmethod
    def get_exchanges_data(ids: list, api_key: str):
        """ids - list of ids related to the id of cryptoexchange"""
        list_of_strings_ids = list()
        for id in ids:
            list_of_strings_ids.append(str(id))
        string_ids = ",".join(list_of_strings_ids)
        headers = {
            "X-CMC_PRO_API_KEY": api_key
        }
        params = {
            "id": string_ids
        }
        response = requests.get("https://pro-api.coinmarketcap.com/v1/exchange/info", headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()["data"]
            exchanges_data = list()
            current_time = time.time()
            for id, values in data.items():
                spot_volume_usd_24h = values["spot_volume_usd"]
                weekly_visits = values["weekly_visits"]
                exchanges_data.append(
                    {
                        "id": id,
                        "spot_volume_usd_24h": spot_volume_usd_24h,
                        "weekly_visits": weekly_visits,
                        "last_requested": current_time
                    }
                )
            return exchanges_data
        else:
            return f"Request failed. Status code: {response.status_code}"
