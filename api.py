import requests
import time

class SleeperConn(object):
    def __init__(self, base_url="https://api.sleeper.app/v1", debug=False):

        self.base_url = base_url
        self.debug    = debug

    def _get(self, endpoint):
        """
        Helper method to make GET requests to the Sleeper API.

        :param endpoint: str, API endpoint (relative to the base URL)
        :return: dict, JSON response from the API
        """
        start_timestamp = time.perf_counter()
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            returnval = response.json()
        except requests.RequestException as e:
            print(f"Error fetching data from {url}: {e}")
            returnval = None

        elapsed = time.perf_counter() - start_timestamp
        if self.debug:
            print(f"API call to {endpoint} took: {elapsed:.4f}s")

        return returnval
