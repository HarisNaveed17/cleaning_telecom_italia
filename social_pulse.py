import pandas as pd
import json
from geopy.geocoders import Bing
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm

datetime = []
pos = []
municip = []

with open('social-pulse-milano.geojson', 'r') as t:
    x = json.load(t)
    # print(x['features'][0])
    # print(len(x['features']))
    for i in range(len(x['features'])):
        datetime.append(x['features'][i]['created'])
        x['features'][i]['geomPoint.geom']['coordinates'].reverse()
        pos.append(x['features'][i]['geomPoint.geom']['coordinates'])
        municip.append(x['features'][i]['municipality.name'])

tweets = pd.DataFrame({'datetime': datetime, 'coordinates': pos, 'municipality': municip})
tweets = tweets[tweets['municipality'] == 'Milano']
# You'll have to issue a key from Bing Maps. Remember that it only allows you 150,000 free requests so you'll
# have to either purchase more or use two or more accounts to label all of them.
geolocator = Bing('AvgTAK2Hb8xTK0o7D2IrSGaNItRjei97g-9JQNl61sO_1brIFVj-IRL_-t5aUTTv')
reverse = RateLimiter(geolocator.reverse, min_delay_seconds=0.01)
tqdm.pandas()
tweets = tweets.assign(addresses=tweets['coordinates'].iloc[:125000].progress_apply(reverse))
tweets.to_csv('tweets_unclassified.csv')
print(-1)
