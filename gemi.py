import aiohttp
import asyncio
import time

url = "https://publicity.businessportal.gr/api/searchCompany"

headers = {
  'Accept': 'application/json, text/plain, */*',
  'Accept-Language': 'en-US,en;q=0.9,el;q=0.8',
  'Connection': 'keep-alive',
  'Content-Type': 'application/json',
  'Cookie': '_ga=GA1.1.126991110.1716456604; _ga_HG88EBQE2M=GS1.1.1716456604.1.1.1716456614.0.0.0; next-i18next=el; _ga_L7TS87DDSM=GS1.1.1716554135.7.0.1716554135.0.0.0; _ga_4Z268JDK2N=GS1.1.1716713707.6.0.1716713707.0.0.0',
  'Origin': 'https://publicity.businessportal.gr',
  'Referer': 'https://publicity.businessportal.gr/',
  'Sec-Fetch-Dest': 'empty',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Site': 'same-origin',
  'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36',
  'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
  'sec-ch-ua-mobile': '?1',
  'sec-ch-ua-platform': '"Android"'
}
# Define the static parts of the payload
base_payload ={
  "dataToBeSent": {
    "city": None,
    "postcode": None,
    "legalType": [
      "1",
      "2",
      "3",
      "4",
      "8",
      "9",
      "10",
      "11",
      "12",
      "13",
      "14",
      "16",
      "17",
      "18",
      "19",
      "20",
      "64",
      "65",
      "66",
      "67",
      "68",
      "69",
      "70"
    ],
    "status": [
      "0",
      "3",
      "4",
      "5",
      "7",
      "15",
      "17",
      "18",
      "19",
      "20"
    ],
    "suspension": [
      True,
      False
    ],
    "category": [],
    "specialCharacteristics": [],
    "employeeNumber": [],
    "armodiaGEMI": [
      "18"
    ],
    "kad": [
      10444,
      10459,
      10465,
      10466,
      1,
      546,
      642,
      3904,
      3953,
      4096,
      4328,
      8088,
      8380,
      8496,
      8805,
      8990,
      9033,
      9434,
      9722,
      9785,
      9885,
      10092,
      10265
    ],
    "recommendationDateFrom": "01/01/1970",
    "recommendationDateTo": "31/12/2023",
    "closingDateFrom": None,
    "closingDateTo": None,
    "alterationDateFrom": None,
    "alterationDateTo": None,
    "person": [],
    "personrecommendationDateFrom": None,
    "personrecommendationDateTo": None,
    "radioValue": "all",
    "places": [],
    "page": 2
  },
  "token": None,
  "language": "el"
}

start_time = time.time()

df_list = []
json_list=[]
armodiaGEMI = ['1127']
title='YPOURGEIO_ERGASIAS_KALO'
nbr_pages = 15
base_payload["dataToBeSent"]["armodiaGEMI"] = armodiaGEMI
base_payload["dataToBeSent"]["recommendationDateFrom"] = '01/01/1970'
base_payload["dataToBeSent"]["recommendationDateTo"] = '31/12/2023'
async def fetch(session, page):
    payload = base_payload.copy()
    payload["dataToBeSent"]["page"] = page

    payload_json = json.dumps(payload)
    try:
        async with session.post(url, headers=headers, data=payload_json) as resp:
            response = await resp.json()
            json_list.append(response)
            df_list.append(pd.DataFrame(response['hits']))
    except Exception as e:
        print(f"An error occurred for page {page}: {e}")

async def main():
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, page) for page in range(1, nbr_pages)]
        await asyncio.gather(*tasks)

# In a Jupyter notebook, simply call the main function with await
await main()
print("--- %s seconds ---" % (time.time() - start_time))


count = 0
for i in range(len(df_list)):
    count += len(df_list[i])
print(count)   

with open(f'RAW_{title}.ndjson', 'a') as f:
    for json_obj in json_list:
        f.write(json.dumps(json_obj) + '\n')
merged_df = pd.concat(df_list, ignore_index=True)
merged_df['armodiaGEMI'] = armodiaGEMI[0]

def process_list_entry_page_column(entry):
    if not entry:  # Check if the list is empty
        return np.nan  # or return '' for an empty string
    elif len(entry) == 1:  # Check if the list has only one element
        return entry[0]
    else:  # If the list has more than one element
        return '?'.join(entry)
merged_df['ProcessedTitle'] = merged_df['title'].apply(process_list_entry_page_column)
merged_df = merged_df.drop(columns=['title'])
merged_df.to_csv(f'{title}.csv',index=False)
print(pd.to_datetime('today'))
