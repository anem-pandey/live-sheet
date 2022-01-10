import requests
import json
import pandas as pd
import gspread
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, timedelta

startdt = date.today() - timedelta(10)
sdtstr = startdt.strftime("%Y-%m-%d")
enddt = date.today() + timedelta(2)
enddtstr = enddt.strftime("%Y-%m-%d")

tz = "T18:30:00.000Z"

startdate = sdtstr + tz
enddate = enddtstr + tz

headers = {
    'Connection': 'keep-alive',
    'Accept': 'application/json, text/plain, */*',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    'Content-Type': 'application/json',
    'Origin': 'http://ordertracking.pristyncare.com.s3-website.ap-south-1.amazonaws.com',
    'Referer': 'http://ordertracking.pristyncare.com.s3-website.ap-south-1.amazonaws.com/',
    'Accept-Language': 'en-US,en;q=0.9',
}

raw_data = '{"appDate":{"startDate":"2022-01-05T18:30:00.000Z","endDate":"2022-01-06T18:30:00.000Z"},"page_number":1,"page_size":600,"sort":{"key":"AppointmentStartTime","order":-1}}'
datas = json.loads(raw_data)
datas['appDate']['startDate'] = startdate
datas['appDate']['endDate'] = enddate
data = json.dumps(datas)
response = requests.post('http://overhaullb-1186054598.ap-south-1.elb.amazonaws.com/otp_test/central-dashboard-routing/getReportingData', headers=headers, data=data, verify=False)

livedf = json.loads(response.text)
df = pd.json_normalize(livedf, record_path = ['data'])

responsecount = requests.post('http://overhaullb-1186054598.ap-south-1.elb.amazonaws.com/otp_test/central-dashboard-routing/getCountForReportingData', headers=headers, data=data, verify=False)
count = json.loads(responsecount.text)

counttxt = count["data"][0]["count"]

pageNum = 1

while True:
    pageNum = pageNum + 1
    datas['page_number'] = pageNum
    data = json.dumps(datas)
    response = requests.post('http://overhaullb-1186054598.ap-south-1.elb.amazonaws.com/otp_test/central-dashboard-routing/getReportingData', headers=headers, data=data, verify=False)

    livedf = json.loads(response.text)
    df2 = pd.json_normalize(livedf, record_path = ['data'])
    df = df.append(df2, ignore_index = True)
    if len(df.index) == counttxt:
        break


df = df.fillna('')
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('livesheet.json', scope)

gc = gspread.authorize(credentials)

spreadsheet_key = '1kiC_2M3xguOC2mNy4Vgegr78MfTjsnbQXqBAOHDVgh4'
wks_name = 'raw_data'
d2g.upload(df, spreadsheet_key, wks_name, credentials=credentials, row_names=True)
