import requests, os
import pandas as pd

#!/usr/bin/env python3

def main():
    wsoc_apikey = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI0NjFiMTExMS02ZjdhLTRkYmItOWQyOS0yMzAzOWZlMjI4OGUiLCJqdGkiOiJhN2U5MjBhN2RhMDJkMjFkMDVhNTljN2E0ZTVmYzAxMDkyMGE0NDRhNTY2Mjg1ZDJkMDk4Y2Y1OWQ5OWEwODhkYmQ0NTVjOGQyNGYxNzkwMyIsImlhdCI6MTczNzY2OTUxMC42MDI1OTUsIm5iZiI6MTczNzY2OTUxMC42MDI1OTcsImV4cCI6NDg5MTI2OTUxMC41OTMyODgsInN1YiI6IjdhMmQ3MDM4LTMxYzgtNDUxNS1iZTM4LTkwNDRiOWY3ZWQ0OSIsInNjb3BlcyI6WyJjb25uZWN0Iiwic2Vuc29yLXJlYWQtb25seSIsImF0aGxldGVzLXVwZGF0ZSIsInRhZ3MtdXBkYXRlIiwiYWN0aXZpdGllcy11cGRhdGUiLCJhbm5vdGF0aW9ucy11cGRhdGUiLCJwYXJhbWV0ZXJzLXVwZGF0ZSJdfQ.vUaUio4sdG1fxs9FgO1DHtSRDsInCUiHdbKh01o8o4AjzIcoKXcg_S0vnJr7yO5TWv5wn5_9wYbY4b3bO-48WDr3lZY4EsTEKNCwQe0PzyJ11JJEMZpqylisp2oM97PnqVZWIDXD1tatWrAq0i0BBX93TCfanvzQf93eVKEzCD89S8UwFW5gyrwTH2Zx0HcXUo7PIOJTc-Ie1NDoItAYrSyIfzkJxHV_vOadiTuHuDyl6DFuyJrPwJMJW4MGhb3l1L88HlYNw297ePeq1HOy8vl1cBgvjjpxqp9lTSGj1nZjUjGFkCT6jqh2DmZyrVr0F-Pi3QSwPIOV3ziYYeyenRLBDZddY50ioLC9kk5Y2tFOKmBT6rFK881ek7RyNwt0i8k-RSVCrcgm1QLnPCk6DeOWUPSquSQY5TrUmSboyCMss6mNml_dyw7Kme5uZhv37H8B82ZIal73q_zxIuoX6wvu-pUinrarCAfyDJi37T72gG1w9EndE5eF-uhHUINtVIRuRVPzNlPvGUsnBIoqsm_6N_jS4tQsDSPewGbzZ-PcX2bzL3j5QcSZuu0zJjCJQ740pjr1wqXVWW_qsPj-Fauq54aGfHTDJZfI0U2dW9E5TeG4-2yw3en9-79_IIVoi0u4fxMzI8FeeD_h1CCrNNDg5ZcuhpXpruucQhpR3fE"
    msoc_apikey = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI0NjFiMTExMS02ZjdhLTRkYmItOWQyOS0yMzAzOWZlMjI4OGUiLCJqdGkiOiJiZmMzNTMxOGFkNzJmZDE0ZDdjZTMyMmNiMWFmMmRhNGM2OWQ2YWY4YmIyZWU1MWFkZTFkNjIyMjU1YTE4MjhhZDA2OTY5YjU4YzRiYTBjZiIsImlhdCI6MTczNzUwMDE3Mi4zNDU2OCwibmJmIjoxNzM3NTAwMTcyLjM0NTY4MiwiZXhwIjo0ODkxMTAwMTcyLjMzNzE2OCwic3ViIjoiMWIxOThhY2UtZWM1NC00NTEwLWI2YjktM2UwMWYzMTgxNzYyIiwic2NvcGVzIjpbImNvbm5lY3QiLCJzZW5zb3ItcmVhZC1vbmx5IiwiYXRobGV0ZXMtdXBkYXRlIiwidGFncy11cGRhdGUiLCJhY3Rpdml0aWVzLXVwZGF0ZSIsImFubm90YXRpb25zLXVwZGF0ZSIsInBhcmFtZXRlcnMtdXBkYXRlIl19.KT5lz3tadAwMTR4A04nmFxQLPS8tlKEzGxm5os9TucX8aro1wsqTIFW77tvJITjsDeUuhRgNh9oZuc0bzIWJcNjx_zdht1hKH1bX0HodzhN5Nf791BezQOwPdtTOZH9fEZtCzT5auhh64Et4YC9S9_iu2YtUWr8NCVfi7acchY1oScZEm_Dqd_fewB3oza9auKmppDBtMkghLsS8zmINxGLxFCpZo3LTCiKZlbZGoRL_lLGRlrtxG_bVGFqsOoGYH9MjT0cBCYIDAOI8lHuQhmoedcFUW-qBb7szd3ol07b6jQB5glDHSTaoNtHwTXSlVkkUKKU6DKcXN60G7k5Fi8ky_m9eJSWWrrta6xX8RkX_xn_RZmSwSoUkAsXquGTIW0ujYmtd7d3c-M2UMBleXD3cUiY7xgAoac1C486SNESuKaTf58SQk-bzTL1V7HhnBzdr3OtE-8zPi1VeKzqEB3brvkWIwPwv9DJNDO-i6tG013PABcmRcwWN2ckelCY8EmfayMz5XdBJNBDjxfJB_5QSiXlFJpgvb4DuJ_fs2ktDfUNgoDL0-Jpxgqc228AzAa1kY4vkyKhkcG4AtqnwbKz334qKj_jB9kI-SAqVx00IyLO3rbbJxJlD2keUvKWsqXiDyllLhG93Zq0mZruooyS_ysHfIucaYoWZaoR-6sM"

    athletes_api_url = "https://connect-us.catapultsports.com/api/v6/athletes"
    activities_api_url = "https://connect-us.catapultsports.com/api/v6/activities"
    stats_api_url = "https://connect-us.catapultsports.com/api/v6/stats"
    positions_api_url = "https://connect-us.catapultsports.com/api/v6/positions"
    parameters_api_url = "https://connect-us.catapultsports.com/api/v6/parameters"

    activityId = "e92f11e7-6b5b-4629-a024-4a79d6cff89e"
    athleteId = ""

    used_api = activities_api_url
    used_key = wsoc_apikey

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": f"Bearer {used_key}"
    }

    payload = {
        "parameters": ["total_distance", "average_player_load"],
        "filters": [
            {
                "comparison": "=",
                "name": "activity_id",
                "values": [activityId]
            }
        ],
        "group_by": ["athlete"],
        "source": "cached_stats"
    }

    try:
        response = requests.get(used_api, headers=headers)
        # response = requests.post(used_api, json=payload, headers=headers)
        response.raise_for_status()

        # Step 1: Parse JSON
        data = response.json()  # gives you list of dicts

        # Step 2: Convert to DataFrame
        df = pd.DataFrame(data)

        # Step 3: Export to Excel or CSV
        # df.to_excel("wsoc.xlsx", index=False)  # Excel
        df.to_csv("activities.csv", index=False)     # CSV

        print("Data exported successfully!")

    except requests.exceptions.RequestException as err:
        print(f"Error: {err}")

main()