import requests
import pandas as pd
import os
import dotenv
import json
dotenv.load_dotenv()
SPORTDEVS_API_KEY = os.getenv("SPORTDEVS_API_KEY")

def get_past_matches(date:str):
    url = f"https://tennis.sportdevs.com/matches-by-date?date=eq.{date}"
    headers = {
        "Authorization": f"Bearer {SPORTDEVS_API_KEY}"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()[-1]
        
        # Fetch past matches
        with open(f"etl/past_matches_api/raw_data_{date}.txt", "w") as f:
            f.write(json.dumps(data, indent=2))

        
        matches_data = []
        for match in data['matches']:
            match_info = {
                'status': match.get('status', None),
                'start_time': match.get('start_time', None),
                'tournament_id': match.get('tournament_id', None),
                'tournament_name': match.get('tournament_name', None),
                'away_team_name': match.get('away_team_name', None),
                'home_team_name': match.get('home_team_name', None),
                'home_team_score': match.get('home_team_score', None),
                'away_team_score': match.get('away_team_score', None)
            }
            matches_data.append(match_info)
        
        df = pd.DataFrame(matches_data)
        
        # Save to CSV
        df.to_csv(f"etl/past_matches_api/past_matches_{date}.csv", index=False)
        print(df)
    else:
        raise Exception(f"Error fetching data: {response.status_code}")


# Não achei
# Bracket Size, Seed, Idade, Altura,
if __name__ == "__main__":
    date = "2025-01-14"
    data = get_past_matches(date)[-1]
    # Example date