import requests
import json
from datetime import datetime, timedelta


def get_match_id(data):
    """解析比赛数据并提取 matchId"""
    match_id_list = []
    match_date_list = []
    for sub_match in data["value"]["matchList"]:
        for k, match in sub_match.items():
            if k == "matchDate" or k == "isToday":
                continue
            for details in match:
                if details["gmMatchId"] != 0:
                    match_id_list.append(details["gmMatchId"])
                    match_date_list.append(sub_match['matchDate'])
    return match_id_list, match_date_list


def fetch_data(start_date, end_date, season_id, league_id):
    """发送请求并返回比赛数据"""
    url = "https://webapi.sporttery.cn/gateway/uniform/football/league/getMatchResultV1.qry"
    params = {
        "seasonId": season_id,
        "uniformLeagueId": league_id,
        "startDate": start_date.strftime("%Y-%m-%d"),
        "endDate": end_date.strftime("%Y-%m-%d"),
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print(f"请求失败，状态码: {response.status_code}")
        return None


def crawl_match_ids(start_date, end_date, season_id, league_id):
    """爬取从 start_date 到 end_date 之间的所有比赛 ID"""
    current_date = start_date
    all_match_ids = []
    all_match_dates = []
    while current_date < end_date:
        # 确定当前的 7 天范围
        range_start = current_date
        range_end = min(current_date + timedelta(days=6), end_date)

        # 请求数据
        print(f"正在获取 {range_start} 到 {range_end} 的比赛数据...")
        data = fetch_data(range_start, range_end, season_id, league_id)
        if data:
            match_ids, match_dates = get_match_id(data)
            all_match_ids.extend(match_ids)
            all_match_dates.extend(match_dates)
        else:
            print(f"未能获取 {range_start} 到 {range_end} 的数据")

        # 更新日期，进入下一周
        current_date += timedelta(days=7)
    return all_match_ids, all_match_dates


if __name__ == "__main__":
    # 起始日期和结束日期
    start_date = datetime(2024, 8, 16)
    end_date = datetime(2024, 8, 22)
    # 英超 2024-2025
    season_id = "11817"
    league_id = "72"

    # 爬取所有比赛 ID gmMatchId
    match_ids, match_dates = crawl_match_ids(start_date, end_date, season_id, league_id)
    assert len(match_ids) == len(match_dates), '爬取错误'
    print(f"获取到的比赛 ID: {match_ids}")
    print(f"获取到的比赛日期: {match_dates}")
