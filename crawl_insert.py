import requests
import json
from datetime import datetime, timedelta
import pymysql


def get_had(data):
    # 获取 hadList 的内容
    had_list = data["value"]["oddsHistory"]["hadList"]

    # 按照日期和时间找到最新的一条记录
    latest_entry = max(had_list, key=lambda x: (x["updateDate"], x["updateTime"]))

    # 提取最新记录的 a, d, h 值(负平胜)
    a_value = latest_entry["a"]
    d_value = latest_entry["d"]
    h_value = latest_entry["h"]

    # print(f"最新的数据为: a={a_value}, d={d_value}, h={h_value}")
    return [h_value, a_value, d_value]


def get_hhad(data):
    # 获取 hhadList 的内容
    hhad_list = data["value"]["oddsHistory"]["hhadList"]

    # 按照日期和时间找到最新的一条记录
    latest_entry = max(hhad_list, key=lambda x: (x["updateDate"], x["updateTime"]))

    # 提取最新记录的 a, d, h 值
    a_value = latest_entry["a"]
    d_value = latest_entry["d"]
    h_value = latest_entry["h"]

    # print(f"最新的数据为: a={a_value}, d={d_value}, h={h_value}")
    return [h_value, d_value, d_value]


def get_ttg(data):
    # 获取 ttgList 的内容
    ttg_list = data["value"]["oddsHistory"]["ttgList"]

    # 按照日期和时间找到最新的一条记录
    latest_entry = max(ttg_list, key=lambda x: (x["updateDate"], x["updateTime"]))

    # 提取最新记录的 a, d, h 值
    lst = []
    for i in range(8):
        lst.append(latest_entry[f"s{i}"])

    # print(f"最新的数据为: {lst}")
    return lst


def get_hafu(data):
    # 获取 hafuList 的内容
    hafu_list = data["value"]["oddsHistory"]["hafuList"]

    # 按照日期和时间找到最新的一条记录
    latest_entry = max(hafu_list, key=lambda x: (x["updateDate"], x["updateTime"]))

    # 提取最新记录的 a, d, h 值
    lst = []
    for i in ["a", "d", "h"]:
        for j in ["a", "d", "h"]:
            lst.append(latest_entry[f"{i}{j}"])

    # print(f"最新的数据为: {lst}")
    return lst


def get_crs(data):
    # 获取 crsList 的内容
    crs_list = data["value"]["oddsHistory"]["crsList"]

    # 按照日期和时间找到最新的一条记录
    latest_entry = max(crs_list, key=lambda x: (x["updateDate"], x["updateTime"]))

    lst = []
    lst.append(latest_entry["s01s00"])
    lst.append(latest_entry["s02s00"])
    lst.append(latest_entry["s02s01"])
    lst.append(latest_entry["s03s00"])
    lst.append(latest_entry["s03s01"])
    lst.append(latest_entry["s03s02"])
    lst.append(latest_entry["s04s00"])
    lst.append(latest_entry["s04s01"])
    lst.append(latest_entry["s04s02"])
    lst.append(latest_entry["s05s00"])
    lst.append(latest_entry["s05s01"])
    lst.append(latest_entry["s05s02"])
    lst.append(latest_entry["s-1sh"])
    lst.append(latest_entry["s00s00"])
    lst.append(latest_entry["s01s01"])
    lst.append(latest_entry["s02s02"])
    lst.append(latest_entry["s03s03"])
    lst.append(latest_entry["s-1sd"])
    lst.append(latest_entry["s00s01"])
    lst.append(latest_entry["s00s02"])
    lst.append(latest_entry["s01s02"])
    lst.append(latest_entry["s00s03"])
    lst.append(latest_entry["s01s03"])
    lst.append(latest_entry["s02s03"])
    lst.append(latest_entry["s00s04"])
    lst.append(latest_entry["s01s04"])
    lst.append(latest_entry["s02s04"])
    lst.append(latest_entry["s00s05"])
    lst.append(latest_entry["s01s05"])
    lst.append(latest_entry["s02s05"])
    lst.append(latest_entry["s-1sa"])

    # print(f"最新的数据为: {lst}")
    return lst


def get_meta(data):
    lst = []
    meta_list = data["value"]["oddsHistory"]
    lst.append(meta_list["homeTeamAllName"])
    lst.append(meta_list["awayTeamAllName"])

    meta_list = data["value"]["matchResultList"]

    for item in meta_list:
        if item["code"] == "HHAD":
            lst.append(item["combinationDesc"])
        elif item["code"] == "HAFU":
            lst.append(item["combinationDesc"])
        elif item["code"] == "CRS":
            lst.append(item["combinationDesc"])

    # print(lst)
    return lst


def restore_data(data):
    lst = ["hadList", "hhadList", "ttgList", "hafuList", "crsList"]
    for item in lst:
        if len(data["value"]["oddsHistory"][item]) == 0:
            return False
    return True


def crawl_match_bet(match_id_list):
    # 页面 URL
    url = "https://webapi.sporttery.cn/gateway/jc/football/getFixedBonusV1.qry"

    all_data = []
    # 记录要移除的index
    remove_idx = []
    for i, match_id in enumerate(match_id_list):
        params = {"clientCode": 3001, "matchId": match_id}
        # 发送请求获取页面内容
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        }
        response = requests.get(url, headers=headers, params=params)
        # print(response.text)
        # 将 response.text 转换为 JSON 对象
        data = json.loads(response.text)

        # 判断data是否缺失某些赔率数据
        if restore_data(data) == False:
            remove_idx.append(i)
            print("数据缺失，跳过该比赛")
            continue

        lst = []
        lst.append(get_had(data))  # 胜平负
        lst.append(get_hhad(data))  # 让球胜平负
        lst.append(get_ttg(data))  # 总进球
        lst.append(get_crs(data))  # 比分
        lst.append(get_hafu(data))  # 半场胜平负
        lst.append(get_meta(data))  # 比赛元信息
        print(lst)
        all_data.append(lst)
    return all_data, remove_idx


def insert_match_result(all_data, ids, dates, conn):
    try:
        with conn.cursor() as cursor:
            for i, data in enumerate(all_data):
                # 插入数据的 SQL 语句
                sql = """
                    INSERT INTO match_result (match_id, team_home, team_away, hhad, hafu, crs, date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                # 执行插入操作
                cursor.execute(
                    sql,
                    (
                        ids[i],
                        data[-1][0],
                        data[-1][1],
                        data[-1][2],
                        data[-1][3],
                        data[-1][4],
                        dates[i],
                    ),
                )
                conn.commit()  # 提交事务
                print("数据插入成功！")
    except Exception as e:
        print(f"插入数据时出错: {e}")
        conn.close()


def insert_match_had(all_data, ids, connection):
    try:
        with connection.cursor() as cursor:
            for i, data in enumerate(all_data):
                # 插入数据的 SQL 语句
                sql = """
                    INSERT INTO match_had (match_id, h, a, d)
                    VALUES (%s, %s, %s, %s)
                """
                # 执行插入操作
                cursor.execute(
                    sql,
                    (ids[i], data[0][0], data[0][1], data[0][2]),
                )
                connection.commit()  # 提交事务
                print("数据插入成功！")
    except Exception as e:
        print(f"插入数据时出错: {e}")
        connection.close()


def insert_match_hhad(all_data, ids, connection):
    try:
        with connection.cursor() as cursor:
            for i, data in enumerate(all_data):
                # 插入数据的 SQL 语句
                sql = """
                    INSERT INTO match_hhad (match_id, h, a, d)
                    VALUES (%s, %s, %s, %s)
                """
                # 执行插入操作
                cursor.execute(
                    sql,
                    (ids[i], data[1][0], data[1][1], data[1][2]),
                )
                connection.commit()  # 提交事务
                print("数据插入成功！")
    except Exception as e:
        print(f"插入数据时出错: {e}")
        connection.close()


def insert_match_crs(all_data, ids, connection):
    try:
        with connection.cursor() as cursor:
            for i, data in enumerate(all_data):
                # 插入数据的 SQL 语句
                sql = """
                    INSERT INTO match_crs
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                lst = [ids[i]]
                lst.extend([data[-3][i] for i in range(len(data[-3]))])
                # 执行插入操作
                cursor.execute(
                    sql,
                    lst,
                )
                connection.commit()  # 提交事务
                print("数据插入成功！")
    except Exception as e:
        print(f"插入数据时出错: {e}")
        connection.close()


def insert_match_hafu(all_data, ids, connection):
    try:
        with connection.cursor() as cursor:
            for i, data in enumerate(all_data):
                # 插入数据的 SQL 语句
                sql = """
                    INSERT INTO match_hafu
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                lst = [ids[i]]
                lst.extend([data[-2][i] for i in range(len(data[-2]))])
                # 执行插入操作
                cursor.execute(
                    sql,
                    lst,
                )
                connection.commit()  # 提交事务
                print("数据插入成功！")
    except Exception as e:
        print(f"插入数据时出错: {e}")
        connection.close()


def insert_match_ttg(all_data, ids, connection):
    try:
        with connection.cursor() as cursor:
            for i, data in enumerate(all_data):
                # 插入数据的 SQL 语句
                sql = """
                    INSERT INTO match_ttg
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                lst = [ids[i]]
                lst.extend([data[2][i] for i in range(len(data[2]))])
                # 执行插入操作
                cursor.execute(
                    sql,
                    lst,
                )
                connection.commit()  # 提交事务
                print("数据插入成功！")
    except Exception as e:
        print(f"插入数据时出错: {e}")
        connection.close()


# 插入所有数据到数据库
def insert_all_data(all_data, new_match_ids, new_match_dates):
    # 数据库连接配置
    connection = pymysql.connect(
        host="localhost",  # 数据库地址
        user="root",  # 用户名
        password="root",  # 密码
        database="lottery",  # 数据库名称
        charset="utf8mb4",  # 字符集
    )
    # 插入比赛元信息到数据库
    insert_match_result(all_data, new_match_ids, new_match_dates, connection)
    # 插入胜平负到数据库
    insert_match_had(all_data, new_match_ids, connection)
    # 插入让球胜平负到数据库
    insert_match_hhad(all_data, new_match_ids, connection)
    # 插入比分
    insert_match_crs(all_data, new_match_ids, connection)
    # 半场胜平负
    insert_match_hafu(all_data, new_match_ids, connection)
    # 总进球
    insert_match_ttg(all_data, new_match_ids, connection)
    # 关闭数据库连接
    connection.close()


if __name__ == "__main__":
    from crawler_all_match_id import crawl_match_ids

    # 起始日期和结束日期
    start_date = datetime(2024, 8, 16)
    end_date = datetime(2024, 8, 22)
    # 英超 2024-2025
    season_id = "11817"
    league_id = "72"

    # 爬取所有比赛 ID gmMatchId
    match_ids, match_dates = crawl_match_ids(start_date, end_date, season_id, league_id)
    # print(f"获取到的比赛 ID: {match_ids}")
    # print(f"获取到的比赛日期: {match_dates}")

    # 爬取赔率数据
    all_data, remove_idx = crawl_match_bet(match_ids)
    new_match_ids = [item for i, item in enumerate(match_ids) if i not in remove_idx]
    new_match_dates = [
        item for i, item in enumerate(match_dates) if i not in remove_idx
    ]
    print(f"获取到的比赛 ID: {new_match_ids}")
    print(f"获取到的比赛日期: {new_match_dates}")

    print(len(all_data), len(new_match_ids), len(new_match_dates))

    insert_all_data(all_data, new_match_ids, new_match_dates)
