import requests
import json
import datetime
from datetime import timedelta, date
import pymysql
from .crawler_all_match_id import crawl_match_ids, crawl_five_league_match_ids
from .crawl_insert import crawl_match_bet, insert_all_data

def query_match_ids_by_date_range(start_date, end_date):
    # 数据库连接配置
    connection = pymysql.connect(
        host="localhost",  # 数据库地址
        user="root",  # 用户名
        password="123456",  # 密码
        database="lottery",  # 数据库名称
        charset="utf8mb4",  # 字符集
    )

    match_ids = []  # 存储查询到的 match_id
    try:
        with connection.cursor() as cursor:
            # 查询 match_id 的 SQL 语句，按时间范围过滤
            sql = """
                SELECT match_id FROM match_result 
                WHERE date BETWEEN %s AND %s
            """
            # 执行查询操作，传递时间范围参数
            cursor.execute(sql, (start_date, end_date))
            # 获取所有查询结果
            result = cursor.fetchall()
            # 将结果中的 match_id 存储到 match_ids 列表
            match_ids = [row[0] for row in result]

            print("查询成功！")
    except Exception as e:
        print(f"查询数据时出错: {e}")
    finally:
        # 确保连接关闭
        connection.close()

    return match_ids


def insert_or_update(all_data, match_ids, match_dates, cur_match_ids):
    if len(match_ids) == 0:
        print("当前阶段比赛未开起赔率")
        return
    # 数据库连接配置
    connection = pymysql.connect(
        host="localhost",  # 数据库地址
        user="root",  # 用户名
        password="123456",  # 密码
        database="lottery",  # 数据库名称
        charset="utf8mb4",  # 字符集
    )
    try:
        with connection.cursor() as cursor:
            for i, match_id in enumerate(match_ids):
                # 已有数据，update即可
                if match_id in cur_match_ids:
                    update_match_had = """ UPDATE match_had SET h = %s, a = %s, d = %s WHERE match_id = %s """
                    cursor.execute(
                        update_match_had,
                        (
                            all_data[i][0][0],
                            all_data[i][0][1],
                            all_data[i][0][2],
                            match_ids[i],
                        ),
                    )
                    print(f"更新比赛信息1成功，match_id={match_id}")
                    update_match_hhad = """ UPDATE match_hhad SET h = %s, a = %s, d = %s WHERE match_id = %s """
                    cursor.execute(
                        update_match_hhad,
                        (
                            all_data[i][1][0],
                            all_data[i][1][1],
                            all_data[i][1][2],
                            match_ids[i],
                        ),
                    )

                    update_match_ttg = """ UPDATE match_ttg SET s0 = %s, s1 = %s, s2 = %s, s3 = %s, s4 = %s, s5 = %s, s6 = %s, s7=%s
                    WHERE match_id = %s
                    """
                    cursor.execute(
                        update_match_ttg,
                        (
                            all_data[i][2][0],
                            all_data[i][2][1],
                            all_data[i][2][2],
                            all_data[i][2][3],
                            all_data[i][2][4],
                            all_data[i][2][5],
                            all_data[i][2][6],
                            all_data[i][2][7],
                            match_ids[i],
                        ),
                    )
                    print(f"更新比赛信息2成功，match_id={match_id}")

                    update_match_crs = """
                    UPDATE match_crs SET s01s00=%s,s02s00=%s,s02s01=%s,s03s00=%s,s03s01=%s,s03s02=%s,s04s00=%s,s04s01=%s,s04s02=%s,s05s00=%s,s05s01=%s,s05s02=%s,s_1sh=%s,
                    s00s00=%s,s01s01=%s,s02s02=%s,s03s03=%s,s_1sd=%s,
                    s00s01=%s,s00s02=%s,s01s02=%s,s00s03=%s,s01s03=%s,s02s03=%s,s00s04=%s,s01s04=%s,s02s04=%s,s00s05=%s,s01s05=%s,s02s05=%s,s_1sa=%s
                    WHERE match_id=%s
                    """
                    lst = [all_data[i][3][j] for j in range(len(all_data[i][3]))]
                    lst.append(match_id)
                    cursor.execute(update_match_crs, lst)
                    print(f"更新比赛信息3成功，match_id={match_id}")

                    update_match_hafu = """
                    UPDATE match_hafu SET aa=%s,ad=%s,ah=%s,da=%s,dd=%s,dh=%s,ha=%s,hd=%s,hh=%s
                    WHERE match_id=%s
                    """
                    lst = [all_data[i][4][j] for j in range(len(all_data[i][4]))]
                    lst.append(match_id)
                    cursor.execute(update_match_hafu, lst)
                    print(f"更新比赛信息4成功，match_id={match_id}")

                    connection.commit()
                    print(f"更新比赛信息成功，match_id={match_id}")
                # 新的match，insert
                else:
                    print("开始插入数据, match_id={match_id}")
                    lst = [match_id]
                    lst.extend(
                        [all_data[i][-1][j] for j in range(len(all_data[i][-1]))]
                    )
                    lst.append(match_dates[i])
                    print(lst)
                    if len(lst) == 4:
                        insert_match_result = """
                        INSERT INTO match_result (match_id, team_home, team_away, date) VALUES (%s, %s, %s, %s)
                        """
                    else:
                        insert_match_result = """
                        INSERT INTO match_result VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """
                    cursor.execute(insert_match_result, lst)
                    insert_match_had = """
                    INSERT INTO match_had VALUES (%s, %s, %s, %s)
                    """
                    lst = [match_id]
                    lst.extend([all_data[i][0][j] for j in range(len(all_data[i][0]))])
                    cursor.execute(insert_match_had, lst)
                    insert_match_hhad = """
                    INSERT INTO match_hhad VALUES (%s, %s, %s, %s)
                    """
                    lst = [match_id]
                    lst.extend([all_data[i][1][j] for j in range(len(all_data[i][1]))])
                    cursor.execute(insert_match_hhad, lst)
                    insert_match_ttg = """
                    INSERT INTO match_ttg VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    lst = [match_id]
                    lst.extend([all_data[i][2][j] for j in range(len(all_data[i][2]))])
                    cursor.execute(insert_match_ttg, lst)
                    insert_match_crs = """
                    INSERT INTO match_crs VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    lst = [match_id]
                    lst.extend([all_data[i][3][j] for j in range(len(all_data[i][3]))])
                    cursor.execute(insert_match_crs, lst)
                    insert_match_hafu = """
                    INSERT INTO match_hafu VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    lst = [match_id]
                    lst.extend([all_data[i][4][j] for j in range(len(all_data[i][4]))])
                    cursor.execute(insert_match_hafu, lst)
                    connection.commit()
                    print(f"插入比赛信息成功，match_id={match_id}")
    except Exception as e:
        print(f"插入/更新数据时出错: {e}")
        connection.rollback()
    finally:
        # 确保连接关闭
        cursor.close()
        connection.close()

def crawl_insert_newest_match():
    # 获取当前日期
    today = date.today()
    # 起始日期和结束日期
    start_date = today
    end_date = start_date + datetime.timedelta(days=6)

    # # 起始日期和结束日期
    # start_date = datetime(2024, 11, 23)
    # end_date = datetime(2024, 11, 29)

    leagues = {
        "英超 2024-2025": {"season_id": "11817", "league_id": "72"},
        "德甲 2024-2025": {"season_id": "11956", "league_id": "1803"},
        "法甲 2024-2025": {"season_id": "11848", "league_id": "74"},
        "西甲 2024-2025": {"season_id": "11820", "league_id": "24"},
        "意甲 2024-2025": {"season_id": "11962", "league_id": "73"},
    }

    # TODO 更新最新数据到数据库，包括 1、更新已有比赛赔率 2、加入最新比赛赔率
    # 获取一段时期的全部gmMatchId
    # match_ids, match_dates, match_bcs = crawl_match_ids(
    #     start_date, end_date, leagues["西甲 2024-2025"]["season_id"], leagues["西甲 2024-2025"]["league_id"]
    # )

    # 获取五大联赛的全部gmMatchId
    match_ids, match_dates, match_bcs = crawl_five_league_match_ids(
        start_date, end_date, leagues
    )
    print(len(match_ids), len(match_dates), len(match_bcs))
    # 根据获取的match_id爬取赔率数据（有些赔率数据有缺失，需要丢弃）
    all_data, remove_idx = crawl_match_bet(match_ids)
    new_match_ids = [item for i, item in enumerate(match_ids) if i not in remove_idx]
    new_match_dates = [
        item for i, item in enumerate(match_dates) if i not in remove_idx
    ]
    new_match_bcs = [item for i, item in enumerate(match_bcs) if i not in remove_idx]
    print(f"获取到的比赛 ID: {new_match_ids}")
    print(f"获取到的比赛日期: {new_match_dates}")
    print(f"获取到的比赛半场比分:\n{new_match_bcs}")

    # 查询已有id
    cur_match_ids = query_match_ids_by_date_range(start_date, end_date)
    print(f"已有比赛id{cur_match_ids}")

    insert_or_update(all_data, new_match_ids, new_match_dates, cur_match_ids)

if __name__ == "__main__":
    crawl_insert_newest_match()
