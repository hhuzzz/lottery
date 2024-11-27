import pandas as pd
import pymysql


def get_data_list():
    # 读取Excel文件
    excel_file = "./crawler/汇总体彩.xlsx"  # 替换为你的Excel文件路径
    sheet_name = "Sheet1"  # 替换为你的表单名

    # 使用 pandas 读取Excel数据（第一行作为列名）
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    df = df.astype(str)

    match_result_list = []
    match_had_list = []
    match_hhad_list = []
    match_crs_list = []
    match_hafu_list = []
    match_ttg_list = []
    for _, row in df.iterrows():
        match_result = []
        match_result.append(row["matchid"])
        match_result.append(row["主球队1"])
        match_result.append(row["客球队2"])
        match_result.append(row["让球结果"])
        match_result.append(row["半场结果"])
        match_result.append(row["整场比分"])
        match_result.append(row["日期"])
        match_result_list.append(match_result)

        match_had = []
        match_had.append(row["matchid"])
        match_had.append(row["非让球胜"])
        match_had.append(row["非让球负"])
        match_had.append(row["非让球平"])
        match_had_list.append(match_had)

        match_hhad = []
        match_hhad.append(row["matchid"])
        match_hhad.append(row["让球胜"])
        match_hhad.append(row["让球负"])
        match_hhad.append(row["让球平"])
        match_hhad_list.append(match_hhad)

        match_crs = []
        match_crs.append(row["matchid"])
        match_crs.append(row["1:0"])
        match_crs.append(row["2:0"])
        match_crs.append(row["2:1"])
        match_crs.append(row["3:0"])
        match_crs.append(row["3:1"])
        match_crs.append(row["3:2"])
        match_crs.append(row["4:0"])
        match_crs.append(row["4:1"])
        match_crs.append(row["4:2"])
        match_crs.append(row["5:0"])
        match_crs.append(row["5:1"])
        match_crs.append(row["5:2"])
        match_crs.append(row["胜其他"])
        match_crs.append(row["0:0"])
        match_crs.append(row["1:1"])
        match_crs.append(row["2:2"])
        match_crs.append(row["3:3"])
        match_crs.append(row["平其他"])
        match_crs.append(row["0:1"])
        match_crs.append(row["0:2"])
        match_crs.append(row["1:2"])
        match_crs.append(row["0:3"])
        match_crs.append(row["1:3"])
        match_crs.append(row["2:3"])
        match_crs.append(row["0:4"])
        match_crs.append(row["1:4"])
        match_crs.append(row["2:4"])
        match_crs.append(row["0:5"])
        match_crs.append(row["1:5"])
        match_crs.append(row["2:5"])
        match_crs.append(row["负其他"])
        match_crs_list.append(match_crs)

        match_hafu = []
        match_hafu.append(row["matchid"])
        match_hafu.append(row["负负"])
        match_hafu.append(row["负平"])
        match_hafu.append(row["负胜"])
        match_hafu.append(row["平负"])
        match_hafu.append(row["平平"])
        match_hafu.append(row["平胜"])
        match_hafu.append(row["胜负"])
        match_hafu.append(row["胜平"])
        match_hafu.append(row["胜胜"])
        match_hafu_list.append(match_hafu)

        match_ttg = []
        match_ttg.append(row["matchid"])
        match_ttg.append(row["进球数：0"])
        match_ttg.append(row["进球数：1"])
        match_ttg.append(row["进球数：2"])
        match_ttg.append(row["进球数：3"])
        match_ttg.append(row["进球数：4"])
        match_ttg.append(row["进球数：5"])
        match_ttg.append(row["进球数：6"])
        match_ttg.append(row["进球数：7+"])
        match_ttg_list.append(match_ttg)

    return (
        match_result_list,
        match_had_list,
        match_hhad_list,
        match_crs_list,
        match_hafu_list,
        match_ttg_list,
    )


# def insert_database(
#     match_result_list,
#     match_had_list,
#     match_hhad_list,
#     match_crs_list,
#     match_hafu_list,
#     match_ttg_list,
# ):
#     # 数据库连接配置
#     connection = pymysql.connect(
#         host="localhost",  # 数据库地址
#         user="root",  # 用户名
#         password="root",  # 密码
#         database="lottery",  # 数据库名称
#         charset="utf8mb4",  # 字符集
#     )
#     # 插入比赛元信息到数据库
#     insert_match_result(match_result_list, connection)
#     # 插入胜平负到数据库
#     insert_match_had(match_had_list, connection)
#     # 插入让球胜平负到数据库
#     insert_match_hhad(match_hhad_list, connection)
#     # 插入比分
#     insert_match_crs(match_crs_list, connection)
#     # 半场胜平负
#     insert_match_hafu(match_hafu_list, connection)
#     # 总进球
#     insert_match_ttg(match_ttg_list, connection)
#     # 关闭数据库连接
#     connection.close()
#     pass

def insert_database(
    match_result_list,
    match_had_list,
    match_hhad_list,
    match_crs_list,
    match_hafu_list,
    match_ttg_list,
):
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
            for i in range(len(match_result_list)):
                insert_match_result = """
                    INSERT INTO match_result VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                print('正在插入result 数据')
                cursor.execute(insert_match_result, match_result_list[i])

                insert_match_had = """
                    INSERT INTO match_had VALUES (%s, %s, %s, %s)
                """
                print('正在插入had 数据')
                cursor.execute(insert_match_had, match_had_list[i])

                insert_match_hhad = """
                    INSERT INTO match_hhad VALUES (%s, %s, %s, %s)
                """
                print('正在插入hhad 数据')
                cursor.execute(insert_match_hhad, match_hhad_list[i])

                insert_match_hafu = """
                    INSERT INTO match_hafu VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                print('正在插入hafu 数据')
                cursor.execute(insert_match_hafu, match_hafu_list[i])

                insert_match_ttg = """
                    INSERT INTO match_ttg VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                print('正在插入ttg 数据')
                # cursor.execute(insert_match_ttg, [int(match_ttg_list[i][0])] + match_ttg_list[i][1:])
                cursor.execute(insert_match_ttg, match_ttg_list[i])

                insert_match_crs = """
                    INSERT INTO match_crs VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                print('正在插入crs 数据')
                cursor.execute(insert_match_crs, match_crs_list[i])
                


                connection.commit()


                print(f'插入第{i+1}条比赛信息成功，match_id={match_result_list[i][0]}')
    except Exception as e:
        print(f"插入/更新数据时出错: {e}")
        connection.rollback()
    finally:
        # 确保连接关闭
        cursor.close()
        connection.close()

def insert_match_result(match_result_list, connection):
    try:
        with connection.cursor() as cursor:
            for i, data in enumerate(match_result_list):
                # 插入数据的 SQL 语句
                sql = """
                    INSERT INTO match_result (match_id, team_home, team_away, hhad, hafu, crs, date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                # 执行插入操作
                # print(data)
                cursor.execute(sql, data)
                connection.commit()  # 提交事务
    except Exception as e:
        print(f"插入比赛结果数据时出错: {e}")
        connection.rollback()
        connection.close()


def insert_match_had(match_had_list, connection):
    try:
        with connection.cursor() as cursor:
            for i, data in enumerate(match_had_list):
                # 插入数据的 SQL 语句
                sql = """
                    INSERT INTO match_had (match_id, h, a, d)
                    VALUES (%s, %s, %s, %s)
                """
                # 执行插入操作
                # print(data)
                cursor.execute(sql, data)
                connection.commit()  # 提交事务
    except Exception as e:
        print(f"插入非让球胜平负数据时出错: {e}")
        connection.rollback()
        connection.close()


def insert_match_hhad(match_hhad_list, connection):
    try:
        with connection.cursor() as cursor:
            for i, data in enumerate(match_hhad_list):
                # 插入数据的 SQL 语句
                sql = """
                    INSERT INTO match_hhad (match_id, h, a, d)
                    VALUES (%s, %s, %s, %s)
                """
                # 执行插入操作
                cursor.execute(sql, data)
                
                connection.commit()  # 提交事务
    except Exception as e:
        print(f"插入让球胜平负数据时出错: {e}")
        connection.rollback()
        connection.close()


def insert_match_crs(match_crs_list, connection):
    try:
        with connection.cursor() as cursor:
            for i, data in enumerate(match_crs_list):
                # 插入数据的 SQL 语句
                sql = """
                    INSERT INTO match_crs
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                # 执行插入操作
                cursor.execute(sql, data)
                connection.commit()  # 提交事务
    except Exception as e:
        print(f"插入比分数据时出错: {e}")
        connection.rollback()
        connection.close()


def insert_match_hafu(match_hafu_list, connection):
    try:
        with connection.cursor() as cursor:
            for i, data in enumerate(match_hafu_list):
                # 插入数据的 SQL 语句
                sql = """
                    INSERT INTO match_hafu
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                # 执行插入操作
                print(data)
                cursor.execute(sql, data)
                connection.commit()  # 提交事务
                print(f'插入第{i+1}场比赛 半场胜平负 成功')
    except Exception as e:
        print(f"插入半场胜平负数据时出错: {e}")
        connection.rollback()
        connection.close()


def insert_match_ttg(match_ttg_list, connection):
    try:
        with connection.cursor() as cursor:
            for i, data in enumerate(match_ttg_list):
                # 插入数据的 SQL 语句
                sql = """
                    INSERT INTO match_ttg
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                # 执行插入操作
                cursor.execute(sql, data)
                connection.commit()  # 提交事务
                print(f'插入第{i+1}场比赛 总进球 成功')
    except Exception as e:
        print(f"插入总进球数据时出错: {e}")
        connection.rollback()
        connection.close()


if __name__ == "__main__":
    (
        match_result_list,
        match_had_list,
        match_hhad_list,
        match_crs_list,
        match_hafu_list,
        match_ttg_list,
    ) = get_data_list()
    insert_database(
        match_result_list,
        match_had_list,
        match_hhad_list,
        match_crs_list,
        match_hafu_list,
        match_ttg_list,
    )
