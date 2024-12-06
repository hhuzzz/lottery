from flask import Flask, jsonify, request
import config
from datetime import date, datetime
import pymysql
import dashscope
import re
from flask_apscheduler import APScheduler
from blueprints import user_bp
import fcntl

app = Flask(__name__)
app.register_blueprint(user_bp)
app.config.from_object(config)

# 添加任务
from crawler.crawl_insert_newest_match import crawl_insert_newest_match

def lock_func():
    f = open("scheduler.lock", "wb")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        crawl_insert_newest_match()
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        f.close()
    except:
        print('当前爬虫任务正在执行，直接跳过')


scheduler = APScheduler()
scheduler.init_app(app)
scheduler.add_job(
    id='crawl_insert_task',  # 任务 ID
    func=lock_func,  # 任务函数
    trigger='interval',  # 触发类型：间隔
    days=1,  # 每隔 1 天运行一次
    start_date='2024-11-28 14:04:00',  # 可选：任务开始时间
)
scheduler.start()


@app.route("/api/getAllCompetitions", methods=["GET"])
def get_all_matches():
    # 数据库连接配置
    connection = pymysql.connect(
        host="localhost",  # 数据库地址
        user="root",  # 用户名
        password="123456",  # 密码
        database="lottery",  # 数据库名称
        charset="utf8mb4",  # 字符集
    )
    # 获取当前日期（格式：YYYY-MM-DD）
    today = date.today().strftime("%Y-%m-%d")
    # 执行查询：查找当天的比赛
    cursor = connection.cursor(pymysql.cursors.DictCursor)  # 使用字典游标获取字段名作为键
    query = """
        SELECT t1.*, t2.h, t2.a, t2.d,
        t3.h AS rh, t3.a AS ra, t3.d AS rd,
        t4.aa, t4.ad, t4.ah, t4.da, t4.dd, t4.dh, t4.ha, t4.hd, t4.hh,
        t5.s0, t5.s1, t5.s2, t5.s3, t5.s4, t5.s5, t5.s6, t5.s7,
        t6.s01s00, t6.s02s00, t6.s02s01, t6.s03s00, t6.s03s01, t6.s03s02, t6.s04s00, t6.s04s01, t6.s04s02,
        t6.s05s00, t6.s05s01, t6.s05s02, t6.s_1sh,
        t6.s00s00, t6.s01s01, t6.s02s02, t6.s03s03, t6.s_1sd,
        t6.s00s01, t6.s00s02, t6.s01s02, t6.s00s03, t6.s01s03, t6.s02s03, t6.s00s04, t6.s01s04, t6.s02s04,
        t6.s00s05, t6.s01s05, t6.s02s05, t6.s_1sa
        FROM match_result t1
        JOIN match_had t2 ON t1.match_id = t2.match_id
        JOIN match_hhad t3 ON t1.match_id = t3.match_id
        JOIN match_hafu t4 ON t1.match_id = t4.match_id
        JOIN match_ttg t5 ON t1.match_id = t5.match_id
        JOIN match_crs t6 ON t1.match_id = t6.match_id
        WHERE date = %s
    """
    cursor.execute(query, ("2024-11-23"))
    # cursor.execute(query, (today))
    matches = cursor.fetchall()  # 获取所有匹配数据
    # print(type(matches), type(matches[0]))
    # 关闭数据库连接
    cursor.close()
    connection.close()

    # 如果没有找到数据，返回一个空列表
    if not matches:
        # return jsonify({"message": "No matches found for today."}), 404
        return jsonify({"data":{}, "msg":"未查询到今日比赛", "code":200})

    msg = [['主队','客队','让球胜平负','半全场胜平负','比分', '非让球胜','非让球负','非让球平','让球胜','让球负','让球平',
            '负负','负平','负胜','平负','平平','平胜','胜负','胜平','胜胜','总进球0','总进球1','总进球2','总进球3','总进球4','总进球5','总进球6','总进球7+',
            '1:0','2:0','2:1','3:0','3:1','3:2','4:0','4:1','4:2','5:0','5:1','5:2','胜其它','0:0','1:1','2:2','3:3','平其它','0:1','0:2','1:2','0:3','1:3','2:3','0:4','1:4','2:4','0:5','1:5','2:5','负其它']]
    for match in matches:
        match["date"] = match["date"].strftime("%Y-%m-%d")
        tmp = []
        for k, v in match.items():
            if k == 'match_id' or k == 'date':
                continue
            tmp.append(v)
        msg.append(tmp)
    
    # 返回比赛信息
    # return jsonify({"matches": msg})
    return jsonify({"data":{"matches": msg}, "msg":"查询成功", "code":200})


@app.route("/api/getCompetitionDetails", methods=["GET"])
def get_comp_details():
    # 数据库连接配置
    connection = pymysql.connect(
        host="localhost",  # 数据库地址
        user="root",  # 用户名
        password="123456",  # 密码
        database="lottery",  # 数据库名称
        charset="utf8mb4",  # 字符集
    )
    team1 = request.args.get("team1")
    team2 = request.args.get("team2")
    if (team1 == None and team2 == None) or (team1 == None and team2 != None):
        return jsonify({"data":{}, "msg":"未提供队伍名称", "code":400})
    # 执行查询：查找当天的比赛
    cursor = connection.cursor(pymysql.cursors.DictCursor)  # 使用字典游标获取字段名作为键
    if team1 != None and team2 == None:
        query = """
            SELECT t1.team_home, t1.team_away, t1.hhad, t1.hafu, t1.crs, t1.date
            FROM match_result t1
            WHERE t1.team_home = %s OR t1.team_away = %s
            ORDER BY t1.date DESC;
        """
        cursor.execute(query, (team1, team1))
        matches = cursor.fetchall()  # 获取所有匹配数据
        if not matches:
            return jsonify({"data":{}, "msg":"未找到该队伍比赛记录", "code":500})
    else:
        query = """
            SELECT t1.team_home, t1.team_away, t1.hhad, t1.hafu, t1.crs, t1.date
            FROM match_result t1
            WHERE t1.team_home = %s AND t1.team_away = %s OR t1.team_home = %s AND t1.team_away = %s
            ORDER BY t1.date DESC;
        """
        cursor.execute(query, (team1, team2, team2, team1))
        matches = cursor.fetchall()  # 获取所有匹配数据
        if not matches:
            return jsonify({"data":{}, "msg":"未找到两支队伍比赛记录", "code":500})
    # 关闭数据库连接
    cursor.close()
    connection.close()

    msg = [['主队','客队','让球胜平负','半全场胜平负','总比分','比赛日期']]
    for match in matches:
        match["date"] = match["date"].strftime("%Y-%m-%d")
        nums = match["crs"].split(":")
        # match["total"] = int(nums[0]) + int(nums[1])
        tmp = []
        for _, v in match.items():
            tmp.append(v)
        msg.append(tmp)

    # 返回比赛信息
    return jsonify({"data":{"matches": msg}, "msg":"查询成功", "code":200})


@app.route("/api/getSuggestions", methods=["POST"])
def get_suggestion():
    # 获取请求体数据
    data = request.get_json()
    team1 = data["team1"]
    team2 = data["team2"]
    money = data["money"]

    # print(team1, team2)
    bet_data_prompt = get_match_data_prompt(team1, team2)
    print(bet_data_prompt)
    prompt = enhance_prompt(bet_data_prompt, money)
    messages = [{"role": "user", "content": prompt}]
    response = dashscope.Generation.call(
        api_key="sk-2447021f124c4e2c92d92164653db434",
        model="qwen-max-2024-09-19",
        messages=messages,
        result_format="message",
        enable_search=True,
    )

    if response.status_code != 200:
        return jsonify({"data":{}, "msg":"大模型api出错", "code":500})

    def re_extract(response):
        # 正则表达式提取信息
        suggestion = response["output"]["choices"][0]["message"]["content"]
        # print(f'suggestion:{suggestion}')
        result_parrern = r'%%(.*?)%%'
        result_matches = re.search(result_parrern,suggestion)
        
        # 使用正则表达式提取括号中的内容和购买金额
        pattern = r"【.*?\((.*?)\)，购买金额：(.*?)元】"
        list_matches = re.findall(pattern, result_matches[0])
        # 判断总金额是否正确
        total = sum([int(match[1]) for match in list_matches])
        if total != money:
            return jsonify({"data":{}, "msg":"模型出错，投注金额不对", "code":200})
        # 将提取出来的内容构造为所需的格式
        result = [f"{match[0]}，购买金额：{match[1]}元" for match in list_matches]
        print(type(result), result)
        return result

    ans = re_extract(response)
    # print('hello')
    return jsonify({"data":{"suggestion": ans}, "msg":"大模型输出成功", "code":200})


# 根据球队名获取最新赔率数据
def get_match_data_prompt(team1, team2):
    # 数据库连接配置
    connection = pymysql.connect(
        host="localhost",  # 数据库地址
        user="root",  # 用户名
        password="123456",  # 密码
        database="lottery",  # 数据库名称
        charset="utf8mb4",  # 字符集
    )
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    query_get_match_id = """
        SELECT match_id 
        FROM match_result t1
        WHERE t1.team_home = %s AND t1.team_away = %s
        ORDER BY t1.date DESC
        LIMIT 1;
    """
    cursor.execute(query_get_match_id, (team1, team2))
    match_id = cursor.fetchall()[0]["match_id"]
    query_get_match_data = """
        SELECT t2.h, t2.a, t2.d,
        t3.h AS rh, t3.a AS ra, t3.d AS rd,
        t4.aa, t4.ad, t4.ah, t4.da, t4.dd, t4.dh, t4.ha, t4.hd, t4.hh,
        t5.s0, t5.s1, t5.s2, t5.s3, t5.s4, t5.s5, t5.s6, t5.s7,
        t6.s01s00, t6.s02s00, t6.s02s01, t6.s03s00, t6.s03s01, t6.s03s02, t6.s04s00, t6.s04s01, t6.s04s02,
        t6.s05s00, t6.s05s01, t6.s05s02, t6.s_1sh,
        t6.s00s00, t6.s01s01, t6.s02s02, t6.s03s03, t6.s_1sd,
        t6.s00s01, t6.s00s02, t6.s01s02, t6.s00s03, t6.s01s03, t6.s02s03, t6.s00s04, t6.s01s04, t6.s02s04,
        t6.s00s05, t6.s01s05, t6.s02s05, t6.s_1sa
        FROM match_had t2
        JOIN match_hhad t3 ON t2.match_id = t3.match_id
        JOIN match_hafu t4 ON t2.match_id = t4.match_id
        JOIN match_ttg t5 ON t2.match_id = t5.match_id
        JOIN match_crs t6 ON t2.match_id = t6.match_id
        WHERE t2.match_id = %s;
    """
    cursor.execute(query_get_match_data, (match_id))
    data = cursor.fetchall()[0]
    cursor.close()
    connection.close()

    def data2prompt(team1, team2):
        prompt = f"主球队1:{team1}\n客球队2:{team2}\n"
        # 非让球
        prompt += f"玩法1：非让球胜，对应的赔率： {data['h']}\n玩法2：非让球平，对应的赔率： {data['d']}\n玩法3：非让球负，对应的赔率： {data['a']}\n"
        # 让球
        prompt += f"玩法4：让球胜，对应的赔率： {data['rh']}\n玩法5：让球平，对应的赔率： {data['rd']}\n玩法6：让球负，对应的赔率： {data['ra']}\n"
        # 比分
        prompt += f"玩法7：1:0，对应的赔率： {data['s01s00']}\n玩法8：2:0，对应的赔率： {data['s02s00']}\n玩法9：2:1，对应的赔率： {data['s02s01']}\n"
        prompt += f"玩法10：3:0，对应的赔率： {data['s03s00']}\n玩法11：3:1，对应的赔率： {data['s03s01']}\n玩法12：3:2，对应的赔率： {data['s03s02']}\n"
        prompt += f"玩法13：4:0，对应的赔率： {data['s04s00']}\n玩法14：4:1，对应的赔率： {data['s04s01']}\n玩法15：4:2，对应的赔率： {data['s04s02']}\n"
        prompt += f"玩法16：5:0，对应的赔率： {data['s05s00']}\n玩法17：5:1，对应的赔率： {data['s05s01']}\n玩法18：5:2，对应的赔率： {data['s05s02']}\n"
        prompt += f"玩法19：胜其他，对应的赔率： {data['s_1sh']}\n玩法20：0:0，对应的赔率： {data['s00s00']}\n玩法21：1:1，对应的赔率： {data['s01s01']}\n"
        prompt += f"玩法22：2:2，对应的赔率： {data['s02s02']}\n玩法23：3:3，对应的赔率： {data['s03s03']}\n玩法24：平其他，对应的赔率： {data['s_1sd']}\n"
        prompt += f"玩法25：0:1，对应的赔率： {data['s00s01']}\n玩法26：0:2，对应的赔率： {data['s00s02']}\n玩法27：1:2，对应的赔率： {data['s01s02']}\n"
        prompt += f"玩法28：0:3，对应的赔率： {data['s00s03']}\n玩法29：1:3，对应的赔率： {data['s01s03']}\n玩法30：2:3，对应的赔率： {data['s02s03']}\n"
        prompt += f"玩法31：0:4，对应的赔率： {data['s00s04']}\n玩法32：1:4，对应的赔率： {data['s01s04']}\n玩法33：2:4，对应的赔率： {data['s02s04']}\n"
        prompt += f"玩法34：0:5，对应的赔率： {data['s00s05']}\n玩法35：1:5，对应的赔率： {data['s01s05']}\n玩法36：2:5，对应的赔率： {data['s02s05']}\n"
        prompt += f"玩法37：负其他，对应的赔率： {data['s_1sa']}\n"
        # 进球数
        prompt += f"玩法38：进球数：0，对应的赔率： {data['s0']}\n玩法39：进球数：1，对应的赔率： {data['s1']}\n玩法40：进球数：2，对应的赔率： {data['s2']}\n"
        prompt += f"玩法41：进球数：3，对应的赔率： {data['s3']}\n玩法42：进球数：4，对应的赔率： {data['s4']}\n玩法43：进球数：5，对应的赔率： {data['s5']}\n"
        prompt += f"玩法44：进球数：6，对应的赔率： {data['s6']}\n玩法45：进球数：7+，对应的赔率： {data['s7']}\n"
        # 半场胜平负
        prompt += f"玩法46：胜胜，对应的赔率： {data['hh']}\n玩法47：胜平，对应的赔率： {data['hd']}\n玩法48：胜负，对应的赔率： {data['ha']}\n"
        prompt += f"玩法49：平胜，对应的赔率： {data['dh']}\n玩法50：平平，对应的赔率： {data['dd']}\n玩法51：平负，对应的赔率： {data['da']}\n"
        prompt += f"玩法52：负胜，对应的赔率： {data['ah']}\n玩法53：负平，对应的赔率： {data['ad']}\n玩法54：负负，对应的赔率： {data['aa']}\n"
        return prompt
    # 将data转换成prompt
    prompt = data2prompt(team1, team2)
    return prompt




def enhance_prompt(bet_data_prompt, money):
    prompt = f"""
            请分析这个赔率，并结合2队的最近比赛数据，给我一个尽量不亏钱的高概率的投注组合，投注金额{money}元\n
以下为比赛的信息及赔率数据：{bet_data_prompt}\n

基于这些数据，我希望你提供一个系统性的投注建议，确保建议的科学性、合理性和高效性。请按以下步骤进行详细分析，最大化保本，按照每次50元投注，每个投注选项是2元的整数倍。\n

1. **赔率数据和隐含概率计算**：
   - 分析这场比赛的赔率高低，并根据隐含概率计算每个结果的发生概率。请解释各项隐含概率的计算步骤，并基于赔率分析出博彩公司对比赛结果的初步预测。
   - 若存在异常赔率或明显的赔率偏差，请详细说明这些赔率的含义，并分析可能存在的投注机会。\n

2. **算法与模式识别分析**：
   - 利用机器学习和数据挖掘的思路，提供可以识别赔率模式的策略，帮助判断哪些赔率组合更可能对应特定结果（例如热门选项、冷门结果）。
   - 请建议使用简单的分类算法（如逻辑回归、决策树）来识别有投注价值的赔率特征。如果可能，给出该算法的简单实现思路，并详细描述如何使用它识别投注机会。\n

3. **基于赔率的期望收益（EV）和投注策略**：
   - 基于隐含概率和赔率，计算每个结果的期望收益 (EV)，并判断投注是否具有正期望值。如果EV为正值，请提供一个合理的资金分配策略。
   - 提供具体的投注金额分配建议，并解释如何根据期望收益来优化投注金额，以降低风险并提高长期收益。\n

4. **主客场因素的模型分析**：
   - 基于主客场数据，建议一个回归模型（如线性回归或逻辑回归）来进一步优化主客场因素对比赛结果的影响分析。请解释如何利用主客场赔率来判断投注的机会，尤其是在主场优势或冷门客胜赔率存在时。\n
   
5. **风险管理与投注分散**：
   - 在推荐投注方向时，请给出合理的风险管理建议。建议一种分散投注策略，并解释如何在投注选项中分配资金，以保证收益风险比的合理性。
   - 提供小额冷门投注的策略（如平局或高赔率选项），并详细解释这种投注的预期收益与风险。\n

6. **基于历史数据的相似性分析**：
   - 若假设有过去类似赔率的比赛数据，使用 K 近邻算法（KNN）或其他相似性分析方法，帮助识别历史上相似赔率的结果分布。详细解释如何使用历史模式来提高预测的科学性，并判断是否有冷门投注的机会。\n
   
7. **结果回测与验证**：
   - 提供如何使用回测方法（如蒙特卡洛模拟或历史回测）评估该投注策略的长期表现。解释如何根据回测结果优化参数、调整策略以确保策略的稳定性和有效性。\n

8. **最终投注策略和步骤**：
   - 基于上述分析和模型计算，给出明确的投注建议，包括具体的投注方向、推荐金额分配，以及如何在多项选择中有效分散投注。
   - 请在每一步解释策略的逻辑，并建议如何根据市场数据的变化来调整投注方向。\n


请注意，输出的格式严格按照我的指示，我仅希望生成一组投注组合，你最后结论的投注组合用“%%”前后包围，该投注组合中的每一个投注方式用中文中括号包住，示例：%%【玩法+序号(玩法内容)，购买金额：】，【玩法+序号(玩法内容)，购买金额：】，【玩法+序号(玩法内容)，购买金额：】%%。
    """
    return prompt


# init(app)
# app.run(host='0.0.0.0', port=5000)

@app.route("/api/register", methods=["POST"])
def register():
    # 获取请求体数据
    data = request.get_json()
    team1 = data["team1"]
    team2 = data["team2"]
    money = data["money"]



if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
