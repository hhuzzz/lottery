from flask import Blueprint, jsonify, request
import pymysql

bp = Blueprint('user', __name__, url_prefix='/api/user')

@bp.route('/register', methods=['POST'])
def register():
    # 数据库连接配置
    connection = pymysql.connect(
        host="localhost",  # 数据库地址
        user="root",  # 用户名
        password="123456",  # 密码
        database="lottery",  # 数据库名称
        charset="utf8mb4",  # 字符集
    )
    cursor = connection.cursor(pymysql.cursors.DictCursor)  # 使用字典游标获取字段名作为键

    try:
        # 获取用户传递的数据
        data = request.get_json()
        username = data['username']
        password = data['password']

        # 检查用户名是否已存在
        query_exist = """
            SELECT user_id FROM t_user
            WHERE username=%s AND del_flag = 0
        """
        cursor.execute(query_exist, (username,))
        existing_user = cursor.fetchone()  # 检查是否存在记录

        if existing_user:
            return jsonify({"data": {}, "msg": "用户已存在", "code": 200})

        # 插入新用户
        insert_user = """
            INSERT INTO t_user (username, password, del_flag)
            VALUES (%s, %s, 0)
        """
        cursor.execute(insert_user, (username, password))
        connection.commit()  # 提交事务

        return jsonify({"data": {"user_id": cursor.lastrowid}, "msg": "注册成功", "code": 200})

    except Exception as e:
        # 异常处理
        connection.rollback()
        return jsonify({"data": {}, "msg": f"注册失败: {str(e)}", "code": 500})
    
    finally:
        # 关闭资源
        cursor.close()
        connection.close()


@bp.route('/login', methods=['POST'])
def login():
    # 数据库连接配置
    connection = pymysql.connect(
        host="localhost",  # 数据库地址
        user="root",  # 用户名
        password="123456",  # 密码
        database="lottery",  # 数据库名称
        charset="utf8mb4",  # 字符集
    )
    cursor = connection.cursor(pymysql.cursors.DictCursor)  # 使用字典游标获取字段名作为键

    try:
        # 获取用户传递的数据
        data = request.get_json()
        username = data['username']
        password = data['password']

        # 检查用户名和密码是否匹配
        query_login = """
            SELECT user_id FROM t_user
            WHERE username=%s AND password=%s AND del_flag = 0
        """
        cursor.execute(query_login, (username, password))
        user = cursor.fetchone()  # 获取查询结果

        if user:
            return jsonify({"data": {"user_id": user['user_id']}, "msg": "登录成功", "code": 200})
        else:
            return jsonify({"data": {}, "msg": "用户名或密码错误", "code": 401})

    except Exception as e:
        # 异常处理
        return jsonify({"data": {}, "msg": f"登录失败: {str(e)}", "code": 500})
    
    finally:
        # 关闭资源
        cursor.close()
        connection.close()
