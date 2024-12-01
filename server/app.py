import os
import asyncio
import datetime
import json
import random
import secrets
import string
import logging
from flask import Flask, jsonify, request, render_template
import pymysql
from pymysql.cursors import DictCursor
from dotenv import load_dotenv

load_dotenv()

db_list = []

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# 데이터베이스 연결 설정
def get_db_connection():
    return pymysql.connect(
        user=os.getenv('DB_USER', 'default_user'),
        password=os.getenv('DB_PASSWORD', 'default_password'),
        host=os.getenv('DB_HOST', '127.0.0.1'),
        port=int(os.getenv('DB_PORT', 3306)),
        database=os.getenv('DB_NAME', 'main_db'),
        charset='utf8',
        cursorclass=DictCursor
    )


def db_Job_List(sql, num):
    db_list.put((sql, num))

async def do_db_Job_list():
    while True:
        if not db_list.empty():
            sql, num = db_list.get()
            if num == 0:  # insert일 경우
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(sql)
                        conn.commit()
        await asyncio.sleep(0.2)



# 만료된 작업을 job_information_after 테이블로 이동하고 job_information 테이블에서 삭제
def check_data():
    now = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    sql_insert = "INSERT INTO job_information_after SELECT * FROM job_information WHERE job_date < %s"
    sql_delete = "DELETE FROM job_information WHERE job_date < %s"

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_insert, (now,))
            cursor.execute(sql_delete, (now,))
            conn.commit()

# JSON 직렬화기
def json_default(value):
    if isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')

@app.route('/job_in', methods=['POST'])
def job_in():
    try:
        data = request.values
        job_date = datetime.datetime.strptime(data['job_date'], "%Y-%m-%d %H:%M:%S.%f").replace(microsecond=0, second=0)
        job_end_date = datetime.datetime.strptime(data['job_end_date'], "%Y-%m-%d %H:%M:%S.%f").replace(microsecond=0, second=0)
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        rand = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(random.randint(6, 10)))

        sql_check = "SELECT job_num FROM job_information WHERE job_num = %s"
        sql_insert = """
            INSERT INTO job_information
            (job_num, user_info, money, job_date, job_end_date, now_date, place_number, need_user_max, place, job_do, detail, mini_detail, in_user_cnt)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (rand, data['user'], data['pay'], job_date, job_end_date, now, data['place_number'], data['need_user_max'], data['place'], data['job_do'], data['detail'], '더디테일', 0)

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                while True:
                    cursor.execute(sql_check, (rand,))
                    if cursor.fetchone() is None:
                        break
                    rand = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(random.randint(6, 10)))
                cursor.execute(sql_insert, params)
                conn.commit()

        return "true"

    except Exception as e:
        logging.error(f"Error: {e}")
        return "false"

@app.route('/job_in/out_data', methods=['POST'])
def get_date():
    try:
        user_id = request.values['user']
        sql = "SELECT * FROM job_information WHERE user_info = %s" if user_id != "all" else "SELECT * FROM job_information"
        sql_columns = """
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'job_information' 
            ORDER BY ORDINAL_POSITION
        """

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_columns, (os.getenv('DB_NAME'),))
                columns = [col['COLUMN_NAME'] for col in cursor.fetchall()]
                columns.pop(1)

                cursor.execute(sql, (user_id,) if user_id != "all" else None)
                rows = cursor.fetchall()

        result = {col: {i: str(row[col]) for i, row in enumerate(rows)} for col in columns}
        return json.dumps(result, default=json_default, ensure_ascii=False)

    except Exception as e:
        logging.error(f"Error: {e}")
        return "에러1"


@app.route('/job_in/get_worker_data', methods=['POST'])
def get_img_owner():
    try:
        num = request.values['num']
        user_id = request.values['user']
        logging.info(f"User ID: {user_id}, Num: {num}")

        if num == '1':
            sql = "SELECT work_path FROM user_info_owner WHERE user_id = %s"
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (user_id,))
                    result = cursor.fetchone()
            logging.info(f"Result: {result}")
            if result:
                return jsonify(result)
            else:
                return jsonify({'work_path': 'None'})

        elif num == '2':
            user_ids = request.values['users'].split("||")
            result_json = {col: [] for col in ['user_id', 'image_path', 'real_name']}
            sql = "SELECT user_id, image_path, real_name FROM user_info_worker WHERE user_id = %s"
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    for uid in user_ids:
                        cursor.execute(sql, (uid,))
                        result = cursor.fetchone()
                        if result:
                            for col in result_json:
                                result_json[col].append(result[col])
            return jsonify(result_json)

        elif num == '3':
            sql = "SELECT * FROM user_info_worker WHERE user_id = %s"
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (user_id,))
                    result = cursor.fetchall()
            print(result)
            if result:
                return jsonify(result[0])
            else:
                return jsonify([])
    except Exception as e:
        print(e)
        logging.error(f"Error: {e}")
        return "Error"

@app.route('/job_in/del_jobin_data', methods=['POST'])
def del_jobin_data():
    try:
        job_num = request.values['job_num']
        sql_insert = "INSERT INTO job_information_del SELECT * FROM job_information WHERE job_num = %s"
        sql_delete = "DELETE FROM job_information WHERE job_num = %s"

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_insert, (job_num,))
                cursor.execute(sql_delete, (job_num,))
                conn.commit()
        return "True"

    except Exception as e:
        logging.error(f"Error: {e}")
        return "False"

@app.route('/in_user_info', methods=['POST'])
def in_user_info():
    try:
        user_type = request.values['user']
        if user_type == 'owner':
            uid = request.values['uid']
            sql = "INSERT INTO user_info_owner(user_id) VALUES (%s)"
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (uid,))
                    conn.commit()
            return "True"
    except Exception as e:
        logging.error(f"Error: {e}")
        return "False"

@app.route('/job_in/set_image', methods=['POST'])
def set_image_owner():
    try:
        user_id = request.values['users']
        data = int(request.values['data'])
        f = request.files['image']
        image_path = os.path.join(os.getenv('IMAGE_PATH', 'C:/home_work_server/static/user_image/'), f'{user_id}.jpg')
        f.save(image_path)

        sql_select = "SELECT auth_data FROM user_info_owner WHERE user_id = %s"
        sql_update = "UPDATE user_info_owner SET auth_data = %s WHERE user_id = %s"

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_select, (user_id,))
                result = cursor.fetchone()
                current_auth = int(result['auth_data'])

                data_list = [current_auth % 2]
                while current_auth > 1:
                    current_auth //= 2
                    data_list.append(current_auth % 2)

                if len(data_list) < data:
                    new_auth = int(result['auth_data']) + 2 ** (data - 1)
                elif data_list[data - 1] == 0:
                    new_auth = int(result['auth_data']) + 2 ** (data - 1)
                else:
                    new_auth = int(result['auth_data'])

                cursor.execute(sql_update, (new_auth, user_id))
                conn.commit()
        return 'True'

    except Exception as e:
        logging.error(f"Error: {e}")
        return 'False'

@app.route('/owner_init', methods=['POST'])
def owner_init():
    try:
        user_id = request.values['users']
        sql = "SELECT auth_data FROM user_info_owner WHERE user_id = %s"

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (user_id,))
                result = cursor.fetchone()
        return jsonify({'auth': str(result['auth_data'])}) if result else jsonify({})

    except Exception as e:
        logging.error(f"Error: {e}")
        return jsonify({})

@app.route('/customer_find', methods=['POST', 'GET'])
def customer_find():
    try:
        jobnums = request.values['nums']
        jobnums_list = jobnums.split(',')

        results = []
        temp = []
        sql_job_info = "SELECT user_info FROM job_information WHERE job_num = %s"
        sql_user_name = "SELECT name FROM user_info_owner WHERE user_id = %s"

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                for num in jobnums_list:
                    cursor.execute(sql_job_info, (num.strip(),))
                    result = cursor.fetchone()
                    if result:
                        temp.append(result['user_info'])

                for user_id in temp:
                    cursor.execute(sql_user_name, (user_id.strip(),))
                    result = cursor.fetchone()
                    if result:
                        results.append(result['name'])
        return jsonify({'results': results})

    except Exception as e:
        logging.error(f"Error: {e}")
        return jsonify([])

@app.route('/worker_find', methods=['POST', 'GET'])
def worker_find():
    try:
        jobnums = request.values['nums']
        jobnums_list = jobnums.split(',')

        results = []
        temp = []
        sql_job_info = "SELECT user_info FROM job_information WHERE job_num = %s"
        sql_user_name = "SELECT name FROM user_info_owner WHERE user_id = %s"

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                for num in jobnums_list:
                    cursor.execute(sql_job_info, (num.strip(),))
                    result = cursor.fetchone()
                    if result:
                        temp.append(result['user_info'])

                for user_id in temp:
                    cursor.execute(sql_user_name, (user_id.strip(),))
                    result = cursor.fetchone()
                    if result:
                        results.append(result['name'])
        return jsonify({'results': results})

    except Exception as e:
        logging.error(f"Error: {e}")
        return jsonify([])

def update_job_information(job_num, user_id):
    print(job_num)
    print(user_id)
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            # job_num으로 해당 행을 찾음
            cursor.execute("SELECT in_user, in_user_cnt, job_date, place FROM job_information WHERE job_num = %s", (job_num,))
            result = cursor.fetchone()

            if result:
                current_in_user = result['in_user']
                current_in_user_cnt = result['in_user_cnt']
                job_date = result['job_date'].strftime('%Y-%m-%d')
                place = result['place']
                start_time = result['job_date'].strftime('%H:%M:%S')

                # 중복된 user_id가 있는지 확인
                if current_in_user and user_id in current_in_user.split('||'):
                    return {"status": "error", "message": "User ID already exists."}
                else:
                    # in_user 필드 업데이트
                    if current_in_user:
                        new_in_user = f"{current_in_user}||{user_id}"
                    else:
                        new_in_user = user_id

                    # in_user_cnt 필드 업데이트
                    new_in_user_cnt = current_in_user_cnt + 1

                    # 업데이트된 데이터를 job_information 테이블에 반영
                    cursor.execute("""
                        UPDATE job_information
                        SET in_user = %s, in_user_cnt = %s
                        WHERE job_num = %s
                    """, (new_in_user, new_in_user_cnt, job_num))

                    # user_info_worker 테이블의 job_apply 필드 업데이트
                    cursor.execute("SELECT job_apply FROM user_info_worker WHERE user_id = %s", (user_id,))
                    worker_result = cursor.fetchone()
                    if worker_result:
                        current_job_apply = worker_result['job_apply']
                        new_job_apply_entry = f"{job_date}|{place}|{start_time}"
                        if current_job_apply:
                            new_job_apply = f"{current_job_apply}||{new_job_apply_entry}"
                        else:
                            new_job_apply = new_job_apply_entry

                        cursor.execute("""
                            UPDATE user_info_worker
                            SET job_apply = %s
                            WHERE user_id = %s
                        """, (new_job_apply, user_id))

                    connection.commit()
                    return {"status": "success", "message": "Job information and user job application updated successfully."}
            else:
                return {"status": "error", "message": "Job not found."}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        connection.close()

@app.route('/job_in/apply', methods=['POST'])
def apply_for_job():
    try:
        job_num = request.values['job_num']
        user_id = request.values['user_id']
        result = update_job_information(job_num, user_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


def db_Job_List(sql, num):
    db_list.append([sql, num])

async def do_db_Job_lit():
    await asyncio.sleep(0.2)
    if db_list:
        data = db_list.pop()
        if data[1] == 0:  # insert일 경우
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(data[0])
                    conn.commit()


@app.route('/get_job_apply', methods=['POST'])
def get_job_apply():
    try:
        user_id = request.values['user_id']
        connection = get_db_connection()
        with connection.cursor() as cursor:
            # user_id로 job_apply 값을 검색
            cursor.execute("SELECT job_apply FROM user_info_worker WHERE user_id = %s", (user_id,))
            result = cursor.fetchone()

            if result:
                return jsonify({"status": "success", "job_apply": result['job_apply']})
            else:
                return jsonify({"status": "error", "message": "User ID not found."})
    except Exception as e:
        logging.error(f"Error: {e}")
        return jsonify({"status": "error", "message": str(e)})
    finally:
        connection.close()

@app.route('/view_jobs', methods=['GET'])
def view_jobs():
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT user_id, real_name, job_apply FROM user_info_worker")
            results = cursor.fetchall()

        for result in results:
            if result['job_apply']:
                jobs = result['job_apply'].split('||')
                jobs = [job.split('|') for job in jobs]
                result['jobs'] = jobs
            else:
                result['jobs'] = []
        print(results)
        return render_template('view_jobs.html', workers=results)
    except Exception as e:
        logging.error(f"Error: {e}")
        return jsonify({"status": "error", "message": str(e)})
    finally:
        connection.close()


@app.route('/move_to_money_receive', methods=['POST'])
def move_to_money_receive():
    try:
        data = request.json
        user_id = data['user_id']
        job_details = data['job_details']

        job_apply_clauses = [f"{job['date']}|{job['place']}|{job['start_time']}" for job in job_details]

        connection = get_db_connection()
        with connection.cursor() as cursor:
            # user_info_worker 테이블에서 job_apply를 업데이트하고 money_receive로 옮김
            cursor.execute("SELECT job_apply, money_receive FROM user_info_worker WHERE user_id = %s", (user_id,))
            result = cursor.fetchone()
            if result:
                current_job_apply = result['job_apply']
                current_money_receive = result['money_receive']

                # job_apply에서 해당 내용을 삭제
                new_job_apply = "||".join([ja for ja in current_job_apply.split('||') if ja not in job_apply_clauses])

                # money_receive에 해당 내용을 추가
                new_money_receive = current_money_receive
                for job in job_apply_clauses:
                    if new_money_receive:
                        new_money_receive += f"||{job}|입금완료"
                    else:
                        new_money_receive = f"{job}|입금완료"

                cursor.execute("UPDATE user_info_worker SET job_apply = %s, money_receive = %s WHERE user_id = %s",
                               (new_job_apply, new_money_receive, user_id))
                connection.commit()

        return jsonify({"status": "success"})
    except Exception as e:
        logging.error(f"Error: {e}")
        return jsonify({"status": "error", "message": str(e)})
    finally:
        connection.close()


@app.route('/get_money_receive', methods=['POST'])
def get_money_receive():
    global connection
    try:
        user_id = request.values['user_id']
        connection = get_db_connection()
        with connection.cursor() as cursor:
            # user_id로 money_receive 값을 검색
            cursor.execute("SELECT money_receive FROM user_info_worker WHERE user_id = %s", (user_id,))
            result = cursor.fetchone()

            if result:
                return jsonify({"status": "success", "money_receive": result['money_receive']})
            else:
                return jsonify({"status": "error", "message": "User ID not found."})
    except Exception as e:
        logging.error(f"Error: {e}")
        return jsonify({"status": "error", "message": str(e)})
    finally:
        connection.close()


@app.route('/update_event_push', methods=['POST'])
def update_event_push():
    try:
        user_id = request.values['user_id']
        event_push_value = request.values['event_push_value']

        # event_push_value를 int로 변환
        event_push_int = int(event_push_value)

        connection = get_db_connection()
        with connection.cursor() as cursor:
            # user_id로 검색하여 event_push 필드를 업데이트
            cursor.execute("SELECT * FROM user_info_worker WHERE user_id = %s", (user_id,))
            result = cursor.fetchone()

            if result:
                cursor.execute("UPDATE user_info_worker SET event_push = %s WHERE user_id = %s", (event_push_int, user_id))
                connection.commit()
                return jsonify({"status": "success", "message": "Event push updated successfully."})
            else:
                return jsonify({"status": "error", "message": "User ID not found."})
    except ValueError:
        return jsonify({"status": "error", "message": "Invalid event_push_value. It must be an integer."})
    except Exception as e:
        logging.error(f"Error: {e}")
        return jsonify({"status": "error", "message": str(e)})
    finally:
        connection.close()

@app.route('/get_event_push', methods=['POST'])
def get_event_push():
    try:
        user_id = request.values['user_id']
        connection = get_db_connection()
        with connection.cursor() as cursor:
            # user_id로 event_push 값을 검색
            cursor.execute("SELECT event_push FROM user_info_worker WHERE user_id = %s", (user_id,))
            result = cursor.fetchone()
            if result:
                return jsonify({"status": "success", "event_push": result['event_push']})
            else:
                return jsonify({"status": "error", "message": "User ID not found."})
    except Exception as e:
        logging.error(f"Error: {e}")
        return jsonify({"status": "error", "message": str(e)})
    finally:
        connection.close()


host_addr = "0.0.0.0"
port_num = int(os.getenv('FLASK_PORT', 5000))
if __name__ == '__main__':
    check_data()
    app.run(debug=os.getenv('FLASK_DEBUG', 'False') == 'True', host=host_addr, port=port_num)
