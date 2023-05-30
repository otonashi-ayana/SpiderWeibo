import time, random, re, calendar, traceback
from threading import Thread
from queue import Queue
import requests, json
import pymysql
import pandas as pd
import numpy as np

global thread_status

key_words = ['俄罗斯', '乌克兰', '俄乌', '普京', '泽连斯基', '俄军', '乌军', '俄方', '乌方']  # 关键词
db = pymysql.connect(host='localhost', port=3306, user='root', passwd='root', db='database', charset='utf8mb4')
cursor = db.cursor()


# 读取目标博主列表
def read_blogger_list():
    data = pd.read_csv("blogger_list.csv")
    col_1 = np.array(data["blogger_id"])
    blg_id_list = []
    for i in col_1:
        blg_id_list.append(str(i))
    print(blg_id_list)
    return blg_id_list


# 随机睡眠时间
def random_sleep(mu=1, sig=0.4):
    sec = random.normalvariate(mu, sig)
    if sec <= 0:
        sec = mu
    time.sleep(sec)


# 格式化并返回日期
def date_format(object):
    date_split = object['created_at'].split(' ')
    year = date_split[5]
    mon = str(list(calendar.month_abbr).index(date_split[1]))
    day = date_split[2]
    date_formatted = year + '-' + mon + '-' + day

    return date_formatted


# 获得博主页个人信息
def user_info(user_id, attempts):
    data = {
        'custom': user_id,
    }
    headers = {
        'Referer': 'https://weibo.com/' + str(user_id),
        'Host': 'weibo.com',
        'Cookie': cookie,
        'Connection': 'close'
    }
    try:
        res = requests.get('https://weibo.com/ajax/profile/info', headers=headers, params=data).content.decode("utf-8")
        json_data = json.loads(res)
        print('已取得博主%s的user_info包' % (str(user_id)))
        return json_data
    except:
        attempts += 1
        print('【' + str(user_id) + '】''user_info() request error, trying again...', attempts)
        if attempts < 5:
            return user_info(user_id, attempts)
        else:
            print("连接似乎出现问题，数据采集终止")
            return None


# 将博主页信息写入数据库
def blogger_insert(blg_json):
    if blg_json['data']['user']['verified_type'] != -1:
        try:
            veri_info = blg_json['data']['user']['verified_reason']  # 认证信息（注意缺省）
        except:
            veri_info = ''
    else:
        veri_info = "Not verified"

    try:
        cursor.execute("INSERT INTO blogger_base("
                       "blogger_name,"
                       "blogger_id,"
                       "blogger_region,"
                       "blogger_fans,"
                       "blogger_follow,"
                       "blogger_gender,"
                       "blogger_weibo,"
                       "blogger_veri_type,"
                       "blogger_veri_info"
                       ") VALUES('%s','%s','%s',%s,%s,'%s',%s,%s,'%s'); "
                       % (blg_json['data']['user']['screen_name'],
                          blg_json['data']['user']['id'],
                          blg_json['data']['user']['location'],
                          blg_json['data']['user']['followers_count'],
                          blg_json['data']['user']['friends_count'],
                          blg_json['data']['user']['gender'],
                          blg_json['data']['user']['statuses_count'],
                          blg_json['data']['user']['verified_type'],
                          veri_info
                          )
                       )
        db.commit()
        print('博主信息已存入数据库')
    except Exception as e:
        print(f'博主信息存入数据库错误{e}')


# 主功能集成函数
def all_post_data(user_id):  # user_id的全体博文内容获取
    global thread_status
    thread_status = 1

    months = [1, 2, 3, 4]

    restartPoint = None
    while True:  # 生产者队列获取页面json失败时，重复该月json获取
        for curmonth in months:
            if restartPoint:
                if curmonth != restartPoint:
                    continue
            print('正在打包博主%s月的全部博文页...' % curmonth)
            task_queue = Queue()
            if post_page_list_producer(task_queue, user_id, curmonth) == -1:  # 队列一次性获得全部的当月博文页
                restartPoint = curmonth
                break

            inserted_list = []
            thread_list = []
            for index in range(20):
                consumer_thread = Thread(target=post_data_get_consumer, args=(task_queue, user_id, inserted_list))
                thread_list.append(consumer_thread)
            for t in thread_list:
                t.start()
            for t in thread_list:
                t.join()
            if thread_status == -1:
                print('有线程未完成任务，程序终止：检查用户 %s 的 %s 月博文数据' % (user_id, curmonth))
                exit()
            else:
                print('用户 %s 的 %s 月数据采集完成' % (user_id, curmonth))

            restartPoint = None
        if restartPoint is None:
            break


# 打包所有当月博文json
def post_page_list_producer(task_queue, user_id, curmonth):
    post_page = 1
    while True:
        # random_sleep()
        json_data = post_req(user_id, post_page, curmonth, retried_times=0)
        if json_data is None:
            print('全部博文包请求成功，完成打包')
            break
        elif json_data == -1:
            return -1
        else:
            task_queue.put(json_data)
            print('博文包请求成功', 'month:', curmonth, 'page:', post_page)
            post_page += 1
            random_sleep()


# 消化队列中的博文json
def post_data_get_consumer(task_queue, user_id, inserted_list):  # 多线程处理page的json_data
    global thread_status
    in_db = pymysql.connect(host='localhost', port=3306, user='root', passwd='root', db='dataday0224',
                            charset='utf8mb4')
    in_cursor = in_db.cursor()
    while task_queue.empty() is not True:
        json_data = task_queue.get()
        for object in json_data['data']['list']:
            date_formatted = date_format(object)
            if post_json_data_insert(user_id, object, in_db, in_cursor, inserted_list) == -1:
                print('由于本条博文数据插入失败，该线程结束')
                thread_status = -1
                return -1
            else:
                print(date_formatted+'is not wanted date')
        task_queue.task_done()


def post_json_data_insert(user_id, object, in_db, in_cursor, inserted_list):
    # 处理关键字并匹配
    pattern = re.compile('|'.join(key_words))
    text = object['text_raw']
    result_findall = pattern.findall(text)

    if result_findall and object['comments_count'] >= 10:
        if object['id'] not in inserted_list:
            date_formatted = date_format(object)
            if '<span class="expand">展开</span>' in object['text']:
                mblogid = object['mblogid']
                req = long_text_req(user_id, mblogid)
                print(object['text_raw'])
                object['text_raw'] = req['data']['longTextContent']
                print(object['text_raw'])
            if 'retweeted_status' in object:
                print("#################################################")
                print(object['retweeted_status']['user']['verified_type'])
                print(object['retweeted_status']['text_raw'])
                print("#################################################")
                in_cursor.execute(
                    "INSERT INTO blog_base("
                    "blog_date,"
                    "blog_text,"
                    "blog_id,"
                    "blog_comments,"
                    "blog_likes,"
                    "blog_keywords,"
                    "id_blogger,"
                    "retweeted_blogger_name,"
                    "retweeted_verified_type,"
                    "retweeted_text"
                    ") VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s); ",
                    (date_formatted,
                     object['text_raw'],
                     object['id'],
                     object['comments_count'],
                     object['attitudes_count'],
                     str(set(result_findall)).strip('{}'),
                     user_id,
                     object['retweeted_status']['user']['screen_name'],
                     object['retweeted_status']['user']['verified_type'],
                     object['retweeted_status']['text_raw']
                     )
                )
            else:  # 这是原创的
                in_cursor.execute(
                    "INSERT INTO blog_base("
                    "blog_date,"
                    "blog_text,"
                    "blog_id,"
                    "blog_comments,"
                    "blog_likes,"
                    "blog_keywords,"
                    "id_blogger"
                    ") VALUES(%s,%s,%s,%s,%s,%s,%s); ",
                    (date_formatted,
                     object['text_raw'],
                     object['id'],
                     object['comments_count'],
                     object['attitudes_count'],
                     str(set(result_findall)).strip('{}'),
                     user_id
                     )
                )
            in_db.commit()
            print('相关博文', object['text_raw'], '\n')
            counts = 0
            # 调用评论抓取函数
            if post_comment_data(user_id, object['id'], in_db, in_cursor, counts) is None:
                print('id%s 评论获得失败，删除该条博文数据、程序终止' % object['id'])
                in_cursor.execute("delete from comment_base where id_blog=%s" % object['id'])
                in_cursor.execute("delete from blog_base where blog_id=%s" % object['id'])
                in_cursor.commit()
                return -1
            inserted_list.append(object['id'])  # 记录该博文id防止重复
            return 1
        else:
            print('id%s 已存博文，跳过该条' % object['id'])
            return 1
    else:
        print('无关博文(comments_count:%s)' % object['comments_count'])
        return 1


# 请求单页博文页的函数
def post_req(user_id, page, curmonth, retried_times):
    headers = {
        'Host': 'weibo.com',
        'Referer': 'https://weibo.com/' + user_id + '?refer_flag=1001030103_',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0',
        'Cookie': cookie,
    }
    params = {
        'uid': user_id,
        'page': page,
        'feature': 0,
        'displayYear': 2023,
        'curMonth': curmonth,
        'stat_date': '2023' + str(curmonth).rjust(2, '0'),
    }
    if retried_times < 5:
        try:
            res = requests.get('https://weibo.com/ajax/statuses/mymblog', headers=headers,
                               params=params).content.decode('UTF-8')
        except:
            traceback.print_exc()
            random_sleep()
            retried_times += 1
            post_req(user_id, page, curmonth, retried_times)
        else:
            json_data = json.loads(res)  # 加载json文件
            if json_data['data']['list']:
                return json_data
            else:
                return None
    else:
        return -1


def long_text_req(bloggerID, mblogid):
    headers = {
        'Host': 'weibo.com',
        'Referer': 'https://weibo.com/' + bloggerID,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0',
        'Cookie': cookie,
    }
    res = requests.get('https://weibo.com/ajax/statuses/longtext?id=' + mblogid, headers=headers).content.decode(
        'UTF-8')
    json_data = json.loads(res)  # 加载json文件
    return json_data


# def post_data(user_id, page, curmonth):
#     headers = {
#         'Host': 'weibo.com',
#         'Referer': 'https://weibo.com/xinhuashidian',
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0',
#         'Cookie': cookie,
#         'Connection': 'close'
#
#     }
#     params = {
#         'uid': user_id,
#         'page': page,
#         'feature': 0,
#         'displayYear': 2022,
#         'curMonth': curmonth,
#         'stat_date': '2022' + str(curmonth).rjust(2, '0'),
#     }
#     try:
#         res = requests.get('https://weibo.com/ajax/statuses/mymblog', headers=headers, params=params,
#                            proxies=get_random_proxy()).content.decode('UTF-8')
#         json_data = json.loads(res)  # 加载json文件
#         if json_data['data']['list']:
#             # pool = Pool(10)
#             # params = [(user_id, x, x['text_raw']) for x in json_data['data']['list']]
#             #
#             # pool.map(post_pre_deal_packed, params)
#             # for object in json_data['data']['list']:
#             #     # post_pre_deal(user_id, object)
#             #     # 尝试该页的博文进行多线程post_pre_deal
#             return 1
#         else:
#             return 0
#     except:
#         print('*********************************************************************************')
#         print('*********************************************************************************')
#         print('【' + str(user_id), '[' + str(page) + ']' + '】''post_data() error, trying again...')
#         print('*********************************************************************************')
#         print('*********************************************************************************')
#
#         return post_data(user_id, page, curmonth)

# 评论（及用户信息）抓取

def post_comment_data_rest(user_id, post_id, max_id, in_db, in_cursor, counts):
    headers = {
        'Host': 'weibo.com',
        'Referer': 'https://weibo.com/' + str(user_id) + '?refer_flag=1001030103_',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0',
        'Cookie': cookie,
        # 'Connection': 'close'

    }
    params = {
        'flow': 1,
        'id': post_id,
        'is_show_bulletin': 2,
        'count': 20,
        'max_id': max_id,
    }
    try:
        res = requests.get('https://weibo.com/ajax/statuses/buildComments', headers=headers,
                           params=params).content.decode('UTF-8')

        json_data = json.loads(res)  # 加载json文件
    except:
        print('【' + str(post_id) + '】''post_comment_data_rest() request error, trying again...')
        traceback.print_exc()
        try:
            post_comment_data_rest(user_id, post_id, max_id, in_db, in_cursor, counts)
        except:
            print("retried failed")
            return None
    else:
        for object in json_data['data']:
            if counts >= 1000:
                break

            date_formatted = date_format(object)
            random_sleep()
            print('【', str(date_formatted), '[' + str(object['floor_number']) + ']】', '正在存储该条评论, postID:',
                  post_id)

            user_json = object['user']

            if user_json['verified_type'] != -1:
                try:
                    veri_info = user_json['verified_reason']  # 认证信息（注意缺省）
                except:
                    veri_info = ''
            else:
                veri_info = "Not verified"

            try:
                in_cursor.execute(
                    "INSERT INTO comment_base("
                    "comment_date,"
                    "comment_text,"
                    "commenter_name,"
                    "commenter_region,"
                    "commenter_id,"
                    "commenter_fans,"
                    "commenter_follow,"
                    "commenter_gender,"
                    "commenter_weibo,"
                    "commenter_veri_type,"
                    "commenter_veri_info,"
                    "id_blog"
                    ") VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",
                    (date_formatted,
                     object['text'],
                     object['user']['screen_name'],
                     user_json['location'],
                     object['user']['id'],
                     user_json['followers_count'],
                     user_json['friends_count'],
                     user_json['gender'],
                     user_json['statuses_count'],
                     user_json['verified_type'],
                     veri_info,
                     post_id
                     )
                )
                in_db.commit()
                counts += 1
            except:
                print('An sql error on post_comment_data_rest')
                print(object['text'])
                traceback.print_exc()

        if json_data['max_id'] != 0:
            random_sleep()
            return post_comment_data_rest(user_id, post_id, json_data['max_id'], in_db, in_cursor, counts)
        else:
            return 1


def post_comment_data(user_id, post_id, in_db, in_cursor, counts):
    headers = {
        'Host': 'weibo.com',
        'Referer': 'https://weibo.com/' + str(user_id) + '?refer_flag=1001030103_',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0',
        'Cookie': cookie,
    }
    params = {
        'id': post_id,
        'is_show_bulletin': 2,
        'count': 20,
        'flow': 1,
    }
    try:
        res = requests.get('https://weibo.com/ajax/statuses/buildComments', headers=headers,
                           params=params).content.decode('UTF-8')
        json_data = json.loads(res)  # 加载json文件
    except:
        print('【' + str(post_id) + '】''post_comment_data() request error, trying again...')
        traceback.print_exc()
        try:
            post_comment_data(user_id, post_id, in_db, in_cursor, counts)
        except:
            print("retried failed")
            return None
    else:
        for object in json_data['data']:
            date_formatted = date_format(object)
            random_sleep()
            print('【', str(date_formatted), '[' + str(object['floor_number']) + ']】', '正在存储该条评论, postID:',
                  post_id)

            user_json = object['user']

            if user_json['verified_type'] != -1:
                try:
                    veri_info = user_json['verified_reason']  # 认证信息（注意缺省）
                except:
                    veri_info = ''
            else:
                veri_info = "Not verified"

            try:
                in_cursor.execute(
                    "INSERT INTO comment_base("
                    "comment_date,"
                    "comment_text,"
                    "commenter_name,"
                    "commenter_region,"
                    "commenter_id,"
                    "commenter_fans,"
                    "commenter_follow,"
                    "commenter_gender,"
                    "commenter_weibo,"
                    "commenter_veri_type,"
                    "commenter_veri_info,"
                    "id_blog"
                    ") VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",
                    (date_formatted,
                     object['text'],
                     object['user']['screen_name'],
                     user_json['location'],
                     object['user']['id'],
                     user_json['followers_count'],
                     user_json['friends_count'],
                     user_json['gender'],
                     user_json['statuses_count'],
                     user_json['verified_type'],
                     veri_info,
                     post_id
                     )
                )
                in_db.commit()
                counts += 1
            except:
                print('An sql error on post_comment_data')
                print(object['text'])
                traceback.print_exc()

        if json_data['max_id'] != 0:
            random_sleep()
            return post_comment_data_rest(user_id, post_id, json_data['max_id'], in_db, in_cursor, counts)
        else:
            return 1


# def commenter_info_req(user_id, commenter_cookie, attempt_times):
#     data = {
#         'custom': user_id,
#     }
#     headers = {
#         'Referer': 'https://weibo.com/' + str(user_id),
#         'Host': 'weibo.com',
#         'Cookie': commenter_cookie,
#         'Connection': 'close',
#     }
#     try:
#         res = requests.get('https://weibo.com/ajax/profile/info', headers=headers, params=data,
#                            proxies=get_random_proxy()).content.decode("utf-8")
#         json_data = json.loads(res)
#         return json_data
#     except:
#         print('*********************************************************************************')
#         print('【用户id' + str(user_id) + '】''user_info() request error, trying again...')
#         print('*********************************************************************************')
#         attempt_times += 1
#         print(attempt_times)
#         if attempt_times < 3:
#             return commenter_info_req(user_id, commenter_cookie, attempt_times)
#         else:
#             # new_cookie_get()
#             return 0


# def commenter_info():
#     in_db = pymysql.connect(host='localhost', port=3306, user='root', passwd='0089Wittgenstein', db='data_base',
#                             charset='utf8mb4')
#     in_cursor = in_db.cursor()
#     in_cursor.execute("select blogger_name from blogger_base where id_blogger_base=1")
#     res = in_cursor.fetchone()  # 得到查询结果
#     print(res)

cookie = ''


def main():
    # 获取目标博主列表
    blg_id_list = read_blogger_list()
    cursor.execute("select blogger_id from blogger_base")
    existed_blogger = cursor.fetchall()

    for blg in blg_id_list:
        blgID = (blg,)
        if blgID not in existed_blogger:  # 未存入库的博主处理
            print('blogger not exists')
            attempts = 0
            blg_json = user_info(blg, attempts)

            if blg_json is None:
                print(blg, 'user_info() error')
                break
            blogger_insert(blg_json)
            all_post_data(blg)
        else:  # 已存入库的博主处理方式
            print('blogger exists')
            all_post_data(blg)


if __name__ == '__main__':
    main()

cursor.close()
db.close()
