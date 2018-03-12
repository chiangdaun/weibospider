import pymysql

def get_nickname():
    # 打开数据库连接
    db = pymysql.connect("localhost", "root", "123456", "sina",use_unicode=True, charset="utf8")
    cursor = db.cursor()# 使用 cursor() 方法创建一个游标对象 cursor
    sel = "select nick_name from nickname"
    cursor.execute(sel)#执行查询操作
    rs = cursor.fetchall()
    nick_name_list = []
    for i in range(len(rs)):
        nick_name_list.append(rs[i][0])
        #print(rs[i][0])
    db.commit()
    cursor.close()
    db.close()# 关闭数据库连接
    return nick_name_list

if __name__ == '__main__':
    nick_name_list = get_nickname()
    print(nick_name_list)