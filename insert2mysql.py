import pymysql

# 打开数据库连接
db = pymysql.connect("120.76.192.186", "root", "isearch.cs.swust", "isearch5",use_unicode=True, charset="utf8")

# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()

# 使用 execute()  方法执行 SQL 查询
cursor.execute("SELECT VERSION()")

# 使用 fetchone() 方法获取单条数据.
data = cursor.fetchone()

#输出数据库版本
print("Database version : %s " % data)

list = [
    '四川新闻网','网络新闻联播','广东新闻联播','汕头新闻联播','梅州新闻联播','湖南卫视新闻联播','新闻联播天气预报','西南民族大学学生会',
    '四川发布','四川旅游','四川交通','四川共青团','四川广汉三星堆博物馆','四川大学','西南科技大学','西南科技大学学生会','西南交通大学研究生会',
    '西南科技大学图书馆','西南科技大学新闻系','西南科技大学校园BBS','央视新闻','电子科技大学','西安电子科技大学','西南交通大学','西南石油大学',
    '西南财经大学','西南财经大学学生会','四川农业大学','四川农业大学学生会','成都理工大学','成都理工大学学生会','四川师范大学','西南民族大学',
    '成都中医药大学','西华大学','成都信息工程大学','西华师范大学','西南医科大学','四川旅游学院','四川文理学院','四川民族学院学生联合会',
    '成都文理学院','乐山师范学院','中国民用航空飞行学院','成都工业学院','四川理工学院','川北医学院','宜宾学院','成都医学院','内江师范学院',
    '绵阳师范学院','攀枝花学院','西昌学院学生会微博','成都师范学院',
    '北京大学','清华大学','浙江大学','复旦大学','中国人民大学',
]

for i in range(len(list)):
    # print(len(list))
    # print(list[i])
    insert_nickname = "insert into nickname(nick_name) values ('%s')" % (list[i])
    #print(insert_nickname)
    cursor.execute(insert_nickname)
    db.commit()
cursor.close()

# 关闭数据库连接
db.close()