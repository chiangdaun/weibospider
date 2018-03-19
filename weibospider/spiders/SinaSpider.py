from scrapy import Spider,Request,FormRequest
import re
from scrapy import Selector
from weibospider.items import BaseInfoItem,BaseinfoMap,TweetsInfoItem,TweetsItem,FollowItem,FanItem
from urllib import parse
import time,json
import logging
import pymongo
import redis
import pymysql


class sinaSpider(Spider):
    name = "weibo"
    host = "https://weibo.cn"
    redis_key = "weiboSpider:start_urls"
    time = time.clock()
    infocount=0
    tweetscount=0
    requestcount=0
    rconn = redis.Redis()
    client = pymongo.MongoClient(host='202.115.161.209')
    auth = client.admin
    auth.authenticate('root', 'cs.swust')
    db = client['weibo-id2']

    def timed_task(self, dalay):
        if time.clock()-self.time >= dalay:
            self.time=time.clock()
            msg='获取用户信息{}条 ，获取微博{}条，向引擎提交请求{}次 '.format(self.infocount,self.tweetscount,self.requestcount)
            logging.info(msg)

    def start_requests(self):
        #uids=self.read_uid()
        uids = self.get_nickname()
        for uid in uids:
            result=self.db['users'].find_one({"NickName": uid}, {'_id': 0, 'NickName': 1})
            if not result:
                url='https://weibo.cn/search/'
                postdata={'keyword':uid,'suser':'找人'}
                yield FormRequest(url, formdata=postdata,callback=self.parse_userurl,meta={'uid':uid},priority=22,dont_filter=True)
                self.requestcount+=1
            else:
                uid=self.db['users'].find_one({"NickName":uid}, {'_id':0, 'Id': 1})
                uid=uid["Id"]
                url = 'https://weibo.cn/{}/info'.format(uid)
                yield Request(url, callback=self.parse_user_info,priority=22,dont_filter=True)

    def parse_userurl(self, response):
        selector=Selector(response)
        uid=response.meta.get('uid')
        xpathstr='//a[@href and contains(text(),"{}")]/@href'.format(uid)
        href=selector.xpath(xpathstr).extract_first()
        if href:
            url=parse.urljoin(response.url,href)
            yield Request(url,callback=self.parse_uid,priority=21,dont_filter=True)
        else:
            with open('idresult.txt', 'a', encoding='utf-8') as f:
                f.write(uid + '\n')

    def parse_uid(self, response):
        selector = Selector(response)
        href=selector.xpath('//a[@href and contains(text(),"资料")]/@href').extract_first()
        user_info_url=parse.urljoin(response.url,href)
        yield Request(user_info_url,callback=self.parse_user_info,priority=20,dont_filter=True)
        self.requestcount += 1

    def read_uid(self):#直接从文本读id
        uids=[]
        with open('.\weibospider\ids.txt', 'r',encoding='utf-8') as f:
            for line in f.readlines():
                if len(line) > 1:
                    line.strip()
                    uids.append(line.replace('\n', ''))
        return uids

    def get_nickname(self):#从数据库读id
        # 打开数据库连接
        db = pymysql.connect("120.76.192.186", "root", "XXXXXX", "isearch5", use_unicode=True, charset="utf8")
        cursor = db.cursor()  # 使用 cursor() 方法创建一个游标对象 cursor
        sel = "select nick_name from nickname"
        cursor.execute(sel)  # 执行查询操作
        rs = cursor.fetchall()
        nick_name_list = []
        for i in range(len(rs)):
            nick_name_list.append(rs[i][0])
            # print(rs[i][0])
        db.commit()
        cursor.close()
        db.close()  # 关闭数据库连接
        return nick_name_list

    def parse_user_info(self, response):
        self.timed_task(10)
        if response.status==200:
            selector=Selector(response)
            userinfo=BaseInfoItem()
            try:
                temp=re.search(r'(\d+)/info', response.url)
                if temp:
                    ID = temp.group(1)
                    infotext=";end".join(selector.xpath('body/div[@class="c"]//text()').extract())  # 获取标签里的所有text
                    for key in BaseinfoMap.keys():
                        value=BaseinfoMap.get(key)
                        temp=re.search('{}:(.*?);end'.format(value), infotext)
                        if temp:
                            userinfo[key]= temp[1]
                    Viplevel=re.search('会员等级.+?(\d{1,2})级\s+;end', infotext)
                    if Viplevel:
                        userinfo['Viplevel']=int(Viplevel[1])
                    else:
                        userinfo['Viplevel']=0
                    userinfo['Id']= ID
                    yield Request(url="https://weibo.cn/u/{}?page=1".format(ID), callback=self.parse_tweets,meta={'baseitem':userinfo,'nickname':userinfo['NickName']},dont_filter=True,priority=12)
                    #yield Request(url="https://weibo.cn/{}/follow".format(ID), callback=self.parse_relationship,meta={'info':'follow','id':ID,'list':[]}, dont_filter=True,priority=9)
                    #yield Request(url="https://weibo.cn/{}/fans".format(ID), callback=self.parse_relationship,meta={'info':'fans','id':ID,'list':[]}, dont_filter=True,priority=9)
                    self.requestcount+=1

            except Exception as e:
                logging.info(e)

    def parse_tweets(self, response):
        self.timed_task(10)
        if response.status==200:
            selector = Selector(response)
            max_crawl_page=20
            try:
                result=re.search(r'u/(\d+)', response.url)
                if result:
                    Id=result.group(1)
                    Nickname = response.meta.get('nickname')
                    current_page = int(re.search(r'page=(\d*)', response.url)[1])
                    if current_page == 1:                                        #抽取微博数量信息
                        item = response.meta['baseitem']

                        infotext=''.join(selector.xpath('//div[@class="tip2"]//text()').extract())
                        Tweets = re.search('微博\[(\d+)\]', infotext)[1]  # 微博数
                        Follows = re.search('关注\[(\d+)\]', infotext)[1]  # 关注数
                        Fans = re.search('粉丝\[(\d+)\]', infotext)[1]  # 粉丝数
                        for key in TweetsInfoItem.fields:
                            try:
                                item[key]=eval(key)
                            except NameError:
                                logging.info('Field is Not Defined', key)
                        yield item
                        self.infocount+=1


                    divs = selector.xpath('body/div[@class="c" and @id]')
                    for weibo in divs:
                        weiboitem=TweetsItem()
                        NickName=Nickname
                        id = Id+'-'+weibo.xpath('@id').extract_first()
                        IsTransfer = bool(weibo.xpath('.//span[@class="cmt"]').extract_first())
                        Content=''.join(weibo.xpath('.//span[@class="ctt"]//text()').extract())
                        Like=int(weibo.xpath('.//a[contains(text(), "赞[")]/text()').re_first('赞\[(.*?)\]'))
                        Transfer = int(weibo.xpath('.//a[contains(text(), "转发[")]/text()').re_first('转发\[(.*?)\]'))
                        # Comment = weibo.xpath('//a[contains(text(), "评论[") and not(contains(text(), "原文"))]//text()').re_first('评论\[(.*?)\]')
                        Comment = int(weibo.xpath('.//a[re:test(text(),"^评论\[")]/text()').re_first('评论\[(.*?)\]'))
                        timeandtools=weibo.xpath('div/span[@class="ct"]/text()')
                        if re.search('来自',''.join(timeandtools.extract())):          #有的微博网页发的 没有来自.....
                            PubTime=timeandtools.re_first('(.*?)\\xa0')
                            Tools=timeandtools.re_first('来自(.*)')
                        else:
                            PubTime=''.join(timeandtools.extract())
                            Tools=''
                        Co_oridinates=weibo.xpath('div/a[re:test(@href,"center=([\d.,]+)")]').re_first("center=([\d.,]+)")
                        for key in weiboitem.fields:
                            if key != 'CommentsList' and key != 'TransferList':
                                try:
                                    weiboitem[key] = eval(key)
                                except NameError:
                                    logging.info('Field is Not Defined', key)
                        if len(weiboitem['Content'])>=0:
                            commentlist = []
                            transferHref = weibo.xpath('.//a[re:test(text(),"^转发\[")]/@href').extract_first()
                            if weiboitem['Comment']>=1:
                                commentHref=weibo.xpath('.//a[re:test(text(),"^评论\[")]/@href').extract_first()
                                yield Request(url=commentHref, callback=self.parse_comments,
                                              meta={'weiboitem':weiboitem,'comments':commentlist,
                                                    'transferHref':transferHref},dont_filter=True,priority=28)
                            else:
                                if weiboitem['Transfer']>=1 and transferHref:
                                    yield Request(url=transferHref, callback=self.parse_transfer,
                                                  meta={'weiboitem': weiboitem, 'comments': commentlist,
                                                        }, dont_filter=True, priority=28)
                                else:
                                    weiboitem['CommentsList'] = []
                                    weiboitem['TransferList'] = []
                                    weiboitem['PubTime']=self.transfer_pubtime(weiboitem['PubTime'])
                                    yield weiboitem
                                    self.tweetscount += 1


                    if current_page<max_crawl_page:              #持续获取下一页直到max页面限制
                        next_page=selector.xpath('body/div[@class="pa" and @id="pagelist"]//a[contains(text(),"下页")]/@href').extract_first()
                        if next_page:
                            next_page=parse.urljoin(response.url,next_page)
                            yield Request(next_page, callback=self.parse_tweets,dont_filter=True,priority=13,meta={'nickname':Nickname})
                            self.requestcount+=1
            except Exception as e:
                logging.info(e)

    def transfer_pubtime(self,datetime):
        if re.match('\d+月\d+日', datetime):
            year = time.strftime('%Y', time.localtime())
            date = year + '年' + datetime
            datetime = date.replace(r'年', '-').replace(r'月', '-').replace(r'日', '') + ':00'
        if re.match('\d+分钟前', datetime):
            minute = re.match('(\d+)', datetime).group(1)
            datetime = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time() - float(minute) * 60)) + ':00'
        if re.match('今天.*', datetime):
            datetime = re.match('今天(.*)', datetime).group(1).strip()
            datetime = time.strftime("%Y-%m-%d", time.localtime()) + ' ' + datetime + ':00'
        return datetime

    def parse_comments(self,response):
        self.timed_task(10)
        selector=Selector(response)
        commentlist=response.meta.get('comments')
        commentsdiv=selector.xpath('//div[@class="c" and starts-with(@id,"C")]')
        for comment in commentsdiv:
            try:
                nickname=comment.xpath('./a/text()').extract_first()
                temp=comment.xpath('./a/@href').extract_first()
                uid=str(temp).split('/')[-1]
                content=''.join(comment.xpath('./span[@class="ctt"]//text()').extract())
                like=int(comment.xpath('.//a[contains(text(), "赞[")]/text()').re_first('赞\[(.*?)\]'))
                if content and len(content)>0:
                    commentlist.append({'name':nickname,'uid':uid,'comment':content,'like':like})
            except Exception as e:
                pass
        next_url = selector.xpath('//a[text()="下页"]/@href').extract_first()
        if next_url and len(commentlist)<3000 :
            next_url = parse.urljoin(response.url, next_url)
            response.meta['comments']=commentlist
            yield Request(next_url,priority=28, meta=response.meta,callback=self.parse_comments, dont_filter=True)
        else:
            weiboitem=response.meta.get('weiboitem')
            transferHref = response.meta.get('transferHref')
            if weiboitem['Transfer']>=1 and transferHref :
                yield Request(url=transferHref, callback=self.parse_transfer,
                              meta={'weiboitem': weiboitem, 'comments': commentlist,
                                    }, dont_filter=True, priority=29)
            else:
                weiboitem['TransferList']=[]
                weiboitem['CommentsList']=commentlist
                weiboitem['PubTime']=self.transfer_pubtime(weiboitem['PubTime'])
                yield weiboitem
                self.tweetscount += 1

    def parse_transfer(self, response):
        self.timed_task(10)
        transferlist=response.meta.get('transferlist',[])
        selector = Selector(response)
        transferdiv = selector.xpath('//div[@class="c" and not (@id)]')
        for div in transferdiv:
            text=''.join(div.xpath('.//text()').extract())
            if "来自" in text:
                try:
                    nickname = div.xpath('./a/text()').extract_first()
                    temp = div.xpath('./a/@href').extract_first()
                    uid = str(temp).split('/')[-1]
                    like = int(div.xpath('.//a[contains(text(), "赞[")]/text()').re_first('赞\[(.*?)\]'))
                    content=re.search(nickname+'\:(.*)赞\[.*',text).group(1)
                    if content and len(content) > 0:
                        transferlist.append({'name': nickname, 'uid': uid, 'content': content, 'like': like})
                except Exception as e:
                    pass
        next_url = selector.xpath('//a[text()="下页"]/@href').extract_first()
        if next_url and len(transferlist) < 3000:
            next_url = parse.urljoin(response.url, next_url)
            response.meta['transferlist']=transferlist
            yield Request(next_url, priority=30,meta=response.meta,callback=self.parse_transfer, dont_filter=True)
        else:
            weiboitem=response.meta.get('weiboitem')
            weiboitem['TransferList'] = transferlist
            weiboitem['CommentsList'] = response.meta.get('comments')
            weiboitem['PubTime']=self.transfer_pubtime(weiboitem['PubTime'])
            yield weiboitem
            self.tweetscount+=1

    def parse_relationship(self, response):
        if response.status==200:
            selector = Selector(response)
            try:
                urls = selector.xpath('//a[text()="关注他" or text()="关注她"]/@href').extract()
                uids = re.findall('uid=(\d+)', ";".join(urls), re.S)
                list=response.meta.get('list')
                rediskey = 'weibo:requests'
                count = self.rconn.zcard(rediskey)
                for uid in uids:
                    list.append(uid)
                    if int(count)<=10000:
                        yield Request(url="https://weibo.cn/{}/info".format(uid), callback=self.parse_user_info)
                        self.requestcount += 1
                next_url = selector.xpath('//a[text()="下页"]/@href').extract_first()
                info = response.meta.get('info')
                id = response.meta.get('id')
                if next_url:
                    next_url=parse.urljoin(response.url,next_url)
                    yield Request(next_url, callback=self.parse_relationship,meta={'info':info,'id':id,'list':list}, dont_filter=True,priority=11)
                    self.requestcount += 1
                else:
                    if info=='follow':relationitem=FollowItem()
                    elif info=='fans':relationitem=FanItem()
                    relationitem['Id']=id
                    relationitem['List']=list
                    yield relationitem

            except Exception as e:
                logging.info(e)



