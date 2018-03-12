import  sys
import os
import time

from subprocess import Popen

from scrapy.cmdline import execute

# sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# execute(['scrapy','crawl','weibo'])

while True:
    # sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    # execute(['scrapy','crawl','weibo'])
    os.system("scrapy crawl weibo")
    time.sleep(1530)