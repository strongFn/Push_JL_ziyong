from lxml import etree
import pandas as pd
import datetime as dt
import pickle
import requests
import os

class Teacher_Masg:

    def __init__(self):
        self.whose = ['巨锋','dream']
        self.gw_mingci = ['岗位需求表','岗位表','岗位', '职位']
        self.bt_mingci = ['岗位代码','招聘单位','招聘人数','学历','学位','专业要求','本科','研究生','招聘对象','职位']
        self.bufasong = ['汕头市','韶关市','湛江市','肇庆市','江门市','茂名市','梅州市','汕尾市','河源市','阳江市','潮州市','揭阳市','云浮市']
        self.bufa = ['龙门','清新区']

        self.all_masg = pd.DataFrame(columns=['date','place','year','title','url','备注','岗位表','链接','精确人数','总人数'])
        self.yifa_masg = pd.DataFrame(columns=self.all_masg.columns)
        self.error_masg = pd.DataFrame(columns=self.all_masg.columns)
        #self.data_cun_all()

        all_masg = self.data_qu('all')
        self.gengxinchucun(all_masg=all_masg)


    def gengxinchucun(self,all_masg):
        # today = dt.datetime.now().date()  # 获取当前日期
        today = dt.datetime(2024, 8, 2).date()
        if len(all_masg[0]) != 0:
            print('存档文件中存在数据')
            row = all_masg[0].loc[0]
            date0 = dt.datetime.strftime(row['date'], '%Y-%m-%d')
            date = dt.datetime.strptime(date0, '%Y-%m-%d').date()
            if date != today:
                print('正在更新储存文件')
                self.data_cun_all()
            else:
                print('存档中均为今日数据，无需清理缓存')
                print('检测是否存在新招聘信息...')
                self.all_masg = all_masg[0]
                self.yifa_masg = all_masg[1]
                self.error_masg = all_masg[2]
                self.paixu()

    def sendmsg(self,who,data):
        url = 'http://192.168.0.5:3001/webhook/msg/v2?token=FoWXl~fGA~Io'
        headers = {
            'Content-Type': 'application/json'
        }
        data = {
            "to": who,
            "data": {"content": data}
        }
        response = requests.post(url, headers=headers, json=data)
        print(response)

    def paixu(self):
        self.all_masg.sort_values(by=['date'], ascending=False, inplace=True)
        self.yifa_masg.sort_values(by=['date'], ascending=False, inplace=True)
        self.error_masg.sort_values(by=['date'], ascending=False, inplace=True)
        self.all_masg.reset_index(drop=True, inplace=True)
        self.yifa_masg.reset_index(drop=True, inplace=True)
        self.error_masg.reset_index(drop=True, inplace=True)

    #爬取单页的招聘信息列表
    def paqu_gg(self,url):
        basic_url = url
        r = requests.get(basic_url)
        html = etree.HTML(r.text)
        lis = html.xpath('/html/body/section/div/div[2]/div[1]/div[2]/div[1]/div')
        # today = dt.datetime.now().date()  # 获取当前日期
        today = dt.datetime(2024, 8, 2).date()
        for li in range(4,len(lis)):
            nianfen = None
            fen_url = 'https://guangdong.zhaojiao.net/zhaojiao/' + lis[li].xpath('./a/@href')[0]
            if fen_url in self.all_masg.values:
                continue
            difang = lis[li].xpath('./a/span[1]/i/text()')[0][0:3]
            riqia = lis[li].xpath('./a/span[3]/text()')[0]
            riqi = dt.datetime.strptime(riqia,'%Y-%m-%d')
            riqijiace = dt.datetime.strptime(riqia, '%Y-%m-%d').date()
            # 只处理日期与今天相同的项
            if riqijiace == today:
                nianfen_jiance = lis[li].xpath('./a/span[2]/text()')[0][0:4]
                if nianfen_jiance.isdigit():
                    nianfen = nianfen_jiance
                    biaoti = lis[li].xpath('./a/span[2]/text()')[0][4:]
                    if biaoti[0] == '年':
                        biaoti = biaoti.replace('年','')
                else:
                    biaoti = lis[li].xpath('./a/span[2]/text()')[0]
                aseries = pd.Series([riqi,difang,nianfen,biaoti,fen_url],index=['date','place','year','title','url'])
                self.all_masg.loc[len(self.all_masg)] = aseries

    def paqu_gg_remen(self):
        ceshi = pd.DataFrame(columns=self.all_masg.columns)
        basic_url ='https://guangdong.zhaojiao.net/zhaojiao/list-150-{}.html'.format('1')
        r = requests.get(basic_url)
        html = etree.HTML(r.text)
        lis = html.xpath('/html/body/section/div/div[2]/div[1]/div[2]/div[1]/div')

        for li in range(3):
            nianfen = None
            fen_url = 'https://guangdong.zhaojiao.net/zhaojiao/' + lis[li].xpath('./a/@href')[0]
            if fen_url in self.all_masg.values:
                continue
            difang = lis[li].xpath('./a/span[1]/i/text()')[0][0:3]
            today = dt.datetime.today()
            num = today.toordinal()
            todays = today.fromordinal(num)
            riqi = todays
            nianfen_jiance = lis[li].xpath('./a/span[2]/text()')[0][0:4]
            if nianfen_jiance.isdigit():
                nianfen = nianfen_jiance
                biaoti = lis[li].xpath('./a/span[2]/text()')[0][4:]
                if biaoti[0] == '年':
                    biaoti = biaoti.replace('年','')
            else:
                biaoti = lis[li].xpath('./a/span[2]/text()')[0]
            aseries = pd.Series([riqi,difang,nianfen,biaoti,fen_url],index=['date','place','year','title','url'])
            ceshi.loc[len(ceshi)] = aseries
            self.all_masg.loc[len(self.all_masg)] = aseries

    #基于paqu_gg爬取多页的招聘信息列表
    def paqu_ggs(self,page):
        #self.paqu_gg_remen()
        for i in range(1,page + 1):
            url = 'https://guangdong.zhaojiao.net/zhaojiao/list-150-{}.html'.format(i)
            self.paqu_gg(url)

        self.all_masg.sort_values(by=['date'],ascending=False,inplace=True)
        self.all_masg.reset_index(drop=True, inplace=True)
        #return zong_massage

    #判断字符串是否在列表的字符串中
    def is_in_list(self,source_data,list_name):
        for field in list_name:
            if str(source_data) in str(field).replace('\n',''):
                return True
        return False

    def is_in_list1(self,source_data,list_name):
        for field in list_name:
            if str(source_data) in str(field) :
                return True
        return False

    #判断两个列表的元素是否有相同
    def list_in_list(self,yanzheng_list,yuan_list):
        for i in yanzheng_list:
            for j in yuan_list:
                if str(i) in str(j).replace('\n',''):
                    #print(i,j)
                    return True
        return False

    #爬取单页的详细信息
    def paqu_xiangxi(self,url):
        global str_beizhu,gangweibiao,gw_url
        r = requests.get(url)
        html = etree.HTML(r.text)
        quxinxi = html.xpath(
            '/html/body/section/div/div[2]/div[1]/div[2]/div[1]/br[1]/following::p[position()<count(/html/body/section/div/div[2]/div[1]/div[2]/div[1]/br[1]/following::p)-count(/html/body/section/div/div[2]/div[1]/div[2]/div[1]/hr/following::p)+1]')
        beizhu = []
        for i in range(len(quxinxi)):
            xinxi = quxinxi[i].xpath('.//text()')
            if len(xinxi) == 0:
                continue
            beizhu.extend(xinxi)
        str_beizhu = ''
        for j in beizhu:
            str_beizhu = str_beizhu + j
        if len(str_beizhu) > 300 or len(str_beizhu) == 0:
            str_beizhu = '无备注信息'
        fujian = html.xpath('/html/body/section/div/div[2]/div[1]/div[2]/div[1]/p')
        gangweibiao = '无'
        gw_url = '无'
        for i in range(len(fujian)):
            if len(fujian[i].xpath('.//@href')) == 0:
                continue
            gangweipanbie = fujian[i].xpath('.//text()')
            if self.list_in_list(self.gw_mingci,gangweipanbie):
                gangweibiao = fujian[i].xpath('.//text()')[0]
                gw_url = fujian[i].xpath('.//@href')[0]
                break
        aseries = pd.Series([str_beizhu,gangweibiao,gw_url], index=['备注','岗位表','链接'])
        return aseries

    #基于paqu_xiangxi爬取多页详细信息
    def paqu_xingxis(self):
        urls = self.all_masg['url']
        for i in range(len(urls)):
            if pd.isnull(self.all_masg['备注'][i]):
                sr_xinxi = self.paqu_xiangxi(urls[i])
                self.all_masg.loc[i,['备注','岗位表','链接']] = sr_xinxi
        #return zong_xingxis

    #发送微信消息
    def send_masg(self,msg):
        for who in self.whose:
            self.sendmsg(who=who,data=msg)

    #整理发送的消息
    def masg_adjust(self,s,n):
        masg_all_list = []
        if len(self.all_masg) != 0:
            for i in range(s,n):
                if self.is_in_list(self.all_masg['place'][i],self.bufasong):
                    continue
                if self.list_in_list(self.bufa,[self.all_masg['title'][i]]):
                    continue
                if self.all_masg['url'][i] in self.yifa_masg.values:
                    continue
                self.yifa_masg.loc[len(self.yifa_masg)] = self.all_masg.loc[i]
                row = self.all_masg.loc[i]
                date = dt.datetime.strftime(row['date'],'%Y-%m-%d')
                palce = row['place']
                title = row['title']
                xiangxi_url = row['url']
                beizhu_masg = row['备注']
                gwei_url = row['链接']
                masg_all = f'''日期：{date}
地市：{palce}
标题：{title}
详细信息：{xiangxi_url}
备注信息：{beizhu_masg}
岗位表链接：{gwei_url}
'''
                masg_all_list.append(masg_all)
            return masg_all_list

    #获取未发的消息位置
    def get_weifa(self):
        if len(self.yifa_masg) != 0:
            for i in range(len(self.all_masg)):
                if self.all_masg.loc[i,'url'] == self.yifa_masg.loc[0,'url']:
                    return i
        else:
            return len(self.all_masg)

    #批量发送微信消息
    def piliang_send(self):
        n = self.get_weifa()
        masg_list = self.masg_adjust(0,n)
        if masg_list:
            for masg in masg_list:
                for who in self.whose:
                    self.sendmsg(who=who,data=masg)

    #存取数据
    def data_cun(self, masg, name):
        with open(f"./message{name}.txt", 'wb') as f:
            pickle.dump(masg, f)

    def data_qu(self, name):
        file_path = f"./message{name}.txt"
        # 检查文件是否存在
        if not os.path.exists(file_path):
            # 如果文件不存在，创建一个空白文件
            self.data_cun_all()
        else:
            # 正常打开文件并读取数据
            with open(file_path, 'rb') as f:
                r = pickle.load(f)
            return r
    def data_cun_all(self):
        all_masg = [self.all_masg,self.yifa_masg,self.error_masg]
        self.data_cun(all_masg,'all')


    def run_all(self):
        self.paqu_ggs(1)
        n = self.get_weifa()
        if n != 0:
            print('存在{}个新招聘信息'.format(len(self.all_masg) - n))
            self.paqu_xingxis()
            self.piliang_send()
        else:
            print('不存在新招聘信息')
        self.data_cun_all()

    #检查是否漏发
    def search_weifa(self):
        weifa_list = []
        weifamas_list = []
        yifa_url = self.yifa_masg['url']
        for i in range(0,8):
            if not self.is_in_list(self.all_masg.loc[i,'url'],yifa_url):
                weifa_list.append(i)
        if len(weifa_list) != 0:
            for j in weifa_list:
                masag = self.masg_adjust(j,j+1)
                weifamas_list.extend(masag)
        self.data_cun_all()
        #return weifamas_list
        if len(weifamas_list) != 0:
            for masg in weifamas_list:
                for who in self.whose:
                    self.sendmsg(who=who,data=masg)

if __name__ == '__main__':
    ceshi = Teacher_Masg()
    ceshi.run_all()
