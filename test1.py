import csv
import pandas as pd
import pymysql
import requests
from bs4 import BeautifulSoup
import schedule
import time


def message():
    schedule.every(1).seconds.do(message)

    conn = pymysql.connect(host='127.0.0.1', user='root', password='1234',

                           db='jspbookdb', charset='utf8')

    # -----------------------------------------------------------------------------------------
    # detect.py 동쪽
    curs = conn.cursor()
    curs.execute('TRUNCATE TABLE test')
    conn.commit()

    f = open('C:/yolov5-master/Ex.csv', 'r')

    csvReader = csv.reader(f)

    for row in csvReader:
        test_name = (row[0])
        test_count = (row[1])

        sql = """insert into test (test_name, test_count) values (%s, %s)"""

        curs.execute(sql, (test_name, test_count))

    # -----------------------------------------------------------------------------------------
    # detect1.py 서쪽
    curs10 = conn.cursor()
    curs10.execute('TRUNCATE TABLE test1')
    conn.commit()

    f10 = open('C:/yolov5-master/Ex1.csv', 'r')

    csvReader = csv.reader(f10)

    for row in csvReader:
        test1_name = (row[0])
        test1_count = (row[1])

        sql = """insert into test1 (test1_name, test1_count) values (%s, %s)"""

        curs10.execute(sql, (test1_name, test1_count))

    # -----------------------------------------------------------------------------------------
    # detect2.py 남쪽
    curs11 = conn.cursor()
    curs11.execute('TRUNCATE TABLE test2')
    conn.commit()

    f11 = open('C:/yolov5-master/Ex2.csv', 'r')

    csvReader = csv.reader(f11)

    for row in csvReader:
        test2_name = (row[0])
        test2_count = (row[1])

        sql = """insert into test2 (test2_name, test2_count) values (%s, %s)"""

        curs11.execute(sql, (test2_name, test2_count))

    # -----------------------------------------------------------------------------------------
    # detect3.py 북쪽
    curs12 = conn.cursor()
    curs12.execute('TRUNCATE TABLE test3')
    conn.commit()

    f12 = open('C:/yolov5-master/Ex3.csv', 'r')

    csvReader = csv.reader(f12)

    for row in csvReader:
        test3_name = (row[0])
        test3_count = (row[1])

        sql = """insert into test3 (test3_name, test3_count) values (%s, %s)"""

        curs12.execute(sql, (test3_name, test3_count))

    # -----------------------------------------------------------------------------------------
    # sum_count 동쪽
    df = pd.read_csv("C:/yolov5-master/Ex.csv", encoding='utf-8')
    df['sum_count'] = (df['count'] / 10) * 100
    df['wait'] = (df['count'] * 2)
    df.loc[(df['sum_count'] >= 0) & (df['sum_count'] <= 20), 'esnc'] = '매우 원활'
    df.loc[(df['sum_count'] > 20) & (df['sum_count'] <= 40), 'esnc'] = '원활'
    df.loc[(df['sum_count'] > 40) & (df['sum_count'] <= 60), 'esnc'] = '보통'
    df.loc[(df['sum_count'] > 60) & (df['sum_count'] <= 80), 'esnc'] = '혼잡'
    df.loc[(df['sum_count'] > 80), 'esnc'] = '매우 혼잡'

    df.to_csv("C:/yolov5-master/sum.csv", encoding='cp949')

    df1 = df.tail(1)
    df1.to_csv("C:/yolov5-master/sum1.csv", encoding='cp949')
    df1_1 = df1['wait']
    df1_1.to_csv("C:/yolov5-master/sum2.csv")

    curs13 = conn.cursor()
    curs13.execute('TABLE sum_count')
    conn.commit()

    f13 = open('C:/yolov5-master/sum.csv', 'r')

    csvReader = csv.reader(f13)

    for row in csvReader:
        sum_name = (row[1])
        s_count = (row[2])
        sum_count = (row[3])
        wait = (row[4])
        esnc = (row[5])

        sql = """insert into sum_count (sum_name, s_count, sum_count, wait, esnc) values (%s, %s, %s, %s, %s)"""

        curs13.execute(sql, (sum_name, s_count, sum_count, wait, esnc))

    curs14 = conn.cursor()
    curs14.execute('TABLE sum_count1')
    conn.commit()

    f14 = open('C:/yolov5-master/sum.csv', 'r')

    csvReader = csv.reader(f14)

    for row in csvReader:
        sum_name = (row[1])
        s_count = (row[2])
        sum_count = (row[3])
        wait = (row[4])
        esnc = (row[5])

        sql = """insert into sum_count1 (sum_name, s_count, sum_count, wait, esnc) values (%s, %s, %s, %s, %s)"""

        curs14.execute(sql, (sum_name, s_count, sum_count, wait, esnc))

    # ---------------------------------------------------------------------------------------------------
    # sum1_count 서쪽
    df10 = pd.read_csv("C:/yolov5-master/Ex1.csv", encoding='utf-8')
    df10['sum_count'] = (df10['count'] / 10) * 100
    df10['wait'] = (df10['count'] * 2)
    df10.loc[(df10['sum_count'] >= 0) & (df10['sum_count'] <= 20), 'wsnc'] = '매우 원활'
    df10.loc[(df10['sum_count'] > 20) & (df10['sum_count'] <= 40), 'wsnc'] = '원활'
    df10.loc[(df10['sum_count'] > 40) & (df10['sum_count'] <= 60), 'wsnc'] = '보통'
    df10.loc[(df10['sum_count'] > 60) & (df10['sum_count'] <= 80), 'wsnc'] = '혼잡'
    df10.loc[(df10['sum_count'] > 80), 'wsnc'] = '매우 혼잡'

    df10.to_csv("C:/yolov5-master/Ex_sum.csv", encoding='cp949')

    df11 = df10.tail(1)
    df11.to_csv("C:/yolov5-master/Ex_sum1.csv", encoding='cp949')
    df11_1 = df11['wait']
    df11_1.to_csv("C:/yolov5-master/Ex_sum2.csv")

    curs15 = conn.cursor()
    curs15.execute('TABLE Ex_sum_count')
    conn.commit()

    f15 = open('C:/yolov5-master/Ex_sum.csv', 'r')

    csvReader = csv.reader(f15)

    for row in csvReader:
        sum_name = (row[1])
        s_count = (row[2])
        sum_count = (row[3])
        wait = (row[4])
        wsnc = (row[5])

        sql = """insert into Ex_sum_count (sum_name, s_count, sum_count, wait, wsnc) values (%s, %s, %s, %s, %s)"""

        curs15.execute(sql, (sum_name, s_count, sum_count, wait, wsnc))

    curs16 = conn.cursor()
    curs16.execute('TABLE Ex_sum_count1')
    conn.commit()

    f16 = open('C:/yolov5-master/Ex_sum.csv', 'r')

    csvReader = csv.reader(f16)

    for row in csvReader:
        sum_name = (row[1])
        s_count = (row[2])
        sum_count = (row[3])
        wait = (row[4])
        wsnc = (row[5])

        sql = """insert into Ex_sum_count1 (sum_name, s_count, sum_count, wait, wsnc) values (%s, %s, %s, %s, %s)"""

        curs16.execute(sql, (sum_name, s_count, sum_count, wait, wsnc))
    # ---------------------------------------------------------------------------------------------------
    # sum2_count 남쪽
    df20 = pd.read_csv("C:/yolov5-master/Ex2.csv", encoding='utf-8')
    df20['sum_count'] = (df20['count'] / 10) * 100
    df20['wait'] = (df20['count'] * 2)
    df20.loc[(df20['sum_count'] >= 0) & (df20['sum_count'] <= 20), 'ssnc'] = '매우 원활'
    df20.loc[(df20['sum_count'] > 20) & (df20['sum_count'] <= 40), 'ssnc'] = '원활'
    df20.loc[(df20['sum_count'] > 40) & (df20['sum_count'] <= 60), 'ssnc'] = '보통'
    df20.loc[(df20['sum_count'] > 60) & (df20['sum_count'] <= 80), 'ssnc'] = '혼잡'
    df20.loc[(df20['sum_count'] > 80), 'ssnc'] = '매우 혼잡'

    df20.to_csv("C:/yolov5-master/Ex1_sum.csv", encoding='cp949')

    df21 = df20.tail(1)
    df21.to_csv("C:/yolov5-master/Ex1_sum1.csv", encoding='cp949')
    df21_1 = df21['wait']
    df21_1.to_csv("C:/yolov5-master/Ex1_sum2.csv")

    curs17 = conn.cursor()
    curs17.execute('TABLE Ex1_sum_count')
    conn.commit()

    f17 = open('C:/yolov5-master/Ex1_sum.csv', 'r')

    csvReader = csv.reader(f17)

    for row in csvReader:
        sum_name = (row[1])
        s_count = (row[2])
        sum_count = (row[3])
        wait = (row[4])
        ssnc = (row[5])

        sql = """insert into Ex1_sum_count (sum_name, s_count, sum_count, wait, ssnc) values (%s, %s, %s, %s, %s)"""

        curs17.execute(sql, (sum_name, s_count, sum_count, wait, ssnc))

    curs18 = conn.cursor()
    curs18.execute('TABLE Ex1_sum_count1')
    conn.commit()

    f18 = open('C:/yolov5-master/Ex1_sum.csv', 'r')

    csvReader = csv.reader(f18)

    for row in csvReader:
        sum_name = (row[1])
        s_count = (row[2])
        sum_count = (row[3])
        wait = (row[4])
        ssnc = (row[5])

        sql = """insert into Ex1_sum_count1 (sum_name, s_count, sum_count, wait, ssnc) values (%s, %s, %s, %s, %s)"""

        curs18.execute(sql, (sum_name, s_count, sum_count, wait, ssnc))
    # ---------------------------------------------------------------------------------------------------
    # sum3_count 북쪽
    df30 = pd.read_csv("C:/yolov5-master/Ex3.csv", encoding='utf-8')
    df30['sum_count'] = (df30['count'] / 10) * 100
    df30['wait'] = (df30['count'] * 2)
    df30.loc[(df30['sum_count'] >= 0) & (df30['sum_count'] <= 20), 'nsnc'] = '매우 원활'
    df30.loc[(df30['sum_count'] > 20) & (df30['sum_count'] <= 40), 'nsnc'] = '원활'
    df30.loc[(df30['sum_count'] > 40) & (df30['sum_count'] <= 60), 'nsnc'] = '보통'
    df30.loc[(df30['sum_count'] > 60) & (df30['sum_count'] <= 80), 'nsnc'] = '혼잡'
    df30.loc[(df30['sum_count'] > 80), 'nsnc'] = '매우 혼잡'

    df30.to_csv("C:/yolov5-master/Ex2_sum.csv", encoding='cp949')

    df31 = df30.tail(1)
    df31.to_csv("C:/yolov5-master/Ex2_sum1.csv", encoding='cp949')
    df31_1 = df31['wait']
    df31_1.to_csv("C:/yolov5-master/Ex2_sum2.csv")

    curs19 = conn.cursor()
    curs19.execute('TABLE Ex2_sum_count')
    conn.commit()

    f19 = open('C:/yolov5-master/Ex2_sum.csv', 'r')

    csvReader = csv.reader(f19)

    for row in csvReader:
        sum_name = (row[1])
        s_count = (row[2])
        sum_count = (row[3])
        wait = (row[4])
        nsnc = (row[5])

        sql = """insert into Ex2_sum_count (sum_name, s_count, sum_count, wait, nsnc) values (%s, %s, %s, %s, %s)"""

        curs19.execute(sql, (sum_name, s_count, sum_count, wait, nsnc))

    curs20 = conn.cursor()
    curs20.execute('TABLE Ex2_sum_count1')
    conn.commit()

    f20 = open('C:/yolov5-master/Ex2_sum.csv', 'r')

    csvReader = csv.reader(f20)

    for row in csvReader:
        sum_name = (row[1])
        s_count = (row[2])
        sum_count = (row[3])
        wait = (row[4])
        nsnc = (row[5])

        sql = """insert into Ex2_sum_count1 (sum_name, s_count, sum_count, wait, nsnc) values (%s, %s, %s, %s, %s)"""

        curs20.execute(sql, (sum_name, s_count, sum_count, wait, nsnc))
    # ---------------------------------------------------------------------------------------------------

    # weather.py
    html = requests.get('https://search.naver.com/search.naver?query=중랑구+날씨')
    soup = BeautifulSoup(html.text, 'html.parser')

    for i in range(25):
        # 위치
        address = soup.find('div', {'class': 'title_area _area_panel'}).find('h2', {'class': 'title'}).text

        # 날씨 정보
        weather_data = soup.find('div', {'class': 'weather_info'})

        # 현재 온도
        temperature = weather_data.find('div', {'class': 'temperature_text'}).text.strip()

        # 날씨 상태
        weather = weather_data.find('span', {'class': 'weather before_slash'}).text

        addresslist = []
        weatherlist = []
        templist = []
        weatherlist.append(weather)
        templist.append(temperature)
        addresslist.append(address)

    data = {"address": addresslist, "weather": weatherlist, "temperature": templist}
    df2 = pd.DataFrame(data)

    df2.to_csv("C:/yolov5-master/weather.csv", encoding="utf-8-sig")

    curs2 = conn.cursor()
    curs2.execute('TRUNCATE TABLE weather')
    conn.commit()

    f2 = open('C:/yolov5-master/weather.csv', 'rt', encoding='UTF8')

    csvReader = csv.reader(f2)

    for row in csvReader:
        address = (row[1])
        weather = (row[2])
        temperature = (row[3])
        sql = """insert into weather (address, weather, temperature) values (%s, %s, %s)"""

        curs2.execute(sql, (address, weather, temperature))

    # distance.py
    next = [2]
    opp = [4]

    df100 = pd.DataFrame(next, columns=['next'])
    df100['opp'] = opp

    df100.to_csv('C:/yolov5-master/distance.csv', index=False)

    # movedata.py
    new_weather = weather

    sunny = [1]
    night_sunny = [1]
    day_little_cloud = [1]
    night_little_cloud = [1]
    day_many_cloud = [1]
    night_many_cloud = [1]
    cloudy = [1]
    shower = [2]
    rain = [2]
    some_rain = [2]
    snow = [2]
    some_snow = [2]
    rain_or_snow = [2]
    some_rain_or_snow = [2]
    snow_or_rain = [2]
    some_snow_or_rain = [2]
    light = [2]
    raindrop = [2]
    haze = [2]
    blizzard = [2]
    fog = [1]
    mist = [1]
    yellow_dust = [1]

    df3 = pd.DataFrame(sunny, columns=['맑음'])
    df3['맑음 (밤)'] = night_sunny
    df3['구름조금 (낮)'] = day_little_cloud
    df3['구름조금'] = night_little_cloud
    df3['구름많음 (낮)'] = day_many_cloud
    df3['구름많음'] = night_many_cloud
    df3['흐림'] = cloudy
    df3['소나기'] = shower
    df3['비'] = rain
    df3['가끔 비, 한때 비'] = some_rain
    df3['눈'] = snow
    df3['가끔 눈, 한때 눈'] = some_snow
    df3['비 또는 눈'] = rain_or_snow
    df3['가끔 비 또는 눈, 한때 비 또는 눈'] = some_rain_or_snow
    df3['눈 또는 비'] = snow_or_rain
    df3['가끔 눈 또는 비, 한때 눈 또는 비'] = some_snow_or_rain
    df3['낙뢰'] = light
    df3['빗방울'] = raindrop
    df3['연무'] = haze
    df3['눈날림'] = blizzard
    df3['안개'] = fog
    df3['박무(엷은 안개)'] = mist
    df3['황사'] = yellow_dust

    df3.to_csv("C:/yolov5-master/movedata.csv", index=False, encoding='cp949')

    df40 = pd.read_csv("C:/yolov5-master/sum2.csv")
    df50 = pd.read_csv("C:/yolov5-master/Ex_sum2.csv")
    df60 = pd.read_csv("C:/yolov5-master/EX1_sum2.csv")
    df70 = pd.read_csv("C:/yolov5-master/Ex2_sum2.csv")
    df100_1 = pd.read_csv("C:/yolov5-master/distance.csv")
    if new_weather == "맑음":
        df4 = pd.DataFrame(sunny, columns=['맑음'])
        df40["wait"] = df40["wait"] + df4["맑음"]
        df50["wait"] = df50["wait"] + df4["맑음"]
        df60["wait"] = df60["wait"] + df4["맑음"]
        df70["wait"] = df70["wait"] + df4["맑음"]
        df100_1["next"] = df100_1["next"] + df4["맑음"]
        df100_1["opp"] = df100_1["opp"] + df4["맑음"]
    if new_weather == "맑음 (밤)":
        df4 = pd.DataFrame(night_sunny, columns=['맑음 (밤)'])
        df40["wait"] = df40["wait"] + df4["맑음 (밤)"]
        df50["wait"] = df50["wait"] + df4["맑음 (밤)"]
        df60["wait"] = df60["wait"] + df4["맑음 (밤)"]
        df70["wait"] = df70["wait"] + df4["맑음 (밤)"]
        df100_1["next"] = df100_1["next"] + df4["맑음 (밤)"]
        df100_1["opp"] = df100_1["opp"] + df4["맑음 (밤)"]
    if new_weather == "구름조금":
        df4 = pd.DataFrame(day_little_cloud, columns=['구름조금'])
        df40["wait"] = df40["wait"] + df4["구름조금"]
        df50["wait"] = df50["wait"] + df4["구름조금"]
        df60["wait"] = df60["wait"] + df4["구름조금"]
        df70["wait"] = df70["wait"] + df4["구름조금"]
        df100_1["next"] = df100_1["next"] + df4["구름조금"]
        df100_1["opp"] = df100_1["opp"] + df4["구름조금"]
    if new_weather == "구름조금 (밤)":
        df4 = pd.DataFrame(night_little_cloud, columns=['구름조금 (밤)'])
        df40["wait"] = df40["wait"] + df4["구름조금 (밤)"]
        df50["wait"] = df50["wait"] + df4["구름조금 (밤)"]
        df60["wait"] = df60["wait"] + df4["구름조금 (밤)"]
        df70["wait"] = df70["wait"] + df4["구름조금 (밤)"]
        df100_1["next"] = df100_1["next"] + df4["구름조금 (밤)"]
        df100_1["opp"] = df100_1["opp"] + df4["구름조금 (밤)"]
    if new_weather == "구름많음":
        df4 = pd.DataFrame(day_many_cloud, columns=['구름많음'])
        df40["wait"] = df40["wait"] + df4["구름많음"]
        df50["wait"] = df50["wait"] + df4["구름많음"]
        df60["wait"] = df60["wait"] + df4["구름많음"]
        df70["wait"] = df70["wait"] + df4["구름많음"]
        df100_1["next"] = df100_1["next"] + df4["구름많음"]
        df100_1["opp"] = df100_1["opp"] + df4["구름많음"]
    if new_weather == "구름많음 (밤)":
        df4 = pd.DataFrame(night_many_cloud, columns=['구름많음 (밤)'])
        df40["wait"] = df40["wait"] + df4["구름많음 (밤)"]
        df50["wait"] = df50["wait"] + df4["구름많음 (밤)"]
        df60["wait"] = df60["wait"] + df4["구름많음 (밤)"]
        df70["wait"] = df70["wait"] + df4["구름많음 (밤)"]
        df100_1["next"] = df100_1["next"] + df4["구름많음 (밤)"]
        df100_1["opp"] = df100_1["opp"] + df4["구름많음 (밤)"]
    if new_weather == "흐림":
        df4 = pd.DataFrame(cloudy, columns=['흐림'])
        df40["wait"] = df40["wait"] + df4["흐림"]
        df50["wait"] = df50["wait"] + df4["흐림"]
        df60["wait"] = df60["wait"] + df4["흐림"]
        df70["wait"] = df70["wait"] + df4["흐림"]
        df100_1["next"] = df100_1["next"] + df4["흐림"]
        df100_1["opp"] = df100_1["opp"] + df4["흐림"]
    if new_weather == "소나기":
        df4 = pd.DataFrame(shower, columns=['소나기'])
        df40["wait"] = df40["wait"] + df4["소나기"]
        df50["wait"] = df50["wait"] + df4["소나기"]
        df60["wait"] = df60["wait"] + df4["소나기"]
        df70["wait"] = df70["wait"] + df4["소나기"]
        df100_1["next"] = df100_1["next"] + df4["소나기"]
        df100_1["opp"] = df100_1["opp"] + df4["소나기"]
    if new_weather == "비":
        df4 = pd.DataFrame(rain, columns=['비'])
        df40["wait"] = df40["wait"] + df4["비"]
        df50["wait"] = df50["wait"] + df4["비"]
        df60["wait"] = df60["wait"] + df4["비"]
        df70["wait"] = df70["wait"] + df4["비"]
        df100_1["next"] = df100_1["next"] + df4["비"]
        df100_1["opp"] = df100_1["opp"] + df4["비"]
    if new_weather == "가끔 비, 한때 비":
        df4 = pd.DataFrame(some_rain, columns=['가끔 비, 한때 비'])
        df40["wait"] = df40["wait"] + df4["가끔 비, 한때 비"]
        df50["wait"] = df50["wait"] + df4["가끔 비, 한때 비"]
        df60["wait"] = df60["wait"] + df4["가끔 비, 한때 비"]
        df70["wait"] = df70["wait"] + df4["가끔 비, 한때 비"]
        df100_1["next"] = df100_1["next"] + df4["가끔 비, 한때 비"]
        df100_1["opp"] = df100_1["opp"] + df4["가끔 비, 한때 비"]
    if new_weather == "눈":
        df4 = pd.DataFrame(snow, columns=['눈'])
        df40["wait"] = df40["wait"] + df4["눈"]
        df50["wait"] = df50["wait"] + df4["눈"]
        df60["wait"] = df60["wait"] + df4["눈"]
        df70["wait"] = df70["wait"] + df4["눈"]
        df100_1["next"] = df100_1["next"] + df4["눈"]
        df100_1["opp"] = df100_1["opp"] + df4["눈"]
    if new_weather == "가끔 눈, 한때 눈":
        df4 = pd.DataFrame(some_snow, columns=['가끔 눈, 한때 눈'])
        df40["wait"] = df40["wait"] + df4["가끔 눈, 한때 눈"]
        df50["wait"] = df50["wait"] + df4["가끔 눈, 한때 눈"]
        df60["wait"] = df60["wait"] + df4["가끔 눈, 한때 눈"]
        df70["wait"] = df70["wait"] + df4["가끔 눈, 한때 눈"]
        df100_1["next"] = df100_1["next"] + df4["가끔 눈, 한때 눈"]
        df100_1["opp"] = df100_1["opp"] + df4["가끔 눈, 한때 눈"]
    if new_weather == "비 또는 눈":
        df4 = pd.DataFrame(rain_or_snow, columns=['비 또는 눈'])
        df40["wait"] = df40["wait"] + df4["비 또는 눈"]
        df50["wait"] = df50["wait"] + df4["비 또는 눈"]
        df60["wait"] = df60["wait"] + df4["비 또는 눈"]
        df70["wait"] = df70["wait"] + df4["비 또는 눈"]
        df100_1["next"] = df100_1["next"] + df4["비 또는 눈"]
        df100_1["opp"] = df100_1["opp"] + df4["비 또는 눈"]
    if new_weather == "가끔 비 또는 눈, 한때 비 또는 눈":
        df4 = pd.DataFrame(some_rain_or_snow, columns=['가끔 비 또는 눈, 한때 비 또는 눈'])
        df40["wait"] = df40["wait"] + df4["가끔 비 또는 눈, 한때 비 또는 눈"]
        df50["wait"] = df50["wait"] + df4["가끔 비 또는 눈, 한때 비 또는 눈"]
        df60["wait"] = df60["wait"] + df4["가끔 비 또는 눈, 한때 비 또는 눈"]
        df70["wait"] = df70["wait"] + df4["가끔 비 또는 눈, 한때 비 또는 눈"]
        df100_1["next"] = df100_1["next"] + df4["가끔 비 또는 눈, 한때 비 또는 눈"]
        df100_1["opp"] = df100_1["opp"] + df4["가끔 비 또는 눈, 한때 비 또는 눈"]
    if new_weather == "눈 또는 비":
        df4 = pd.DataFrame(snow_or_rain, columns=['눈 또는 비'])
        df40["wait"] = df40["wait"] + df4["눈 또는 비"]
        df50["wait"] = df50["wait"] + df4["눈 또는 비"]
        df60["wait"] = df60["wait"] + df4["눈 또는 비"]
        df70["wait"] = df70["wait"] + df4["눈 또는 비"]
        df100_1["next"] = df100_1["next"] + df4["눈 또는 비"]
        df100_1["opp"] = df100_1["opp"] + df4["눈 또는 비"]
    if new_weather == "가끔 눈 또는 비, 한때 눈 또는 비":
        df4 = pd.DataFrame(some_snow_or_rain, columns=['가끔 눈 또는 비, 한때 눈 또는 비'])
        df40["wait"] = df40["wait"] + df4["가끔 눈 또는 비, 한때 눈 또는 비"]
        df50["wait"] = df50["wait"] + df4["가끔 눈 또는 비, 한때 눈 또는 비"]
        df60["wait"] = df60["wait"] + df4["가끔 눈 또는 비, 한때 눈 또는 비"]
        df70["wait"] = df70["wait"] + df4["가끔 눈 또는 비, 한때 눈 또는 비"]
        df100_1["next"] = df100_1["next"] + df4["가끔 눈 또는 비, 한때 눈 또는 비"]
        df100_1["opp"] = df100_1["opp"] + df4["가끔 눈 또는 비, 한때 눈 또는 비"]
    if new_weather == "낙뢰":
        df4 = pd.DataFrame(light, columns=['낙뢰'])
        df40["wait"] = df40["wait"] + df4["낙뢰"]
        df50["wait"] = df50["wait"] + df4["낙뢰"]
        df60["wait"] = df60["wait"] + df4["낙뢰"]
        df70["wait"] = df70["wait"] + df4["낙뢰"]
        df100_1["next"] = df100_1["next"] + df4["낙뢰"]
        df100_1["opp"] = df100_1["opp"] + df4["낙뢰"]
    if new_weather == "빗방울":
        df4 = pd.DataFrame(raindrop, columns=['빗방울'])
        df40["wait"] = df40["wait"] + df4["빗방울"]
        df50["wait"] = df50["wait"] + df4["빗방울"]
        df60["wait"] = df60["wait"] + df4["빗방울"]
        df70["wait"] = df70["wait"] + df4["빗방울"]
        df100_1["next"] = df100_1["next"] + df4["빗방울"]
        df100_1["opp"] = df100_1["opp"] + df4["빗방울"]
    if new_weather == "연무":
        df4 = pd.DataFrame(haze, columns=['연무'])
        df40["wait"] = df40["wait"] + df4["연무"]
        df50["wait"] = df50["wait"] + df4["연무"]
        df60["wait"] = df60["wait"] + df4["연무"]
        df70["wait"] = df70["wait"] + df4["연무"]
        df100_1["next"] = df100_1["next"] + df4["연무"]
        df100_1["opp"] = df100_1["opp"] + df4["연무"]
    if new_weather == "눈날림":
        df4 = pd.DataFrame(blizzard, columns=['눈날림'])
        df40["wait"] = df40["wait"] + df4["눈날림"]
        df50["wait"] = df50["wait"] + df4["눈날림"]
        df60["wait"] = df60["wait"] + df4["눈날림"]
        df70["wait"] = df70["wait"] + df4["눈날림"]
        df100_1["next"] = df100_1["next"] + df4["눈날림"]
        df100_1["opp"] = df100_1["opp"] + df4["눈날림"]
    if new_weather == "안개":
        df4 = pd.DataFrame(fog, columns=['안개'])
        df40["wait"] = df40["wait"] + df4["안개"]
        df50["wait"] = df50["wait"] + df4["안개"]
        df60["wait"] = df60["wait"] + df4["안개"]
        df70["wait"] = df70["wait"] + df4["안개"]
        df100_1["next"] = df100_1["next"] + df4["안개"]
        df100_1["opp"] = df100_1["opp"] + df4["안개"]
    if new_weather == "박무(엷은 안개)":
        df4 = pd.DataFrame(mist, columns=['박무(엷은 안개)'])
        df40["wait"] = df40["wait"] + df4["박무(엷은 안개)"]
        df50["wait"] = df50["wait"] + df4["박무(엷은 안개)"]
        df60["wait"] = df60["wait"] + df4["박무(엷은 안개)"]
        df70["wait"] = df70["wait"] + df4["박무(엷은 안개)"]
        df100_1["next"] = df100_1["next"] + df4["박무(엷은 안개)"]
        df100_1["opp"] = df100_1["opp"] + df4["박무(엷은 안개)"]
    if new_weather == "황사":
        df4 = pd.DataFrame(yellow_dust, columns=['황사'])
        df40["wait"] = df40["wait"] + df4["황사"]
        df50["wait"] = df50["wait"] + df4["황사"]
        df60["wait"] = df60["wait"] + df4["황사"]
        df70["wait"] = df70["wait"] + df4["황사"]
        df100_1["next"] = df100_1["next"] + df4["황사"]
        df100_1["opp"] = df100_1["opp"] + df4["황사"]

    df4.to_csv("C:/yolov5-master/move.csv", index=False, encoding='CP949')
    df40.to_csv("C:/yolov5-master/sum3.csv")
    df50.to_csv("C:/yolov5-master/EX_sum3.csv")
    df60.to_csv("C:/yolov5-master/EX1_sum3.csv")
    df70.to_csv("C:/yolov5-master/EX2_sum3.csv")
    df100_1.to_csv("C:/yolov5-master/distance1.csv")

    curs3 = conn.cursor()
    curs3.execute('TRUNCATE TABLE movedata')
    conn.commit()

    f3 = open('C:/yolov5-master/movedata.csv', 'rt', encoding='CP949')

    csvReader = csv.reader(f3)

    for row in csvReader:
        sunny = (row[0])
        night_sunny = (row[1])
        day_little_cloud = (row[2])
        night_little_cloud = (row[3])
        day_many_cloud = (row[4])
        night_many_cloud = (row[5])
        cloudy = (row[6])
        shower = (row[7])
        rain = (row[8])
        some_rain = (row[9])
        snow = (row[10])
        some_snow = (row[11])
        rain_or_snow = (row[12])
        some_rain_or_snow = (row[13])
        snow_or_rain = (row[14])
        some_snow_or_rain = (row[15])
        light = (row[16])
        raindrop = (row[17])
        haze = (row[18])
        blizzard = (row[19])
        fog = (row[20])
        mist = (row[21])
        yellow_dust = (row[22])

        sql = """insert into movedata (sunny, night_sunny, day_little_cloud, night_little_cloud, day_many_cloud,night_many_cloud,
                        cloudy, shower, rain, some_rain, snow, some_snow, rain_or_snow, some_rain_or_snow, snow_or_rain,
                        some_snow_or_rain, light, raindrop, haze, blizzard, fog, mist, yellow_dust) 
         values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        curs3.execute(sql, (sunny, night_sunny, day_little_cloud, night_little_cloud, day_many_cloud, night_many_cloud,
                            cloudy, shower, rain, some_rain, snow, some_snow, rain_or_snow, some_rain_or_snow,
                            snow_or_rain,
                            some_snow_or_rain, light, raindrop, haze, blizzard, fog, mist, yellow_dust))

    curs4 = conn.cursor()
    curs4.execute('TABLE move')
    conn.commit()

    f4 = open('C:/yolov5-master/move.csv', 'r')

    csvReader = csv.reader(f4)

    for row in csvReader:
        move_name = (row[0])

        sql = """insert into move (move_name) values (%s)"""

        curs.execute(sql, (move_name))

    curs9 = conn.cursor()
    curs9.execute('TRUNCATE TABLE distance')
    conn.commit()
    f9 = open('C:/yolov5-master/distance1.csv', 'r')

    csvReader = csv.reader(f9)

    for row in csvReader:
        next = (row[1])
        opp = (row[2])
        sql = """insert into distance (next, opp) values (%s, %s)"""

        curs.execute(sql, (next, opp))

    wait_list = ['sum3', 'Ex_sum3', 'Ex1_sum3', 'Ex2_sum3']

    wait_data = {}

    for wait in wait_list:
        wait_data[wait] = pd.read_csv(f'{wait}.csv')

    all_data = pd.concat(wait_data.values(), ignore_index=True)
    all_data1 = all_data.transpose()
    all_data1.columns = ['east', 'west', 'south', 'north']
    all_data1.to_csv("C:/yolov5-master/all.csv")

    # 마지막 행 값만 추출
    all_data2 = all_data1.tail(1)
    all_data2.to_csv("C:/yolov5-master/all1.csv")

    all_data3 = pd.read_csv("C:/yolov5-master/all1.csv")

    # 계산 식 (동 east 서 west 남 south 북 north)
    ###동서 동남 동북
    all_data3["ea_we"] = all_data3["east"] - (all_data3["west"] + 0)
    all_data3["ea_so"] = all_data3["east"] - (all_data3["south"] + 3)
    all_data3["ea_no"] = all_data3["east"] - (all_data3["north"] + 3)
    all_data3.to_csv("C:/yolov5-master/all2.csv")

    all_data3[["ea_we", "ea_so", "ea_no"]].to_csv("C:/yolov5-master/all3.csv")
    all_data4 = pd.read_csv("C:/yolov5-master/all3.csv")

    all_data4["max"] = all_data4.max(axis=1, numeric_only=True)
    all_data4.loc[all_data4["max"] == all_data4["ea_we"], 'erec'] = '서쪽 출입구를 추천'
    all_data4.loc[all_data4["max"] > all_data4["ea_we"], 'erec'] = '현재 출입구를 추천'
    all_data4.loc[all_data4["max"] < all_data4["ea_we"], 'erec'] = '현재 출입구를 추천'

    all_data4.loc[all_data4["max"] == all_data4["ea_so"], 'erec'] = '남쪽 출입구를 추천'
    all_data4.loc[all_data4["max"] > all_data4["ea_so"], 'erec'] = '현재 출입구를 추천'
    all_data4.loc[all_data4["max"] < all_data4["ea_so"], 'erec'] = '현재 출입구를 추천'

    all_data4.loc[all_data4["max"] == all_data4["ea_no"], 'erec'] = '북쪽 출입구를 추천'
    all_data4.loc[all_data4["max"] > all_data4["ea_no"], 'erec'] = '현재 출입구를 추천'
    all_data4.loc[all_data4["max"] < all_data4["ea_no"], 'erec'] = '현재 출입구를 추천'
    all_data4.to_csv("C:/yolov5-master/east_cal.csv", encoding='cp949')

    df4_1 = pd.read_csv("C:/yolov5-master/east_cal.csv", encoding='cp949')
    df4_2 = df4_1['erec']
    df4_2.to_csv("C:/yolov5-master/east_cal1.csv", encoding='cp949')
    # ----------------------------------------------------------------------------------
    # 서동 서남 서북
    all_data3["we_ea"] = all_data3["west"] - (all_data3["east"] + 0)
    all_data3["we_so"] = all_data3["west"] - (all_data3["south"] + 3)
    all_data3["we_no"] = all_data3["west"] - (all_data3["north"] + 3)
    all_data3.to_csv("C:/yolov5-master/all4.csv")

    all_data3[["we_ea", "we_so", "we_no"]].to_csv("C:/yolov5-master/all5.csv")
    all_data5 = pd.read_csv("C:/yolov5-master/all5.csv")

    all_data5["max"] = all_data5.max(axis=1, numeric_only=True)
    all_data5.loc[all_data5["max"] == all_data5["we_ea"], 'wrec'] = '동쪽 출입구를 추천'
    all_data5.loc[all_data5["max"] > all_data5["we_ea"], 'wrec'] = '현재 출입구를 추천'
    all_data5.loc[all_data5["max"] < all_data5["we_ea"], 'wrec'] = '현재 출입구를 추천'

    all_data5.loc[all_data5["max"] == all_data5["we_so"], 'wrec'] = '남쪽 출입구를 추천'
    all_data5.loc[all_data5["max"] > all_data5["we_so"], 'wrec'] = '현재 출입구를 추천'
    all_data5.loc[all_data5["max"] < all_data5["we_so"], 'wrec'] = '현재 출입구를 추천'

    all_data5.loc[all_data5["max"] == all_data5["we_no"], 'wrec'] = '북쪽 출입구를 추천'
    all_data5.loc[all_data5["max"] > all_data5["we_no"], 'wrec'] = '현재 출입구를 추천'
    all_data5.loc[all_data5["max"] < all_data5["we_no"], 'wrec'] = '현재 출입구를 추천'
    all_data5.to_csv("C:/yolov5-master/west_cal.csv", encoding='cp949')

    df5_1 = pd.read_csv("C:/yolov5-master/west_cal.csv", encoding='cp949')
    df5_2 = df5_1['wrec']
    df5_2.to_csv("C:/yolov5-master/west_cal1.csv", encoding='cp949')

    # ----------------------------------------------------------------------------------
    # 남동 남서 남북
    all_data3["so_ea"] = all_data3["south"] - (all_data3["east"] + 3)
    all_data3["so_we"] = all_data3["south"] - (all_data3["west"] + 3)
    all_data3["so_no"] = all_data3["south"] - (all_data3["north"] + 6)
    all_data3.to_csv("C:/yolov5-master/all6.csv")

    all_data3[["so_ea", "so_we", "so_no"]].to_csv("C:/yolov5-master/all7.csv")
    all_data6 = pd.read_csv("C:/yolov5-master/all7.csv")

    all_data6["max"] = all_data6.max(axis=1, numeric_only=True)
    all_data6.loc[all_data6["max"] == all_data6["so_ea"], 'srec'] = '동쪽 출입구를 추천'
    all_data6.loc[all_data6["max"] > all_data6["so_ea"], 'srec'] = '현재 출입구를 추천'
    all_data6.loc[all_data6["max"] < all_data6["so_ea"], 'srec'] = '현재 출입구를 추천'

    all_data6.loc[all_data6["max"] == all_data6["so_we"], 'srec'] = '서쪽 출입구를 추천'
    all_data6.loc[all_data6["max"] > all_data6["so_we"], 'srec'] = '현재 출입구를 추천'
    all_data6.loc[all_data6["max"] < all_data6["so_we"], 'srec'] = '현재 출입구를 추천'

    all_data6.loc[all_data6["max"] == all_data6["so_no"], 'srec'] = '북쪽 출입구를 추천'
    all_data6.loc[all_data6["max"] > all_data6["so_no"], 'srec'] = '현재 출입구를 추천'
    all_data6.loc[all_data6["max"] < all_data6["so_no"], 'srec'] = '현재 출입구를 추천'
    all_data6.to_csv("C:/yolov5-master/south_cal.csv", encoding='cp949')

    df6_1 = pd.read_csv("C:/yolov5-master/south_cal.csv", encoding='cp949')
    df6_2 = df6_1['srec']
    df6_2.to_csv("C:/yolov5-master/south_cal1.csv", encoding='cp949')

    # ----------------------------------------------------------------------------------
    # 북동 북서 북남
    all_data3["no_ea"] = all_data3["north"] - (all_data3["east"] + 3)
    all_data3["no_we"] = all_data3["north"] - (all_data3["west"] + 3)
    all_data3["no_so"] = all_data3["north"] - (all_data3["south"] + 6)
    all_data3.to_csv("C:/yolov5-master/all8.csv")

    all_data3[["no_ea", "no_we", "no_so"]].to_csv("C:/yolov5-master/all9.csv")
    all_data7 = pd.read_csv("C:/yolov5-master/all9.csv")

    all_data7["max"] = all_data7.max(axis=1, numeric_only=True)
    all_data7.loc[all_data7["max"] == all_data7["no_ea"], 'nrec'] = '동쪽 출입구를 추천'
    all_data7.loc[all_data7["max"] > all_data7["no_ea"], 'nrec'] = '현재 출입구를 추천'
    all_data7.loc[all_data7["max"] < all_data7["no_ea"], 'nrec'] = '현재 출입구를 추천'

    all_data7.loc[all_data7["max"] == all_data7["no_we"], 'nrec'] = '서쪽 출입구를 추천'
    all_data7.loc[all_data7["max"] > all_data7["no_we"], 'nrec'] = '현재 출입구를 추천'
    all_data7.loc[all_data7["max"] < all_data7["no_we"], 'nrec'] = '현재 출입구를 추천'

    all_data7.loc[all_data7["max"] == all_data7["no_so"], 'nrec'] = '남쪽 출입구를 추천'
    all_data7.loc[all_data7["max"] > all_data7["no_so"], 'nrec'] = '현재 출입구를 추천'
    all_data7.loc[all_data7["max"] < all_data7["no_so"], 'nrec'] = '현재 출입구를 추천'
    all_data7.to_csv("C:/yolov5-master/north_cal.csv", encoding='cp949')

    df7_1 = pd.read_csv("C:/yolov5-master/north_cal.csv", encoding='cp949')
    df7_2 = df7_1['nrec']
    df7_2.to_csv("C:/yolov5-master/north_cal1.csv", encoding='cp949')

    # ----------------------------------------------------------------------------------
    curs5 = conn.cursor()
    curs5.execute('TRUNCATE TABLE east_cal')
    conn.commit()

    f5 = open("C:/yolov5-master/east_cal1.csv", 'r')

    csvReader = csv.reader(f5)

    for row in csvReader:
        erec = (row[1])

        sql = """insert into east_cal (erec) values (%s)"""

        curs.execute(sql, (erec))

    # ----------------------------------------------------------------------------------
    curs6 = conn.cursor()
    curs6.execute('TRUNCATE TABLE west_cal')
    conn.commit()

    f6 = open("C:/yolov5-master/west_cal1.csv", 'r')

    csvReader = csv.reader(f6)

    for row in csvReader:
        wrec = (row[1])

        sql = """insert into west_cal (wrec) values (%s)"""

        curs.execute(sql, (wrec))

    # ----------------------------------------------------------------------------------
    curs7 = conn.cursor()
    curs7.execute('TRUNCATE TABLE south_cal')
    conn.commit()

    f7 = open("C:/yolov5-master/south_cal1.csv", 'r')

    csvReader = csv.reader(f7)

    for row in csvReader:
        srec = (row[1])

        sql = """insert into south_cal (srec) values (%s)"""

        curs.execute(sql, (srec))

    # ----------------------------------------------------------------------------------
    curs8 = conn.cursor()
    curs8.execute('TRUNCATE TABLE north_cal')
    conn.commit()

    f8 = open("C:/yolov5-master/north_cal1.csv", 'r')

    csvReader = csv.reader(f8)

    for row in csvReader:
        nrec = (row[1])

        sql = """insert into north_cal (nrec) values (%s)"""

        curs.execute(sql, (nrec))
    # db의 변화 저장

    conn.commit()

    f.close()
    f2.close()
    f3.close()
    f4.close()
    f5.close()
    f6.close()
    f7.close()
    f8.close()
    f9.close()
    f10.close()
    f11.close()
    f12.close()
    f13.close()
    f14.close()
    f15.close()
    f16.close()
    f17.close()
    f18.close()
    f19.close()
    f20.close()

    conn.close()


schedule.every(1).seconds.do(message)

while True:
    schedule.run_pending()
    time.sleep(1)