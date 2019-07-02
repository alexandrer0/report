import config as cfg
import cx_Oracle as ora
import pandas as pd
import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from time import time

time_start = time()

# Подключение к БД
conn_sib = ora.connect(cfg.user_db + '/' + cfg.pass_db + '@' + cfg.db_sib)
conn_eur = ora.connect(cfg.user_db + '/' + cfg.pass_db + '@' + cfg.db_eur)
# Загрузка даты отчета
ye = cfg.year
mon = cfg.mon
date = '01.' + mon + '.' + ye
print('Отчетный месяц: ', date)

# Список месяцев
m = ['январь', 'февраль', 'март', 'апрель', 'май', 'июнь',
     'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь']

# Корневая папка
path_0 = cfg.path
# Собираем путь к отчету для СВНЦ
path = path_0 + 'Факт по ГТПП для СВНЦ/' + ye + '/' + ye + mon + '01_gtp_fact_vc_ee_4svnc.xls'
# Проверяем наличие отчета для СВНЦ
if not os.path.isfile(path):
    print('Прервано! Отчет для СВНЦ не найден')
    raise SystemExit
# Загружаем отчет из excel и форматируем его
fact_svnc = pd.read_excel(path)
fact_svnc.drop(axis=0, index=[0, 1, 2, 3, 4], inplace=True)
fact_svnc.drop(fact_svnc.columns[[0, 1, 2]], axis=1, inplace=True)
fact_svnc.set_axis(['GTP_CODE', 'VOLUME'], axis=1, inplace=True)
fact_svnc.sort_values('GTP_CODE', inplace=True)
fact_svnc.reset_index(drop=True, inplace=True)
# print(fact_svnc)

query_eur = '''select distinct a.gtp_code, sum(a.fact) over (partition by a.gtp_code) volume
        from
             (select distinct t.trader_code gtp_code, f.target_date, f.hour, f.volume fact
             from frsdb_dev.dev_forem_fact f, frsdb_dev.trader t
             where f.end_ver=999999999999999 and f.oi_id=t.real_trader_id 
             and f.oi_type=4 and t.fed_station<>1 
             and f.target_date between t.begin_date and t.end_date
             and f.target_date between to_date(:d, 'dd.mm.yyyy') and last_day(to_date(:d, 'dd.mm.yyyy'))) a'''
query_sib = '''select distinct a.gtp_code, sum(a.fact) over (partition by a.gtp_code) volume
        from
             (select distinct t.trader_code gtp_code, f.target_date, f.hour, f.volume fact
             from frsdb_dev_sib.dev_forem_fact f, frsdb_dev_sib.trader t
             where f.end_ver=999999999999999 and f.oi_id=t.real_trader_id 
             and f.oi_type=4 and t.fed_station<>1 
             and t.trader_code not in ('PAMUREZN', 'PYAKU5ZN', 'PYAKUTZN')
             and f.target_date between t.begin_date and t.end_date
             and f.target_date between to_date(:d, 'dd.mm.yyyy') and last_day(to_date(:d, 'dd.mm.yyyy'))) a'''
# Загружаем данные из БД Европы и Сибири
fact_eur = pd.read_sql(query_eur, conn_eur, params={'d': date})
if fact_eur.shape[0] == 0:
    print('Прервано! Отсутствуют данные по фактическим объемам в БД Европы')
    raise SystemExit
fact_sib = pd.read_sql(query_sib, conn_sib, params={'d': date})
if fact_sib.shape[0] == 0:
    print('Прервано! Отсутствуют данные по фактическим объемам в БД Сибири')
    raise SystemExit
fact_test = fact_eur.append(fact_sib)
fact_test.drop_duplicates(inplace=True)
fact_test.sort_values('GTP_CODE', inplace=True)
fact_test.reset_index(drop=True, inplace=True)
# Сравниваем отчет с данными в базе
test = fact_test.append(fact_svnc)
test.drop_duplicates(keep=False, inplace=True)
if test.shape[0] != 0:
    print('Прервано! Объемы из базы данных не совпадают с объемами в отчете для СВНЦ: ')
    print(test)
    raise SystemExit
else:
    print('Проверка отчета выполнена успешно! Объемы из базы данных совпадают с объемами в отчете для СВНЦ')

# print(fact_check)

query_version_eur = '''SELECT max(v.version_id) version_id
    FROM frsdb_dev.versions v, frsdb_dev.version_type v2
    WHERE v.version_type = v2.version_type
    and v.period_from = to_date(:d, 'dd.mm.yyyy') and v.period_to = last_day(to_date(:d, 'dd.mm.yyyy'))
    AND v2.version_type=103'''
query_version_sib = '''SELECT max(v.version_id) version_id
    FROM frsdb_dev_sib.versions v, frsdb_dev_sib.version_type v2
    WHERE v.version_type = v2.version_type
    and v.period_from = to_date(:d, 'dd.mm.yyyy') and v.period_to = last_day(to_date(:d, 'dd.mm.yyyy'))
    AND v2.version_type=103'''

df_version_eur = pd.read_sql(query_version_eur, conn_eur, params={'d': date})
df_version_sib = pd.read_sql(query_version_sib, conn_sib, params={'d': date})
version_eur = df_version_eur['VERSION_ID'].values[0]
version_sib = df_version_sib['VERSION_ID'].values[0]
if version_eur is None:
    print('Прервано! Факт для ПРНЦ на Европе не опубликован')
    raise SystemExit
if version_sib is None:
    print('Прервано! Факт для ПРНЦ на Сибири не опубликован')
    raise SystemExit


# Формирование письма
send_body = 'Добрый день!\n\nНаправляем фактические объемы по ГТП потребления ' \
            'за ' + m[int(mon) - 1] + ' ' + ye + '.\n\n' + 'Публикация факта за ' + m[int(mon) - 1] + ' ' + ye + ' ' \
            'по Европе и Сибири выполнена.\n\n' + 'Версии: \n' + 'Европа  ' + str(version_eur) + '\n' \
            'Сибирь  ' + str(version_sib) + '\n\nС уважением, \nОтдел расчета объемов покупки и \nпродажи' \
            ' электрической энергии АО «АТС»'
msg = MIMEMultipart()
msg['From'] = cfg.send_from
msg['To'] = cfg.send_to_svnc
msg['CC'] = cfg.send_cc
msg['Subject'] = 'Факт ГТПП и публикация факта для ПРНЦ за ' + m[int(mon) - 1] + ' ' + ye
msg.attach(MIMEText(send_body, 'plain'))


def add_file(path):
    with open(path, "rb") as f:
        part = MIMEApplication(f.read())
    part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(path))
    msg.attach(part)


add_file(path)
# Отправка письма
s = smtplib.SMTP('smtp.rosenergo.com')
s.send_message(msg)
s.quit()
print('Письмо отправлено за: ', round(time() - time_start, 2), 'сек')