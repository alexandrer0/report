import config as cfg
import cx_Oracle as ora
import pandas as pd
import smtplib
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

query_version_eur = '''SELECT max(v.version_id) version_id
    FROM frsdb_dev.versions v, frsdb_dev.version_type v2
    WHERE v.version_type = v2.version_type
    and v.period_from = to_date(:d, 'dd.mm.yyyy') and v.period_to = last_day(to_date(:d, 'dd.mm.yyyy'))
    AND v2.version_type=102'''
query_version_sib = '''SELECT max(v.version_id) version_id
    FROM frsdb_dev_sib.versions v, frsdb_dev_sib.version_type v2
    WHERE v.version_type = v2.version_type
    and v.period_from = to_date(:d, 'dd.mm.yyyy') and v.period_to = last_day(to_date(:d, 'dd.mm.yyyy'))
    AND v2.version_type=102'''

df_version_eur = pd.read_sql(query_version_eur, conn_eur, params={'d': date})
df_version_sib = pd.read_sql(query_version_sib, conn_sib, params={'d': date})
version_eur = df_version_eur['VERSION_ID'].values[0]
version_sib = df_version_sib['VERSION_ID'].values[0]
if version_eur is None:
    print('Прервано! Данные для ПРНЦ на Европе не опубликованы')
    raise SystemExit
if version_sib is None:
    print('Прервано! Данные для ПРНЦ на Сибири не опубликованы')
    raise SystemExit

send_body = 'Добрый день!\n\nДанные по индикаторам за ' + m[int(mon) - 1] + ' ' + ye + ' опубликованы.\n\n' + 'Версии '\
            'данных: \n\n' + 'Европа  ' + str(version_eur) + '\nСибирь  ' + str(version_sib) + '\n\nС уважением, \n' \
            'Отдел расчета объемов покупки и \nпродажи электрической энергии АО «АТС»'

# Формирование письма
msg = MIMEMultipart()
msg['From'] = cfg.send_from
msg['To'] = cfg.send_to_svnc
msg['CC'] = cfg.send_cc
msg['Subject'] = 'Публикация индикаторов за ' + m[int(mon) - 1] + ' ' + ye
msg.attach(MIMEText(send_body, 'plain'))

# Отправка письма
s = smtplib.SMTP('smtp.rosenergo.com')
s.send_message(msg)
s.quit()
print('Письмо отправлено за: ', round(time() - time_start, 2), 'сек')