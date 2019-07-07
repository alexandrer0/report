from lxml import objectify
from lxml import etree
import pandas as pd
import config as cfg
import cx_Oracle as ora
from time import time

time_start = time()
def time_end():
    print('Отчет проверен за: ', round(time() - time_start, 2), 'сек')

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
# Создаем пути
path_0 = cfg.path
path = path_0 + 'Факт по ГТПГ по ФРС ЦП КОМ/' + ye + '/' + mon + ye + '_MFO_BR_TODFR_VOLUME.xml'

# Парсим xml
col = ('key-gtpg', 'trader-code', 'date-hour', 'volume-fact')
data = []
parser = etree.XMLParser(encoding='windows-1251', remove_comments=True)
xml = objectify.parse(open(path), parser=parser)
root = xml.getroot()
for a in root.getchildren():
    q = {**a.attrib}
    data.append(q)
cp_com = pd.DataFrame(data, columns=col)
cp_com = cp_com.apply(pd.to_numeric, errors='ignore')
cp_com['TARGET_DATE'] = pd.to_datetime(cp_com['date-hour']//100, format='%Y%m%d')
cp_com['HOUR'] = cp_com['date-hour']-100*(cp_com['date-hour']//100)
del cp_com['date-hour']
cp_com.rename(columns={'key-gtpg': 'GTP_CODE', 'trader-code': 'TRADER_CODE', 'volume-fact': 'VOLUME'}, inplace=True)
cp_com = cp_com.reindex(columns=['TARGET_DATE', 'HOUR', 'TRADER_CODE', 'GTP_CODE', 'VOLUME'])
cp_com.sort_values(['TARGET_DATE', 'HOUR', 'TRADER_CODE', 'GTP_CODE'], inplace=True)
cp_com.drop(index=0, inplace=True)
cp_com.reset_index(inplace=True)
del cp_com['index']

# Загружаем данные из БД
query_cp_com_check_eur = '''select distinct c.target_date, c.hour, u.trader_code, t.trader_code gtp_code, c.volume
                        from frsdb_dev.dev_forem_fact c, frsdb_dev.trader t, frsdb_dev.trader u
                        where c.end_ver=999999999999999 and c.isdaily =0
                        and t.trader_type=100 and u.trader_type=2
                        and c.oi_id = t.real_trader_id  and t.parent_object_id=u.real_trader_id
                        and c.target_date between t.begin_date and t.end_date
                        and c.target_date between u.begin_date and u.end_date
                        and c.target_date between to_date(:d, 'dd.mm.yyyy') and last_day(to_date(:d, 'dd.mm.yyyy'))
                        and c.oi_id in
                             (select real_trader_id from FRSDB_DEV.trader_xattr
                             where xattr_type='is_rp_rf_2699' and xattr_value=1 
                             and to_date(:d, 'dd.mm.yyyy') between begin_date and end_date)
                        order by c.target_date, c.hour, u.trader_code, t.trader_code'''

query_cp_com_check_sib = '''select distinct c.target_date, c.hour, u.trader_code, t.trader_code gtp_code, c.volume
                        from frsdb_dev_sib.dev_forem_fact c, frsdb_dev_sib.trader t, frsdb_dev_sib.trader u
                        where c.end_ver=999999999999999 and c.isdaily =0
                        and t.trader_type=100 and u.trader_type=2
                        and c.oi_id = t.real_trader_id  and t.parent_object_id=u.real_trader_id
                        and c.target_date between t.begin_date and t.end_date
                        and c.target_date between u.begin_date and u.end_date
                        and c.target_date between to_date(:d, 'dd.mm.yyyy') and last_day(to_date(:d, 'dd.mm.yyyy'))
                        and c.oi_id in
                             (select real_trader_id from FRSDB_DEV_sib.trader_xattr
                             where xattr_type='is_rp_rf_2699' and xattr_value=1 
                             and to_date(:d, 'dd.mm.yyyy') between begin_date and end_date)
                        order by c.target_date, c.hour, u.trader_code, t.trader_code'''
cp_com_check = pd.read_sql(query_cp_com_check_eur, conn_eur, params={'d': date})\
    .append(pd.read_sql(query_cp_com_check_sib, conn_sib, params={'d': date}))
cp_com_check.sort_values(['TARGET_DATE', 'HOUR', 'TRADER_CODE', 'GTP_CODE'], inplace=True)
cp_com_check.reset_index(inplace=True)
del cp_com_check['index']

# Сравниваем данные
cp_com_compare = cp_com.append(cp_com_check)
cp_com_compare.drop_duplicates(keep=False, inplace=True)
if cp_com_compare.shape[0] != 0:
    print('Прервано! Данные из БД не совпадают с данными в отчете:')
    print(cp_com_compare)
    time_end()
    raise SystemExit
elif cp_com.shape[0] != cp_com_check.shape[0]:
    print('Прервано! Неверное количество записей: ')
    print('в БД    : ', cp_com_check.shape[0])
    print('в отчете: ', cp_com.shape[0])
    time_end()
    raise SystemExit
else:
    print('Проверка отчета выполнена успешно!')
    time_end()