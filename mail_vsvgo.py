import pandas as pd
from StyleFrame import StyleFrame, Styler, utils
import config as cfg
import cx_Oracle as ora
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
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

# Собираем пути к отчетам по ВСВГО и по пускам из ХР
path_vsvgo = path_0 + 'Отчеты по расчету ВСВГО/' + ye + '/' + mon + '/'
path_eur = path_vsvgo + 'Отчет по ВСВГО за ' + m[int(mon) - 1] + ' ' + ye + ' Европа.xls'
path_sib = path_vsvgo + 'Отчет по ВСВГО за ' + m[int(mon) - 1] + ' ' + ye + ' Сибирь.xls'
path_vsvgo_check = path_vsvgo + 'Проверка резервов мощности за ' + m[int(mon) - 1] + '.xls'
# Проверяем наличие отчетов по ВСВГО
if not os.path.isfile(path_eur):
    print('Прервано! Отчет по ВСВГО для Европы не найден')
    raise SystemExit
if not os.path.isfile(path_sib):
    print('Прервано! Отчет по ВСВГО для Сибири не найден')
    raise SystemExit

# Загружаем отчет из excel и форматируем его
vsvgo_eur = pd.read_excel(path_eur)
vsvgo_sib = pd.read_excel(path_sib)
vsvgo_eur.drop(axis=0, index=[0, vsvgo_eur.shape[0] - 1], inplace=True)
vsvgo_eur.drop(vsvgo_eur.columns[[0, 5]], axis=1, inplace=True)
vsvgo_sib.drop(axis=0, index=[0, vsvgo_sib.shape[0] - 1], inplace=True)
vsvgo_sib.drop(vsvgo_sib.columns[[0, 5]], axis=1, inplace=True)
vsvgo = vsvgo_eur.append(vsvgo_sib)
vsvgo.set_axis(['TARGET_DATE', 'HOUR', 'GTP_CODE', 'GA_CODE', 'POWER', 'AMOUNT'], axis=1, inplace=True)
vsvgo['TARGET_DATE'] = pd.to_datetime(vsvgo['TARGET_DATE'], dayfirst=True)
vsvgo.sort_values(['TARGET_DATE', 'HOUR', 'GTP_CODE', 'GA_CODE'], inplace=True)
vsvgo.reset_index(drop=True, inplace=True)

# Загружаем данные из БД Европы и Сибири
query_vsvgo_eur = '''select target_date, hour, t.trader_code gtp_code, ga_code, dp power, vsvgo_amount amount
    from FRSDB_DEV.pbr_vsvgo v, frsdb_dev.trader t
    where v.target_date between to_date(:d, 'dd.mm.yyyy') and last_day(to_date(:d, 'dd.mm.yyyy'))
    and v.end_ver=999999999999999
    and v.target_date between t.begin_date and t.end_date
    and t.trader_type=100 and t.real_trader_id=v.gtp_id
    order by 1,2,3,4'''
query_vsvgo_sib = '''select target_date, hour, t.trader_code gtp_code, ga_code, dp power, vsvgo_amount amount
    from FRSDB_DEV_sib.pbr_vsvgo v, FRSDB_DEV_sib.trader t
    where v.target_date between to_date(:d, 'dd.mm.yyyy') and last_day(to_date(:d, 'dd.mm.yyyy'))
    and v.end_ver=999999999999999
    and v.target_date between t.begin_date and t.end_date
    and t.trader_type=100 and t.real_trader_id=v.gtp_id
    order by 1,2,3,4'''
vsvgo_eur_test = pd.read_sql(query_vsvgo_eur, conn_eur, params={':d': date})
vsvgo_sib_test = pd.read_sql(query_vsvgo_sib, conn_sib, params={':d': date})
vsvgo_test = vsvgo_eur_test.append(vsvgo_sib_test)
vsvgo_test.drop_duplicates(inplace=True)
vsvgo_test.sort_values(['TARGET_DATE', 'HOUR', 'GTP_CODE', 'GA_CODE'], inplace=True)
vsvgo_test.reset_index(drop=True, inplace=True)
# Сравниваем отчет с данными в базе
check = vsvgo_test.append(vsvgo)
check.drop_duplicates(keep=False, inplace=True)
# print(check)
if check.shape[0] != 0:
    print('Прервано! Объемы из базы данных не совпадают с объемами в отчете по ВСВГО: ')
    print(check)
    raise SystemExit
else:
    print('Проверка отчета выполнена успешно! Объемы из базы данных совпадают с объемами в отчете по ВСВГО')
# Создаем отчет по пускам из холодного резерва
query_vsvgo_check_eur = '''select target_date, gtp_id, amount
                        from FRSDB_DEV.pbr_vsvgo_check
                        where target_date=to_date(:d, 'dd.mm.yyyy')'''
query_vsvgo_check_sib = '''select target_date, gtp_id, amount
                        from FRSDB_DEV_sib.pbr_vsvgo_check
                        where target_date=to_date(:d, 'dd.mm.yyyy')'''
vsvgo_check_eur = pd.read_sql(query_vsvgo_check_eur, conn_eur, params={':d': date})
vsvgo_check_sib = pd.read_sql(query_vsvgo_check_sib, conn_sib, params={':d': date})
vsvgo_check = vsvgo_check_eur.append(vsvgo_check_sib)
vsvgo_check.drop_duplicates(inplace=True)
vsvgo_check.sort_values(['TARGET_DATE', 'GTP_ID'], inplace=True)
vsvgo_check['TARGET_DATE'] = vsvgo_check['TARGET_DATE'].dt.date
excel_writer = StyleFrame.ExcelWriter(path_vsvgo_check)
vsvgo_check = StyleFrame(vsvgo_check)
vsvgo_check.set_column_width_dict({'TARGET_DATE': 12, 'GTP_ID': 10, 'AMOUNT': 20})
style_date = Styler(font_size=10,
                    number_format=utils.number_formats.date_time,
                    horizontal_alignment=utils.horizontal_alignments.right)
style = Styler(font_size=10,
               horizontal_alignment=utils.horizontal_alignments.right)
vsvgo_check.apply_column_style(cols_to_style=['TARGET_DATE'], styler_obj=style_date, style_header=True)
vsvgo_check.apply_column_style(cols_to_style=['GTP_ID', 'AMOUNT'], styler_obj=style, style_header=True)
vsvgo_check.rename({'TARGET_DATE': 'Месяц', 'GTP_ID': 'id ГТП', 'AMOUNT': 'Стоимость, руб.'}, inplace=True)
vsvgo_check.to_excel(excel_writer=excel_writer, sheet_name='Проверка резеров мощности', index=False)
excel_writer.save()

# Формирование письма
send_body = 'Добрый день!\n\nРасчет по ВСВГО за ' + m[int(mon) - 1] + ' ' + ye + ' выполнен.\n\n' \
            'Отчеты по ВСВГО и по стоимости пусков из холодного резерва во вложении.\n\n' \
            'С уважением, \nОтдел расчета объемов покупки и \nпродажи электрической энергии АО «АТС»'

msg = MIMEMultipart()
msg['From'] = cfg.send_from_vsvgo
msg['To'] = cfg.send_to_vsvgo
msg['CC'] = cfg.send_cc_vsvgo
msg['Subject'] = 'Расчет ВСВГО за ' + m[int(mon) - 1] + ' ' + ye
msg.attach(MIMEText(send_body, 'plain'))


def add_file(path):
    with open(path, "rb") as f:
        part = MIMEApplication(f.read())
    part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(path))
    msg.attach(part)


add_file(path_eur)
add_file(path_sib)
add_file(path_vsvgo_check)
# Отправка письма
s = smtplib.SMTP('smtp.rosenergo.com')
s.send_message(msg)
s.quit()
print('Письмо отправлено за: ', round(time() - time_start, 2), 'сек')