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

# Создаем пути
path_0 = cfg.path
path_1 = path_0 + 'Отчеты коллегам/' + ye + '/' + mon
path_c_avg = path_1 + '/Коэффициент неготовности сети по ЦЗ ' + m[int(mon) - 1] + ' ' + ye + '.xlsx'
path_c = path_1 + '/Неготовность НЦЗ по часам ' + m[int(mon) - 1] + ' ' + ye + '.xlsx'

if not os.path.exists(path_1):
    os.makedirs(path_1)

# Загружаем исходные данные
query_c_avg_eur = '''select round(gq, 14) c
                    from FRSDB_DEV.pbr_cdu_net_vst_month
                    where end_ver = 999999999999999
                    and target_date = to_date(:d, 'dd.mm.yyyy')
                    and is_unpriced_zone = 0'''
query_c_avg_sib = '''select round(gq, 14) c
                    from FRSDB_DEV_SIB.pbr_cdu_net_vst_month
                    where end_ver = 999999999999999
                    and target_date = to_date(:d, 'dd.mm.yyyy')
                    and is_unpriced_zone = 0'''
query_c_eur = '''select *
                    from frsdb_dev.pbr_cdu_net_vst
                    where end_ver = 999999999999999
                    and target_date between to_date(:d, 'dd.mm.yyyy') and last_day(to_date(:d, 'dd.mm.yyyy'))
                    and is_unpriced_zone<>0'''
query_c_sib = '''select *
                    from frsdb_dev_sib.pbr_cdu_net_vst
                    where end_ver = 999999999999999
                    and target_date between to_date(:d, 'dd.mm.yyyy') and last_day(to_date(:d, 'dd.mm.yyyy'))
                    and is_unpriced_zone<>0'''

df_c_avg_eur = pd.read_sql(query_c_avg_eur, conn_eur, params={':d': date})
if df_c_avg_eur.shape[0] == 0:
    print('Прервано! Отсутствуют данные по среднему значению коэффициента неготовности в БД Европы')
    raise SystemExit
c_avg_eur = df_c_avg_eur.values[0][0]
df_c_avg_sib = pd.read_sql(query_c_avg_sib, conn_sib, params={':d': date})
if df_c_avg_sib.shape[0] == 0:
    print('Прервано! Отсутствуют данные по среднему значению коэффициента неготовности в БД Сибири')
    raise SystemExit
c_avg_sib = df_c_avg_sib.values[0][0]

df_c_avg = pd.DataFrame({'Месяц': date,
                         'Ценовая зона': ['Европа', 'Сибирь'],
                         'Среднее значение коэффициента GQ': [c_avg_eur, c_avg_sib]})
df_c_eur = pd.read_sql(query_c_eur, conn_eur, params={':d': date})
if df_c_eur.shape[0] == 0:
    print('Прервано! Отсутствуют данные по коэффициенту неготовности в БД Европы')
    raise SystemExit
df_c_sib = pd.read_sql(query_c_sib, conn_sib, params={':d': date})
if df_c_sib.shape[0] == 0:
    print('Прервано! Отсутствуют данные по коэффициенту неготовности в БД Сибири')
    raise SystemExit
df_c_eur['TARGET_DATE'] = df_c_eur['TARGET_DATE'].astype('str')
df_c_sib['TARGET_DATE'] = df_c_sib['TARGET_DATE'].astype('str')

# Сохраняем отчеты и форматируем их
style = Styler(font_size=10,
               horizontal_alignment=utils.horizontal_alignments.right)

columns = df_c_avg.axes[1]
df_c_avg = StyleFrame(df_c_avg)
excel_writer = StyleFrame.ExcelWriter(path_c_avg)
for s in columns:
    df_c_avg.set_column_width(s, 7+str(s).__len__())
df_c_avg.apply_column_style(cols_to_style=columns, styler_obj=style, style_header=True)
df_c_avg.to_excel(excel_writer=excel_writer, sheet_name='Среднее значение коэффициента', index=False)
excel_writer.save()
excel_writer.close()

columns = df_c_eur.axes[1]
df_c_eur = StyleFrame(df_c_eur)
df_c_sib = StyleFrame(df_c_sib)
excel_writer = StyleFrame.ExcelWriter(path_c)
for s in columns:
    df_c_eur.set_column_width(s, 12+0.5*str(s).__len__())
    df_c_sib.set_column_width(s, 12+0.5*str(s).__len__())
df_c_eur.apply_column_style(cols_to_style=columns, styler_obj=style, style_header=True)
df_c_sib.apply_column_style(cols_to_style=columns, styler_obj=style, style_header=True)
df_c_eur.to_excel(excel_writer=excel_writer, sheet_name='Европа', index=False)
df_c_sib.to_excel(excel_writer=excel_writer, sheet_name='Сибирь', index=False)
excel_writer.save()
excel_writer.close()

# Формирование письма
send_body = 'Добрый день!\n\nНаправляем отчеты по коэффициенту несоблюдения объемов и сроков ремонтов ФСК ' \
            'за ' + m[int(mon) - 1] + ' ' + ye + '.\n\n' \
            'С уважением, \nОтдел расчета объемов покупки и \nпродажи электрической энергии АО «АТС»'
msg = MIMEMultipart()
msg['From'] = cfg.send_from
msg['To'] = cfg.send_to_net
msg['CC'] = cfg.send_cc
msg['Subject'] = 'Отчеты по коэффициенту несоблюдения объемов и сроков ремонтов ФСК ' + m[int(mon) - 1] + ' ' + ye
msg.attach(MIMEText(send_body, 'plain'))


def add_file(path):
    with open(path, "rb") as f:
        part = MIMEApplication(f.read())
    part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(path))
    msg.attach(part)


add_file(path_c)
add_file(path_c_avg)
# Отправка письма
s = smtplib.SMTP('smtp.rosenergo.com')
s.send_message(msg)
s.quit()

print('Письмо отправлено за: ', round(time() - time_start, 2), 'сек')