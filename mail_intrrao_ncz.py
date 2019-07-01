import config as cfg
import smtplib
import report_intrrao_ncz as ri
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
# Формирование письма
msg = MIMEMultipart()
msg['From'] = cfg.send_from
msg['To'] = cfg.send_to_cap
msg['CC'] = cfg.send_cc
msg['Subject'] = 'VC_PC, переток, ДД НЦЗ ' + ri.m[int(ri.mon) - 1] + ' ' + ri.ye
body = 'Привет!\n\nОтчеты по ИнтерРАО во вложении.\n\nС уважением, \nОтдел расчета объемов покупки' \
            ' и \nпродажи электрической энергии АО «АТС»'
msg.attach(MIMEText(body, 'plain'))


def add_file(path):
    with open(path, "rb") as f:
        part = MIMEApplication(f.read())
    part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(path))
    msg.attach(part)


add_file(ri.path_2)
add_file(ri.path_3)
add_file(ri.path_4)
# Отправка письма
s = smtplib.SMTP('smtp.rosenergo.com')
s.send_message(msg)
s.quit()
