import config as cfg
import smtplib
import report_ppp_ts as ppp
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
# Формирование письма
msg = MIMEMultipart()
msg['From'] = cfg.send_from
msg['To'] = cfg.send_to_ts
msg['CC'] = cfg.send_cc
msg['Subject'] = 'Отчёты по РД и ППП на проверку за ' + ppp.m[int(ppp.mon) - 1] + ' ' + ppp.ye
body = 'Добрый день!\n\nОтправляем на проверку объёмы РД по потребителям в особых регионах и отчёты по ППП' \
            '\n\nС уважением, \nОтдел расчета объемов покупки и \nпродажи электрической энергии АО «АТС»'
msg.attach(MIMEText(body, 'plain'))


def add_file(path):
    with open(path, "rb") as f:
        part = MIMEApplication(f.read())
    part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(path))
    msg.attach(part)


add_file(ppp.path_out_eur)
add_file(ppp.path_out_sib)
add_file(ppp.path_rd)
add_file(ppp.path_ncz)
# Отправка письма
s = smtplib.SMTP('smtp.rosenergo.com')
s.send_message(msg)
s.quit()