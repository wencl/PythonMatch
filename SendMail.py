from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart, MIMEBase
from email.mime.image import MIMEImage
from email.utils import parseaddr, formataddr
import config
import smtplib
import os
from_addr = config.from_email
password = config.emil_password
to_addr = config.to_email
smtp_server = "smtp.163.com"


def _format_addr(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr))

def sedMessage(massage):
    # 邮件对象:
    msg = MIMEMultipart()
    msg['From'] = _format_addr('系统<%s>' % from_addr)
    msg['To'] = _format_addr('收件人<%s>' % to_addr)
    msg['Subject'] = Header('数据异常……', 'utf-8').encode()
    # 邮件正文是MIMEText:
    # msg.attach(MIMEText(massage, 'plain', 'utf-8'))
    # 添加图片

    msg.attach(MIMEText('<html><body><p>' + massage + '</p>' +
                        '<p><img src="cid:300174.jpg"></p>' +
                        '<p><img src="cid:300403.jpg"></p>' +
                        '</body></html>', 'html', 'utf-8'))

    # 添加附件就是加上一个MIMEBase，从本地读取一个图片:
    for filename in os.listdir("./picture"):
        with open('./picture/'+filename, 'rb') as f:
            # 设置附件的MIME和文件名，这里是png类型:
            # mime = MIMEBase('image', 'jpg', filename='1.jpg')
            mime = MIMEImage(f.read())
            f.close()
            # 加上必要的头信息:
            # mime.add_header('Content-Disposition', 'attachment', filename=filename)
            # mime.add_header('Content-ID', '<0>')
            # mime.add_header('X-Attachment-Id', '0')
            mime.add_header('Content-ID', filename)
            # 把附件的内容读进来:
            # mime.set_payload(f.read())
            # 用Base64编码:
            # encoders.encode_base64(mime)
            # 添加到MIMEMultipart:
            msg.attach(mime)



    server = smtplib.SMTP(smtp_server, 25)
    server.set_debuglevel(1)
    server.login(from_addr, password)
    server.sendmail(from_addr, [to_addr], msg.as_string())
    server.quit()

# for filename in os.listdir("./picture"):
#     print(filename)
