# -*- coding: utf8 -*-
import json
import zipfile
import os
import datetime
from ftplib import FTP

from qcloud_cos_v5 import CosConfig
from qcloud_cos_v5 import CosS3Client
from qcloud_cos_v5 import CosServiceError
from qcloud_cos_v5 import CosClientError



def main_handler(event, context):
    print("Received event: " + json.dumps(event, indent = 2)) 
    print("Received context: " + str(context))
    time = (datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    print("Start Time " + str(time))
    data_folder = "/mnt/" #挂载目录
    backup_name = "backup" #备份文件名
    ftp_host = "" #ftp主机
    ftp_port = 21 #ftp端口
    ftp_user = "aaa"  #ftp账号
    ftp_pwd = "aaa"  #ftp密码
    ftp_folder = "scf_backup"  #ftp文件夹
    backup_name = "/tmp/" + backup_name
    file_name = f"{backup_name}{time}.zip"
    z = zipfile.ZipFile(file_name, 'w', zipfile.ZIP_DEFLATED)
    for dirpath, dirnames, filenames in os.walk(data_folder):
      print(f"dirpath:{dirpath}, dirnames:{dirnames}, filenames:{filenames}")
      for filename in filenames:
        try:
          z.write(os.path.join(dirpath, filename))
        except Exception as e:
          print(f"except: {e}, cannot zip file: {dirpath}{filename}")
    z.close()
    print(file_name)

    # 设置用户属性, 包括 secret_id, secret_key, region等。Appid 已在CosConfig中移除，请在参数 Bucket 中带上 Appid。Bucket 由 BucketName-Appid 组成
    secret_id = ''     # 替换为用户的 SecretId，请登录访问管理控制台进行查看和管理，https://console.cloud.tencent.com/cam/capi
    secret_key = ''   # 替换为用户的 SecretKey，请登录访问管理控制台进行查看和管理，https://console.cloud.tencent.com/cam/capi
    region = 'ap-beijing'      # 替换为用户的 region，已创建桶归属的region可以在控制台查看，https://console.cloud.tencent.com/cos5/bucket
                               # COS支持的所有region列表参见https://www.qcloud.com/document/product/436/6224
    token = None               # 如果使用永久密钥不需要填入token，如果使用临时密钥需要填入，临时密钥生成和使用指引参见https://cloud.tencent.com/document/product/436/14048
    bucket = '' #容器名
    cos_folder = ftp_folder
    config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token)  # 获取配置对象
    client = CosS3Client(config)

    response = client.put_object_from_local_file(
        Bucket=bucket,
        LocalFilePath=file_name,
        Key=cos_folder+"/"+os.path.basename(file_name),
    )
    print(response['ETag'])


    ftp = FTP()
    ftp.set_debuglevel(2)
    ftp.set_pasv(True)
    ftp.connect(ftp_host, ftp_port)
    ftp.login(ftp_user, ftp_pwd)
    print(ftp.getwelcome())
    ftp.cwd(ftp_folder)
    bufsize = 1024
    file_handler = open(file_name, 'rb')
    res = -1
    try:
      res = ftp.storbinary(f"STOR {os.path.basename(file_name)}", file_handler, bufsize) # upload file
    except Exception as e:
      print(f"except: {e}, cannot upload file: {ftp_host}:{ftp_port} {file_name}")
    finally:
      ftp.set_debuglevel(0)
      file_handler.close()
      ftp.quit()

    print("End Time " + (datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S'))
    return(res)
