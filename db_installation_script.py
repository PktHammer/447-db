# Please run this as the root user
import os
import json

confirms = ["Y", "y", "Yes", "yes"]
denys = ["N", "n", "No", "no"]

# Defaults
user = 'db_user'
password = 'test_db_pw'
ip_endpoint = '127.0.0.1:3306'

# Generate secret file
user_next = False
while user_next is not True:
    user = str(input("Please set a db user name:") or user)
    password = str(input("Please set a db user password:") or password)
    ip_endpoint = str(input("Please set an IP endpoint:") or ip_endpoint)
    user = json.dumps(user)
    password = json.dumps(password)
    ip_endpoint = json.dumps(ip_endpoint)
    form =  f"user = {user}\n" + \
          f"password = {password}\n" + \
          f"ip_endpoint = {ip_endpoint}\n"
    res = input(f"Looks good? (Y/N?)\n" + form)
    if res in confirms:
        with open('db_gen_secret.py', "w") as f:
            f.write(form)
        user_next = True


# Do you want to setup the database Y/N?

# os.system("apt install mariadb-server")
# os.system("mysql_secure_installation")
# os.system("mariadb -u root")
