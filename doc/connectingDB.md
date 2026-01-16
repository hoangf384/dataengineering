# Hướng dẫn kết nối tới database trên ec2

## trước khi vào nội dung thì mình sẽ giới thiệu sơ qua:

- server: ec2-aws
- db: mysql, cassandra (chạy trên nền docker)
- client: máy mình
- query engine: spark (cũng chạy trên nền docker)

## Mục tiêu
kết nối từ spark tới database mong muốn

## chuẩn bị
file jdbc của mysql và cassandra
tailscale (ưu tiên tải trực tiếp bằng repo manager: dnf hoặc apt)
tài khoản github, google

## hướng dẫn
### Tailscale
> cái này là vpn, kết nối giữa server và client cài đặt trên cả 2 máy
`lệnh này trên fedora nhé trên server thì dùng curl sẽ ổn định hơn (vì dnf trên từng phiên bản khác nhau)`

```bash
sudo dnf install tailscale

sudo systemctl enable tailscaled
sudo systemctl start tailscaled

sudo tailscale up 
```
> login google hoặc github, mình sử dụng github
`sau khi làm trên cả 2 máy thì 90% sẽ thành công, 10% còn lại các bạn hỏi chatgpt nhe kkk`

### MySQL

> kết nối với mysql là cực hình
1. connect to mysql server (docker)
> trong ec2 server nhé
```bash
docker exec -it mysql bash mysql -u root -p
```
> nhập password root của bạn vào `(thường là root, password hoặc mysql)`

```bash
mysql> DROP USER IF EXISTS 'spark'@'%';

mysql> CREATE USER 'spark'@'%'
IDENTIFIED WITH caching_sha2_password
BY 'spark';

mysql> GRANT ALL PRIVILEGES ON mydb.* TO 'spark'@'%';

FLUSH PRIVILEGES;
```
Nguyên lý debug:

Nếu `mysql -h <ip> -u spark -p` chạy được trong Spark container -> JDBC Spark chắc chắn chạy được
2. kết nối từ spark đến mysql
ở máy client nhé
> vào bash của spark master trong docker
```bash
docker exec -it spark-master bash
```
> vào pyspark, mount thêm jdbc connector của mysql

```bash
/opt/spark/bin/pyspark --jars /opt/spark/jars_external/mysql-connector-j-8.4.0.jar
```
> connect mysql 
```python
df = (
     spark.read
     .format("jdbc")
     .option(
         "url",
         "jdbc:mysql://<TAILSCALE_IP>:3306/mydb"
         "?allowPublicKeyRetrieval=true"
         "&useSSL=false"
     )
     .option("driver", "com.mysql.cj.jdbc.Driver")
     .option("user", "spark")
     .option("password", "spark")
     .option("query", "SELECT 1 AS ok")
     .load()
)
df.show()
```
kết quả: 
```
+---+
| ok|
+---+
|  1|
+---+
``` 
chúc bạn kết nối thành công!
