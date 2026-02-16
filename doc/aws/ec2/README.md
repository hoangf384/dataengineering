# Setup EC2 Instance

## Outline
0. [cấu hình tối thiểu](#0-cấu-hình-tối-thiểu)
1. [Tạo EC2 Instance](#1-tạo-ec2-instance)
*   1.1 [SSH vào EC2](#11-ssh-vào-ec2)
*   1.2 [Cấu hình SSH local](#12-cấu-hình-ssh-local)
*   1.3 [Clone project vào EC2](#13-clone-project-vào-ec2)
*   1.4 [Cài đặt Docker & Docker Tools](#14-cài-đặt-docker--docker-tools)
2. [Thiết lập Security Group](#2-thiết-lập-security-group)
*   2.1 [inbound rules](#21-inbound-rules)
*   2.2 [outbound rules](#22-outbound-rules)
3. [IAM Role](#3-iam-role)
*   3.1 [tạo custom policy](#31-tạo-custom-policy)
*   3.2 [IAM Role](#32-tạo-iam-role-cho-ec2)
*   3.3 [IAM Policy](#33-attach-role-vào-ec2-instance)

---
# 0. Cấu hình tối thiểu

| Name              | Type           | AZ              | Volume | vRAM | vCPU |
| ----------------- | -------------- | --------------- | ------ | ---- | ---- |
| Database Instance | m7i-flex.large | ap-southeast-2b | 30GB   | 8GB  | 2    |
| Airflow Instance  | c7i-flex.large | ap-southeast-2c | 10GB   | 4GB  | 2    |
| Spark Instance    | m7i-flex.large | ap-southeast-2b | 8GB    | 8GB  | 2    |

## 1.1 SSH vào EC2

```bash
ssh -i ~/Downloads/key.pem ec2-user@<public-ipv4>
```

Nếu kết nối thành công:

```
,     #_
~\_  ####_        Amazon Linux 2023
~~  \_#####\
~~     \###|
~~       \#/ ___   https://aws.amazon.com/linux/amazon-linux-2023
~~       V~' '->
 ~~~         /
   ~~._.   _/
      _/ _/
    _/m/'
```

SSH thành công

---

## 1.2 Cấu hình SSH local

Mở file:

```bash
~/.ssh/config
```

Thêm:

```ssh
Host data-ec2
    HostName <public-ip-or-elastic-ip>
    User ec2-user
    IdentityFile ~/Downloads/key.pem
#   LocalForward 18080 localhost:18080
```

Kết nối:

```bash
ssh data-ec2
```

---

## 1.3 Clone project vào EC2

```bash
git clone git@github.com:hoangf384/dataengineering.git
```

Hoặc:

```bash
git clone https://github.com/hoangf384/dataengineering.git
```

---

## 1.4 Cài đặt Docker, Docker Compose và Docker Buildx

### Kiểm tra version

```bash
docker --version
docker compose version
docker buildx version
```

---

### Cài Docker Compose & Buildx (Binary)

```bash
sudo mkdir -p /usr/local/lib/docker/cli-plugins

sudo curl -SL \
https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
-o /usr/local/lib/docker/cli-plugins/docker-compose

sudo curl -SL \
https://github.com/docker/buildx/releases/download/v0.17.1/buildx-v0.17.1.linux-amd64 \
-o /usr/local/lib/docker/cli-plugins/docker-buildx

sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx
```

---

### Enable Docker service & cấp quyền

```bash
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user
sudo reboot
```

---

## Kết quả sau mục 1

* EC2 đã chạy
* SSH thành công
* Docker hoạt động
* Docker Compose & Buildx hoạt động
* `ec2-user` có quyền chạy docker

---

# 2. Thiết lập Security Group
Security Group sử dụng cho EC2:

* **Security Group ID**: `sg-0d7ab87d7304151f3`
* **Name**: `Data Platform SG`

---

## 2.1 Inbound Rules

| Type        | Protocol | Port  | Source               | Description            |
| ----------- | -------- | ----- | -------------------- | ---------------------- |
| All traffic | All      | All   | sg-0d7ab87d7304151f3 | Internal communication |
| Custom UDP  | UDP      | 41641 | 0.0.0.0/0            | Tailscale              |
| SSH         | TCP      | 22    | 0.0.0.0/0            | SSH access             |

---

### Giải thích từng rule

#### Rule 1 – Internal SG Communication

```
All traffic
Source: sg-0d7ab87d7304151f3
```

Cho phép các instance trong cùng Security Group giao tiếp với nhau (phù hợp khi:

* Có nhiều EC2 trong cùng data platform
* Dùng Spark cluster (master ↔ worker)
* Airflow ↔ Spark ↔ Superset)

---

#### Rule 2 – Tailscale (UDP 41641)

```
Protocol: UDP
Port: 41641
Source: 0.0.0.0/0
```

Port mặc định của **Tailscale WireGuard tunnel**.

Dùng khi:

* Kết nối private network giữa local machine ↔ EC2
* Không cần expose public port nhiều service

---

#### Rule 3 – SSH

```
Protocol: TCP
Port: 22
Source: 0.0.0.0/0
```

Cho phép SSH từ internet vào EC2.

---
## 2.2 outbound rules

| Type        | protocol | Port    | Source    | Description                    |
|-------------|----------|---------|-----------|--------------------------------|
| ALL traffic | ALL      | ALL     | 0.0.0.0/0 | Allow all outbound to internet |

> để server kết nối được với internet
## Kết quả sau khi cấu hình

* SSH hoạt động
* Tailscale hoạt động
* Các service trong cùng SG có thể giao tiếp nội bộ
* Sẵn sàng deploy Data Platform

---

# 3. IAM Role

 **Mục đích**: IAM Role cho phép EC2 instance:

* Đọc / ghi dữ liệu vào S3
* Lưu Spark event logs
* Không cần hard-code AWS Access Key trong code

---

## 3.1 Tạo Custom Policy

Vào:

```
AWS Console → IAM → Policies → Create Policy
```

Chọn tab **JSON** và dán:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::spark-log-proj",
                "arn:aws:s3:::spark-log-proj/*"
            ]
        }
    ]
}
```

### Giải thích quyền:

| Action          | Ý nghĩa      |
| --------------- | ------------ |
| s3:GetObject    | Đọc file     |
| s3:PutObject    | Ghi file     |
| s3:ListBucket   | Liệt kê file |
| s3:DeleteObject | Xóa file     |

Chỉ cấp quyền cho bucket cụ thể, ví dụ: `spark-log-proj`

Đặt tên policy ví dụ: `Spark-airflow-S3-Log-Policy`

---

## 3.2 Tạo IAM Role cho EC2

Vào:

```
IAM → Roles → Create Role
```

### Bước cấu hình:

* Trusted entity type: **AWS Service**
* Use case: **EC2**

Sau đó:

* Attach policy: `Spark-airflow-S3-Log-Policy`
* Đặt tên role `ec2-airflow-spark-s3-connector`

---

## 3.3 Attach Role vào EC2 Instance

Vào:

```
EC2 → Instances → Chọn instance→ Actions → Security → Modify IAM Role
```

Chọn:

```
ec2-airflow-spark-s3-connector
```

Save

---

## Kiến trúc sau khi hoàn tất

```
EC2 (Spark / Airflow / Superset)
        ↓
IAM Role
        ↓
S3 (spark-log-proj)
```

---

## Kết quả sau mục 3

* EC2 có thể đọc/ghi S3
* Spark có thể lưu event logs lên S3
* Airflow có thể trigger job đọc/ghi data
* Không sử dụng access key thủ công

Security Group sử dụng cho EC2:

* **Security Group ID**: `sg-0d7ab87d7304151f3`
* **Name**: `Data Platform SG`

---