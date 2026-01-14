# Setup EC2

## 1. Tạo EC2 instance

1. Tạo tài khoản AWS, truy cập **AWS Console**
2. Vào **EC2 → Launch instance**
3. Chọn **Amazon Linux 2023**
4. Tạo **key pair** (RSA hoặc ED25519) và **tải file `.pem` về**
5. Cấu hình:

   * RAM: **8GB**
   * Storage: **30GB**
   
6. Launch instance


## 2. Kết nối vào EC2

Trên máy local:

```shell
ssh -i ~/Downloads/key.pem ec2-user@public-ipv4
```


## 3. Kết quả kết nối thành công

Nếu thấy màn hình:

```shell
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
 **Kết nối SSH thành công**


## 4. (Optional) Elastic IP

* Gán **Elastic IP** cho instance
* Mục đích:

  * IP **không đổi khi reboot**
  * Dễ quản lý server hơn



## 5. (Optional) SSH config để truy cập nhanh

Thêm vào `~/.ssh/config`:

```ssh
Host server
    HostName <public-ip-or-elastic-ip>
    User ec2-user
    IdentityFile ~/Downloads/key.pem
```

Sau đó chỉ cần:

```shell
ssh server
```

## 2. Cấu trúc thư mục bên trong ec2
> mkdir -p ~/Dataengineering

```shell
git clone git@github.com:user/reponame.git
```


## 3. Những công cụ sử dụng bên trong ec2

|  | Tools                                      |
|----------|------------------------------------|
| Tools    | git, docker, docker compose, tmux  |

# Những gi nên config thêm trong ec2

> đhs docker compose không có sẵn khi cài docker trên ec2, tra cứu thì nói docker compose không có sẵn khi cài, hay là do server sao sao đó nhưng mà mình phải tải docker compose binary

* `Mình cài bằng binary nên os nào cũng dùng được`

### outline
1. [kiểm tra version docker](#1-kiểm-tra-version-docker)
2. [kiểm tra version docker compose (docker-compose)](#2-kiểm-tra-version-docker-compose-docker-compose)
3. [cài docker compose binary](#3-cài-docker-compose-binary)
4. [chú ý thêm (nên đọc)](#4-chú-ý-thêm-nên-doc)

#### 1. kiểm tra version docker
```shell
docker --version
```
#### 2. kiểm tra version docker compose (docker-compose)
```shell
docker-compose --version && docker compose version
```
#### 3. cài docker compose binary

```shell
sudo mkdir -p /usr/local/lib/docker/cli-plugins

sudo curl -SL \
https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
-o /usr/local/lib/docker/cli-plugins/docker-compose

sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
```

#### 4. chú ý thêm (nên đọc)
> mình grant quyền cho user ec2-user có quyền chạy docker, start docker thành system service chạy docker tự động, và nó sd systemd chứ không sử dụng openRC.

```shell 
sudo systemctl status docker
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user
sudo reboot
```
các bạn nên login lại nhé.
