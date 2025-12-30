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
> trước tiên nên setup ssh của server với github, làm như cái bên kia thôi

```shell
git clone git@github.com:user/reponame.git
```


## 3. Những công cụ sử dụng bên trong ec2

|  | Tools              |
|----------|--------------------|
| Tools    | git, docker, tmux  |



