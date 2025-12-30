# tài liệu hướng dẫn sử dụng git với ssh

## 1. ssh

bạn phải thiết lập SSH trên máy bạn đã, bằng câu lệnh sau

```shell
cd ~ 
ssh-keygen -t ed25519 -C "your_email@example.com"
```

* đường dẫn mặc định là ~/.ssh/id_ed25519
* có thể enter hết 3 lần cũng được, còn chi tiết hơn thì hỏi chatgpt

Sau đó vào github, profile setting, ssh & gpg key, tạo ssh mới -> nhập tên và key
* gợi ý: tên bạn nên đặt rõ ràng (vd như mình sẽ là: fedoralocal)
Đối với key thì bạn vào shell (mình dùng linux nên không rõ window có làm giống mình không), gõ lệnh này: 


```shell
cat ~/.ssh/id_ed25519.pub
```

Copy key trên dán vào key yêu cầu của ssh ở github -> create key

Tiếp tục với shell
```shell
ssh -T git@github.com
```
dòng thông báo `Hi username! You've successfully authenticated, but GitHub does not provide shell access.` là thanh công
