# Gdrive

与 google drive 建立多文件夹双向同步的基础库。

Google drive auth 文件：`~/.gdrive/auth.json`

数据库文件：`~/.gdrive/gdrive.sqlite`

# Use gdrive auth

GDRIVE_TESTING=1 swift run gdrive-auth          # 浏览器登录，凭据写入 auth.json
GDRIVE_TESTING=1 swift run gdrive-auth status   # 检查凭据
GDRIVE_TESTING=1 swift run gdrive-auth ids 10  # 生成 10 个 Drive ID
