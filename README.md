# go-cron
目標：Golangで分散型タスクをスケジューラする。

server worker両方ある。

### ソースコードをローカルにpullする

### etcdサーバの構築
下記のURLを参照してください。
https://github.com/etcd-io/etcd

`package.json`に自分のetcd endpointを指定してください。

### ログ保存するMongodbの構築
簡単にMongodbを構築するため、dockerをおすすめです。

- Dockerでmongodbの構築手順：
`docker pull mongo:latest` 最新のmongodb imageを取得

`docker images` mongo imageを取得したかを確認

`docker run -itd --name mongo -p 27017:27017 mongo` でコンテナの27017ポートをホストの27017ポートにマップする

`package.json`に自分のmongodb endpointを指定してください。
 
 mongodbのcollectionのinsert find deleteなどの詳細操作はmongoのdocを参照してください。
 
### master worker の起動
working directoryなどを指定してから、master workerを起動してください。

### 管理画面
- home画面
![home](https://github.com/baikourin/go-cron/blob/master/src/github.com/gocron/master/main/webroot/static/home.png?raw=true)


- 編集画面
![edit](https://github.com/baikourin/go-cron/blob/master/src/github.com/gocron/master/main/webroot/static/edit.png?raw=true)
