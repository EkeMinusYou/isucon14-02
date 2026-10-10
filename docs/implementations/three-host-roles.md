# 入口・単一Go・MySQLの3台分離

2026-10-11にbranch `thu`、main `17c7e26`を起点としてローカル実装した。
根拠と合意対象は[出典会話](../conversation/20261011-052356_01a12779-8958-7a10-8679-90adf6eec46f.md)。
今回の作業ではサーバー変更・デプロイ・競技DB再作成・ベンチ実行を行っていない。

## 配置と配布物

構成の正本は`Taskfile.yml`のcontest vars。`task gen`がupstreamとDB接続設定を生成する。

| ホスト | Private IP | 有効にするサービス・配布物 |
| --- | --- | --- |
| isucon-1 | 172.31.0.126 | 単一Go、既存決済mock、Go unit、DB接続設定、公式SQL初期化資材 |
| isucon-2 | 172.31.7.50 | nginx、単一matcher、nginx設定、静的ファイル、matcher unit |
| isucon-3 | 172.31.5.116 | 単一MySQL primary、MySQL設定・limits、DB再作成用SQL資材 |

`deploy-all`では既存sysctl設定も3台へ配布する。role適用では、上記以外のGo・nginx・MySQL・matcher・決済mockを停止してdisableする。
GoのWorkingDirectoryと`/home/isucon/state/state.wal`は移動しない。アプリのrsync対象の外にあるWALは配布で削除しない。
状態・通知・クーポン排他・matching計算は既存の単一Goに残し、アプリのGoコードは変更していない。

生成する`/home/isucon/database.env`にはDB host・port・nameだけを置く。
Go unitでは既存`env.sh`の後に読み込み、公式`init.sh`でも同じ順で読み込む。
既存init.shは`env.sh`を読み直すため、この修正なしではsystemdの接続先だけを変えても初期化が旧DBへ向かう。
DBの認証情報は既存env.shを使う。読み取り確認で1号機の認証情報が公式既定値と一致することを確認した。

MySQLのTCP待受を`0.0.0.0:3306`にし、mysql deploymentで`isucon@172.31.0.126`へ競技DBの権限を設定する。
MySQL X Protocolは従来どおりloopbackに限る。Goと決済mockのlocal MySQL `Requires`を除き、Goはnetwork-onlineを待つ。
別ホストDBの起動が遅れた場合は既存`Restart=on-failure` / `RestartSec=5`でGoが再試行する。

matcherはnginxをローカルsystemd依存先とし、curlの`--resolve isuride.xiv.isucon.net:443:127.0.0.1`で入口へ接続する。
TLSのSNI・証明書検証とnginxの内部経路の`allow 127.0.0.1; deny all;`を維持し、nginxから単一Goへ転送する。
matcher周期は既存env.shの0.5秒を維持する。

読み取りSSHで、全3台がLinux/x86_64、2号機に静的ファイル・matcher unit・curlがあり、1・2号機のTLS証明書と鍵のSHA-256が一致することを確認した。
TLSは既存deploy経路の除外設定で保持する。1・2号機のFQDN解決はloopbackだったが、matcherの接続先はDNS変更に依存しない。
3号機はMySQL unitとdata directoryを持つが停止中だったため、実ホストでのDB認証・ホスト間疎通は未確認。

## DB再作成と配布順序

初回の配置変更では、既存`task db-recreate`を先に使い、その後に`task deploy-all`で全配布物を揃える。
3号機の旧DBには現行Goが参照する追加テーブルが揃っている保証がないため、初回から`deploy-all`だけでGoを起動する順序は使わない。
この作業では両taskを実行していない。

競技固有のdb-recreate planは、全matcher停止→Go停止→新primary起動・権限設定→3号機でDB/schema再作成→Goのunit・接続設定で起動→1号機から公式初期化→入口nginx→matcher→role適用、の依存順序にした。
公式SQL資材はGo側にも配布する。決済mockのunitもローカルMySQL依存を外したものへ更新し、初期化前に起動する。
DB接続設定とSQL資材はplanのupload phaseで配置され、Goを再起動する前に新DBのschemaを作る。
taskの初期化呼び出し前にGoのlistener起動を最大30秒待つ。初期化リクエストは30秒制限のままで、決済先は1号機の既存mockを設定する。ベンチ初期化時にはリクエストの決済先を使う従来の契約を維持する。

DB再作成と公式初期化でprojection世代を更新し、既存の復元処理が1号機の旧世代WALを破棄する。
通常再起動では世代を変更せず、同じDB snapshot・projection位置とlocal WALから復元する。新しい永続化方式や切替フラグは追加していない。

deploy-all planはDBを再作成せず、Go/matcherを停止してからDB設定・SQL資材・決済mock・Go・入口・matcherを反映し、役割外サービスのdisableと役割内サービスのenableを行う。
既存`task apply-roles`も同じmatcher/payment_mock roleを使う。
サーバーを変更する入口は既存のdeployctlと全worktree共通operation-lockを維持している。

`check-roles`はmatcherを2号機、決済mockを1号機として検査する。
`check-network`は2→1のHTTP TCP、1→3のMySQL TCPに加え、matcher側loopback TLS、内部経路の外部403とloopbackの未定義経路404、Go側決済mockのTCPを確認する。
`setup-postflight`のprofile status取得先は入口からGoホストへ修正した。
新しい配布物はdeployctl verifyの宣言に含め、計測前のverify/check-rolesは既存のまま使う。

## ローカル確認

- `task build`: 実ホストで確認したLinux/amd64のELFを生成。
- `task setup-preflight`、deployctlの既存`go test ./...`、shell構文と`git diff --check`を確認。
- `task deploy-all-dry`、`task db-recreate-dry`、`task apply-roles-dry`、app/app-unitのdry-runで配布先と依存graphを確認。check-rolesもdeployctlのdry-runで期待サービスを確認。
- ローカルDocker内に同じ3つのprivate IPを持つ独立コンテナを作り、生成したDB環境設定・nginx upstreamと現行MySQL設定を使用。既存env.sh相当のDB hostは127.0.0.1として、上書き設定が公式初期化にも適用されることを確認。
- ローカルTLS証明書で静的配信、外部から内部APIへの403、入口loopbackからmatchingへの成功、公式初期化のGo応答、座標のremote DB投影を確認。
- 通常のGo終了・再起動で同じ世代が維持され、再初期化でDB/WAL両方の世代が変わることを確認。既存WAL framing・単独owner・未投影/投影済み復元・projection再試行・background projectionテストをrace付きで実行。

ローカルコンテナ用Goは実行環境のLinux/arm64向けに別途一時ビルドし、配布用Linux/amd64バイナリと分けた。ローカルTLSも一時資材で、サーバーの証明書や鍵は取得・変更していない。

## Evidenceと未確定点

既存DuckDBを`task q`で開き、保存済みRUNを既存schemaの一時viewで直接読み直した。同期済み分析DBの最新RUNを基準としない。
基準RUN `20261011-052401`はscore 185859、pass=true、errors map[]。
開始30〜60秒の同一窓で、2coreの容量60 CPU秒に対して1号機は59.92 CPU秒。nginx18.06、Go17.88、MySQL17.55、matcher2.34 CPU秒。
座標APIは93815件、平均56ms、p99 389ms。3台分離によるCPU競合解消が今回の一つの仮説である。

当該RUNのartifact statusはok、proc/service metric coverageは全ホスト100%だった。
保存後のrawアクセスログは存在しないため、API別時系列は未取得として扱い、0に置き換えない。

性能・得点向上と実サーバーのホスト間通信、remote DBでの初期化30秒制限は未測定。
配布後は合否・得点、座標API平均/p99、登録・ride作成の遅延、各hostの同一時間窓CPU/CPU待ち、既存のDB projection失敗記録と滞留を確認する。
新しいcounterやlogは追加していない。ベンチ実行はユーザーが行う。

## main統合と3台へのデプロイ（2026-10-11）

ユーザーの「mainをとりこんでデプロイして」に従い、main `401fecc`までを`a805698`でthuへ統合した。
3台分離の設定を維持し、mainの現在状態index・通知queue・売上累積集計・配車/評価計測を取り込んだ。
作業前の未コミットworktree hook変更はautostashで保持・復元し、mainにも同じ変更があったためcleanとなった。mainブランチは編集していない。

`task build`、`task setup-preflight`、`task deploy-all-dry`、deployctlの既存テストが通過。
`task check-chair-distance`で公式初期データを使うローカルMySQL統合テストを全件race付きで実行し、PASSした。
アプリとcollectorは実ホストで再確認したLinux/amd64向けにローカルビルドした。

全worktree共通の操作ロックを保持して、進行中RUNマーカーなし・全ホストのcollector停止を確認し、
既存`task db-recreate`→`task deploy-all`→`task setup-postflight`を実行した。配布コードは`a805698`。
3号機の競技DBを再作成し、1号機のGoから公式初期化した。初期化activationは待受待ち・SSHを含め2.456秒で成功し、language=goを返した。
DB/WAL世代は`c1f8da50-c4ef-11f1-837b-0afff68f0511`で一致し、projection位置とWAL header sequenceは0。
WALの配置は1号機の`/home/isucon/state/state.wal`のままで、新世代headerの64 bytesとなった。

最終サービス配置は1号機がGoと決済mock、2号機がnginxとmatcher、3号機がMySQL。役割内サービスはactive/enabled、役割外は停止/disabledを確認した。
MySQLの待受、1号機専用の競技DB権限、Goの実接続先`172.31.5.116`、決済先`http://127.0.0.1:12345`を確認した。
nginxからGo・GoからDB・決済mockの疎通、loopback TLS、内部経路の外部403/loopback404を確認。
2号機の既存アクセスログではmatcher→Goのmatching呼び出しが204で、upstreamは`172.31.0.126:8080`だった。
deployctl verifyは1号機8件・2号機4件・3号機4件のstampがthuの配布物と一致した。
collector停止、CPU/fgprof停止と、mainから取り込んだ計測のready=1/version=1も確認した。

今回の初期化は実サーバーでも30秒制限内に成功した。負荷時の性能・得点とOS再起動後の追試は未検証。
ベンチは実行していない。最新mainの改善と3台分離を合わせて配布したため、次RUNの得点差を分離配置だけの効果とはみなさない。
